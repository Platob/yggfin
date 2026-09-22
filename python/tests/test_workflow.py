"""Text and FIX ingestion over the checked-in fixture and a replay, in three steps."""

from __future__ import annotations

import datetime
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow
import pytest
from pyiceberg.expressions import EqualTo

from rekep import Field, Message, cli
from rekep.fix import (
    EVENT_CLOCK,
    MESSAGE_KEY,
    SOURCES,
    TRANSACTION_CLOCK,
    UNDATED,
    FixRegistry,
    fix_message_field,
    iceberg_fix_field,
)
from rekep.iceberg import IcebergCatalog, IcebergDataset, iceberg_contract_field, partition_keys

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"
FIX_CONTRACT = ROOT / "schemas" / "rekep" / "fixmsg.json"
WORKFLOW = (("parse_messages", {}), ("parse_fix_raw", {}), ("parse_fix_refined", {}))
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: The day the bridge fixture was captured on. A task covers the last day
#: unless its document names a window, and the fixture is dated, so every
#: run here names its day -- `end: 2026-08-14` is the exclusive end of it.
WINDOW = {"start": "2026-08-14", "end": "2026-08-14"}

#: What the bridge fixture's 144 physical rows produce, first run.
#:
#: `logs.messages` is keyed on `curruuid`, and the native code digests the
#: line's row number and its object beside its bytes, so the 3 exact repeated
#: lines are 3 rows and every line lands. The shipped header dates all 144, so
#: the window's read answers every one of them.
#: A FIX row is a message and not a line -- prose
#: answers none and a line carrying two frames answers two -- and
#: `fix.raw` is keyed on `curruuid`, so the 79 messages those 144 lines
#: carry settle on 49 events the capture describes. The walk merges the
#: observations of one event and adds one expiry, so `fix.refined` reads 49
#: and writes 19: every line carries the session, context and sequence the
#: fold merges on, so it folds more than it did when fifteen lines were left
#: unmatched. The gaps
#: are the point of the keys: one line identity is one line and the same
#: message logged at every hop is one event.
FIRST = {
    "parse_messages": {"read": 144, "written": 144, "skipped": 0},
    "parse_fix_raw": {"read": 144, "written": 49, "skipped": 30},
    "parse_fix_refined": {"read": 49, "written": 19, "skipped": 0},
}

#: What a replay of the same window produces: the same reads and the same
#: writes, because a run replaces what its window carries -- the table holds
#: each row once however often the window runs.
REPLAY = FIRST

#: Stored rows after both runs, and the two snapshots each table then holds.
STORED = {
    "logs.messages": 144,
    "fix.raw": 49,
    "fix.refined": 19,
}

#: The business identifier whose persisted lifecycle chain anchors the
#: acceptance assertions.
CHAIN = "00026877711XOEA0"
CHAIN_EVENTS = 4
CHAIN_LAST_STEP = 1

#: How many events the parse could not date: the messages that stated no
#: `SendingTime`, which sit at the codec's pin in `fix.raw` until the walk
#: dates them by their `TransactTime` -- so no refined row is at the pin.
PINNED = 33


class Ran:
    """One catalog, and the tasks run against it."""

    def __init__(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        self.root = tmp_path
        self.warehouse = tmp_path / "warehouse"
        self.catalog = {
            "name": "rekep",
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
                "warehouse": (tmp_path / "warehouse").as_uri(),
            },
        }
        self._capsys = capsys

    def task(self, name: str, **overrides: Any) -> dict[str, Any]:
        """One task, with its result read back off `stdout`."""
        argv = [
            "task",
            "run",
            str(ROOT / "tasks" / name / f"{name}.json"),
            "--parameter",
            f"catalog={json.dumps(self.catalog)}",
        ]
        for parameter, value in overrides.items():
            argv += ["--parameter", f"{parameter}={json.dumps(value)}"]
        assert cli.main(argv) == 0, name
        return json.loads(self._capsys.readouterr().out)

    def workflow(self, **overrides: Any) -> dict[str, dict[str, Any]]:
        """Every publishing task instance, in dependency order, by stage name."""
        results = {}
        for name, held in WORKFLOW:
            if name == "parse_messages":
                first = {"filesystem": FIXTURE.as_uri(), **WINDOW}
            else:
                first = dict(WINDOW)
            result = self.task(name, **first, **held, **overrides)
            results[result["task"]] = result
        return results

    def rows(self) -> dict[str, int]:
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            return {
                dataset.identifier: dataset.read_arrow_table().num_rows
                for dataset in store.datasets(None)
            }
        finally:
            store.close()

    def table(self, name: str) -> pyarrow.Table:
        """Read one stored table and release its catalog owners."""
        store = IcebergCatalog.from_dict(self.catalog)
        dataset = store.dataset(name)
        try:
            return dataset.read_arrow_table()
        finally:
            dataset.close()
            store.close()

    def partitions(self, name: str) -> dict[str, str]:
        """One stored table's partition spec, by the column it reads."""
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            table = store.catalog.load_table(name)
            return {
                table.schema().find_column_name(field.source_id) or "": str(field.transform)
                for field in table.spec().fields
            }
        finally:
            store.close()

    def layout(self, name: str) -> dict[str, Any]:
        """What Iceberg itself records about one table: key, spec and order."""
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            table = store.catalog.load_table(name)
            schema = table.schema()
            return {
                "key": {schema.find_column_name(held) for held in schema.identifier_field_ids},
                "spec": [
                    (schema.find_column_name(field.source_id), str(field.transform))
                    for field in table.spec().fields
                ],
                "sort": [
                    (schema.find_column_name(field.source_id), str(field.transform))
                    for field in table.sort_order().fields
                ],
            }
        finally:
            store.close()

    def snapshots(self) -> dict[str, int]:
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            return {
                dataset.identifier: len(
                    store.catalog.load_table(dataset.identifier).metadata.snapshots
                )
                for dataset in store.datasets(None)
            }
        finally:
            store.close()


@pytest.fixture()
def ran(tmp_path: Path, capsys: pytest.CaptureFixture) -> Iterator[Ran]:
    yield Ran(tmp_path, capsys)


def counted(result: dict[str, Any]) -> dict[str, int]:
    return {name: result[name] for name in ("read", "written", "skipped")}


def sources(rows: pyarrow.Table) -> list[bytes]:
    """The one line each FIX row names, as the bytes `logs.messages` holds."""
    return [held[0] for held in rows.column(SOURCES).to_pylist()]


def test_the_workflow_publishes_ulbridge_and_a_replay_writes_nothing(ran: Ran) -> None:
    first = ran.workflow()
    assert {name: counted(result) for name, result in first.items()} == FIRST
    assert first["parse_fix_raw"]["messages"] == 79
    assert ran.rows() == STORED
    messages = ran.table("logs.messages")
    assert messages.schema.equals(Message.into_field().into_arrow_schema(), check_metadata=False)
    assert messages.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    instants = {
        row["seqnum"]: row[EVENT_CLOCK]
        for row in messages.select(("seqnum", EVENT_CLOCK)).to_pylist()
    }
    assert instants[1] == datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    # The shipped header dates every line of this capture, the fifteen that
    # group their micros included: no line takes the file's modification
    # time, and none is at the pin, which dates a line only where its handle
    # has no clock.
    assert not any(instant == EPOCH for instant in instants.values())
    assert set(instants) == set(range(1, 145)), "the row number the read counts from 1"
    # The read states an identity over every line, and states a different one
    # for every line: the column is not a constant the declaration filled in.
    identities = messages.column("curruuid").to_pylist()
    assert len(set(identities)) == messages.num_rows
    assert bytes(16) not in identities

    replay = ran.workflow()
    assert {name: counted(result) for name, result in replay.items()} == REPLAY
    assert ran.rows() == STORED, "an idempotent replay adds no row"
    assert ran.snapshots() == {name: 2 for name in STORED}, "and each replay is one commit"


def test_every_result_is_the_shape_a_route_reads(ran: Ran) -> None:
    from rekep.logs import Stage
    from rekep.times import unix_of

    for name, result in ran.workflow().items():
        assert Stage.validated(result) == result
        assert result["task"] == name
        assert result["window"] == {
            "start": unix_of("2026-08-14"),
            "end": unix_of("2026-08-15"),
        }, "the window a run covered is what its result reports"
        assert len(json.dumps(result)) < 4096, "XCom carries a summary, never a payload"
    assert result["targets"] == {"refined": "fix.refined"}
    assert result["sources"] == {"raw": "fix.raw"}


def test_a_window_the_capture_falls_outside_reads_nothing_and_writes_none(ran: Ran) -> None:
    """The default window is the last day, and the fixture is not in it. The
    window is the read's own `where`, so the read answers no line at all --
    every one of the fixture's lines is dated by its header, on a day the
    window does not cover -- and the table is created empty."""
    result = ran.task("parse_messages", filesystem=FIXTURE.as_uri())

    assert counted(result) == {"read": 0, "written": 0, "skipped": 0}
    assert ran.rows() == {"logs.messages": 0}

    raw = ran.task("parse_fix_raw")
    assert counted(raw) == {"read": 0, "written": 0, "skipped": 0}
    assert counted(ran.task("parse_fix_refined")) == {"read": 0, "written": 0, "skipped": 0}
    # And a line the header could not date is dated by its object's own
    # modification time, so it is in the window that covers that instant.
    unframed = ran.root / "unframed.log"
    unframed.write_bytes(b"one physical line\n")
    written = ran.task("parse_messages", filesystem=unframed.as_uri())
    assert counted(written) == {"read": 1, "written": 1, "skipped": 0}
    assert ran.table("logs.messages").column("msgpluginid").to_pylist() == [None]
    assert ran.table("logs.messages").column(EVENT_CLOCK).to_pylist() != [EPOCH]


def test_a_window_replaces_only_the_lines_it_covers(ran: Ran) -> None:
    """A run over the whole day and then one over its first part leave every
    line once: the second run replaces the lines its window covers and no
    other. The fixture's lines straddle one second, which is where it cuts."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    stored = ran.table("logs.messages")
    cut = datetime.datetime(2026, 8, 14, 14, 46, 40, tzinfo=UTC)
    clock = stored.column(EVENT_CLOCK)
    later = stored.filter(pyarrow.compute.greater_equal(clock, cut)).num_rows
    assert 0 < later < STORED["logs.messages"], "the fixture straddles this cut"

    first = ran.task(
        "parse_messages",
        filesystem=FIXTURE.as_uri(),
        start="2026-08-14",
        end="2026-08-14T14:46:40",
    )

    # The window is the read's own `where`, so what the run read is what the
    # window covers: the dated lines before the cut, and nothing it did not
    # write.
    assert first["read"] == first["written"] == STORED["logs.messages"] - later
    assert first["skipped"] == 0
    assert ran.rows() == {"logs.messages": STORED["logs.messages"]}, (
        "the later lines were not the run's to touch"
    )


def test_an_empty_capture_is_read_and_produces_nothing(ran: Ran, tmp_path: Path) -> None:
    """Zero rows is a run, not a failure: the route skips what has no input."""
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "quiet.log").write_text("", encoding="utf-8")

    result = ran.task("parse_messages", filesystem=empty.as_uri(), **WINDOW)

    assert counted(result) == {"read": 0, "written": 0, "skipped": 0}
    assert result["targets"] == {"messages": "logs.messages"}


@pytest.mark.parametrize("name", ["parse_fix_raw", "parse_fix_refined"])
def test_a_fix_stage_refuses_an_empty_registry_before_creating_a_table(
    ran: Ran, tmp_path: Path, name: str
) -> None:
    registry = tmp_path / "empty-fix-registry"
    registry.mkdir()
    argv = [
        "task",
        "run",
        str(ROOT / "tasks" / name / f"{name}.json"),
        "--parameter",
        f"catalog={json.dumps(ran.catalog)}",
        "--parameter",
        f"registry={json.dumps(registry.as_uri())}",
    ]

    assert cli.main(argv) == 1
    assert ran.rows() == {}, "a missing dictionary cannot leave a narrow FIX table"


def test_a_raw_task_forwards_codec_options_without_rewriting_text(ran: Ran, tmp_path: Path) -> None:
    """FIX codec options cross the task boundary unchanged; the text is not rewritten."""
    capture = tmp_path / "prefixed.log"
    capture.write_bytes(
        b"2026-08-14 12:46:39.769 [1] [ULBridge] (INFO) "
        b"  --> 8=FIX.4.4|35=D|11=OPTION-1|55=HOLN|10=000|\n"
    )

    ran.task("parse_messages", filesystem=capture.as_uri(), **WINDOW)
    result = ran.task(
        "parse_fix_raw",
        codec_options={"threads": 2, "batch_row_size": 2},
        **WINDOW,
    )

    assert result["messages"] == result["written"] == 1
    raw = ran.table("fix.raw")
    assert raw.column("msgtype").to_pylist() == ["D"]
    assert raw.column("clordid").to_pylist() == ["OPTION-1"]


def test_the_official_clock_delay_is_a_pin_the_task_forwards(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """What dates a message is the venue's clock, within a stated distance.

    `SendingTime(52)` is when a session put the message on the wire, which is
    not when the thing it reports happened. The parse dates the message by the
    official transaction clock standing within `official_time_delay_ms` of
    that sending clock, and falls back to the sending clock where none stands
    that near -- so the pin is what decides, per run, how far a venue's clock
    may sit from the wire and still be read as the same event. It is a codec
    option and nothing else, so the task forwards it like every other one.

    Each pin gets its own warehouse, because the instant a message settles on
    is what its identity is derived from: a table written under one delay and
    re-run under another does not replace its rows, it gains them.
    """

    def dated_by(name: str, **pinned: Any) -> tuple[int, int]:
        root = tmp_path / name
        root.mkdir()
        held = Ran(root, capsys)
        held.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
        held.task("parse_fix_raw", **pinned, **WINDOW)
        rows = (
            held.table("fix.raw")
            .select((EVENT_CLOCK, "sendingtime", TRANSACTION_CLOCK))
            .to_pylist()
        )
        return (
            sum(1 for row in rows if row["sendingtime"] == row[EVENT_CLOCK]),
            sum(1 for row in rows if row[TRANSACTION_CLOCK] == row[EVENT_CLOCK]),
        )

    # The core's own second, which is what a run takes when it pins nothing.
    assert dated_by("default") == (6, 13)
    # A nonpositive delay admits only a clock equal to the sending one, so
    # every event a venue stamped a little apart falls back to the wire.
    assert dated_by("nought", codec_options={"official_time_delay_ms": 0}) == (16, 3)
    # And a wide one admits the clocks the default already did and no more:
    # no transaction clock in this capture stands between a second and ten
    # minutes from its wire.
    assert dated_by("wide", codec_options={"official_time_delay_ms": 600_000}) == (6, 13)


def test_a_narrow_dictionary_still_answers_every_event(ran: Ran, tmp_path: Path) -> None:
    """A venue stamps nanoseconds; Iceberg v2 holds microseconds. And a
    dictionary narrow enough to type no message type at all still answers a
    row per message: the identity, the instant and the chain are the crate's
    own block, and a frame is a message whether or not a dictionary can name
    its type. The table takes the dictionary's shape -- no `msgtype` column,
    because nothing defined it -- and every event lands in it.
    """
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    registry = tmp_path / "nanosecond-fix-registry"
    registry.mkdir()
    sending_time = Field("sendingtime", pyarrow.timestamp("ns", tz="UTC"), nullable=True)
    sending_time.fix.tag = 52
    # A bare registry already seeds the standard clocks, so a store holding
    # nothing else reads as the empty one the task refuses. One specification
    # field beside the clock is what makes it a dictionary.
    symbol = Field("symbol", "utf8", nullable=True)
    symbol.fix.tag = 55
    FixRegistry.from_fields([sending_time, symbol]).write_into(registry)

    result = ran.task("parse_fix_raw", registry=registry.as_uri(), **WINDOW)

    assert result["read"] == STORED["logs.messages"]
    assert result["messages"] == 79
    # The 79 arrivals reduce to 51, not the full registry's 49: its Account
    # projection derives `PBRK6_EDA` for both arrivals in two duplicate pairs,
    # while this narrow registry retains `/account:0=PBRK6_EDA` as one extra
    # residual entry (63 rather than 62), so the canonical hashes differ.
    assert counted(result)["written"] == 51
    raw = ran.table("fix.raw")
    assert raw.num_rows == 51
    assert "msgtype" not in raw.column_names
    assert raw.num_columns == 35
    # The table is the dictionary's shape, and its clocks are stored at the
    # precision Iceberg v2 holds.
    assert raw.schema.field("sendingtime").type == pyarrow.timestamp("us", tz="UTC")
    assert raw.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    assert "body" not in raw.column_names
    assert raw.column("srcuuids").null_count == 0
    # The walk is another matter: a chain is read off what a message is, and
    # a dictionary that cannot name a message's type places none of them. The
    # refined run reads every `fix.raw` row of the window and lands no row, under
    # the same shape, rather than guessing at a chain.
    walked = ran.task("parse_fix_refined", registry=registry.as_uri(), **WINDOW)
    # No narrow row has a typed message category to enter lifecycle, so the
    # task reports no emitted or deduplicated candidate.
    assert counted(walked) == {"read": 51, "written": 0, "skipped": 0}
    assert ran.table("fix.refined").num_rows == 0
    assert ran.table("fix.refined").schema.equals(raw.schema)


def test_dumped_fix_schema_can_stream_a_mock_row_through_iceberg(ran: Ran) -> None:
    field = iceberg_contract_field(FIX_CONTRACT.read_text(encoding="utf-8"), "FixMsg")
    # The published contract carries the partition spec, so a table built from
    # the document alone is laid out the way both FIX tasks lay theirs out.
    assert partition_keys(field) == {EVENT_CLOCK: "hour"}
    schema = field.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [
            {
                "beginstring": "FIX.4.4",
                # The settled bundle a replayable row always carries. Only a
                # walk stamps `snapunix`, so this row leaves it null.
                EVENT_CLOCK: EPOCH,
                "creaunix": EPOCH,
                "currhashcode": 1,
                "crosshashcode": 2,
                "curruuid": uuid.UUID(int=1).bytes,
                "crossuuid": uuid.UUID(int=2).bytes,
            }
        ],
        schema=schema,
    )
    source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
    store = IcebergCatalog.from_dict(ran.catalog)
    fixes = store.dataset("fix.raw", field=field)
    try:
        assert fixes.overwrite_arrow_reader(source, field, merge_by=True) == 1
        stored = fixes.read_arrow_table(field)
        assert stored.num_rows == 1
        assert stored.num_columns == 128
        # The native row has event facts only. `srcuuids`, where present,
        # joins it to stored lines without copying captures into this table.
        assert "body" not in stored.column_names
        assert "currhashcode" in stored.column_names
        assert "srcuuids" in stored.column_names
        assert not {"body", "loglevel", "msgthreadid"} & set(stored.column_names)
    finally:
        fixes.close()
        store.close()


def test_a_text_table_of_the_previous_shape_is_a_table_of_its_own(ran: Ran) -> None:
    """The event the read settles is stated on every row, so a table that
    never held it is not this one: Iceberg adds no required column to rows
    that never had it, and the dataset refuses rather than landing a table
    half of whose rows state no instant, no identity and no code."""
    declared = Message.into_field().into_arrow_schema()
    previous = Field.from_arrow_schema(
        pyarrow.schema(
            [member for member in declared if member.name not in {EVENT_CLOCK, "curruuid"}],
            metadata=declared.metadata,
        ),
        name="logs.messages",
    )
    store = IcebergCatalog.from_dict(ran.catalog)
    try:
        lines = store.dataset("logs.messages", field=previous)
        lines.create_with_field(previous)
        lines.close()
        lines = store.dataset("logs.messages", field=Message.into_field())
        # Named, so the refusal is the one this test means and not another
        # required column reached first.
        with pytest.raises(ValueError, match=f"cannot add required column: {EVENT_CLOCK}"):
            lines.add_fields(Message.into_field())
        lines.close()
        store.drop_table("logs.messages", purge=True)
    finally:
        store.close()


def test_a_capture_missing_altogether_is_reported(ran: Ran, tmp_path: Path) -> None:
    argv = [
        "task",
        "run",
        str(ROOT / "tasks" / "parse_messages" / "parse_messages.json"),
        "--parameter",
        f"catalog={json.dumps(ran.catalog)}",
        "--parameter",
        f"filesystem={json.dumps((tmp_path / 'absent').as_uri())}",
    ]
    assert cli.main(argv) == 1


def test_several_files_are_one_capture(ran: Ran, tmp_path: Path) -> None:
    """A capture is a directory, opened one naturally sorted path at a time."""
    capture = tmp_path / "capture"
    capture.mkdir()
    lines = FIXTURE.read_bytes().split(b"\n")
    middle = len(lines) // 2
    (capture / "a.log").write_bytes(b"\n".join(lines[:middle]) + b"\n")
    (capture / "b.log").write_bytes(b"\n".join(lines[middle:]))

    result = ran.task("parse_messages", filesystem=capture.as_uri(), **WINDOW)

    assert counted(result) == FIRST["parse_messages"]
    assert ran.rows() == {"logs.messages": STORED["logs.messages"]}


def test_messages_stream_through_hour_partitions(
    ran: Ran,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The application hands Iceberg a reader; partitioned reads stay readers."""
    capture = tmp_path / "hourly"
    capture.mkdir()
    line = FIXTURE.read_bytes().split(b"\n", 1)[0] + b"\n"
    (capture / "14.log").write_bytes(line)
    (capture / "15.log").write_bytes(line.replace(b"2026-08-14 14:", b"2026-08-14 15:", 1))

    handed_to_iceberg: list[pyarrow.Schema] = []
    replace = IcebergDataset.overwrite_arrow_reader

    def observed_replace(
        dataset: IcebergDataset,
        source: pyarrow.RecordBatchReader,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        handed_to_iceberg.append(source.schema)
        return replace(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", observed_replace)
    result = ran.task("parse_messages", filesystem=capture.as_uri(), **WINDOW)

    assert counted(result) == {"read": 2, "written": 2, "skipped": 0}
    assert handed_to_iceberg == [Message.into_field().into_arrow_schema()]

    first = datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    second = datetime.datetime(2026, 8, 14, 15, 46, 39, 769000, tzinfo=UTC)
    store = IcebergCatalog.from_dict(ran.catalog)
    messages = store.dataset("logs.messages", field=Message.into_field())
    try:
        spec = messages.iceberg_table.spec()
        assert [(field.name, str(field.transform)) for field in spec.fields] == [
            ("currunix_hour", "hour")
        ]
        assert {row["partition"]["currunix_hour"] for row in messages.data_files().to_pylist()} == {
            int(first.timestamp() // 3600),
            int(second.timestamp() // 3600),
        }

        rows = []
        for instant in (first, second):
            assert messages.scan_plan(EqualTo(EVENT_CLOCK, instant))["skipped"] == 1
            reader = messages.read_arrow_reader(
                Message.into_field(), row_filter=EqualTo(EVENT_CLOCK, instant)
            )
            try:
                assert isinstance(reader, pyarrow.RecordBatchReader)
                rows.extend(row for batch in reader for row in batch.to_pylist())
            finally:
                reader.close()
    finally:
        messages.close()
        store.close()

    assert {row[EVENT_CLOCK] for row in rows} == {first, second}
    assert {row["msgpluginid"] for row in rows} == {"ULBridge"}
    # The same bytes under two clocks are two lines with two identities.
    assert len({row["curruuid"] for row in rows}) == 2


def test_ulbridge_messages_flow_directly_through_the_fix_codec(
    ran: Ran,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handed_to_iceberg: dict[str, pyarrow.Schema] = {}
    schema_modes: dict[str, bool] = {}
    replace = IcebergDataset.overwrite_arrow_reader

    def observed_replace(
        dataset: IcebergDataset,
        source: pyarrow.RecordBatchReader,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        schema_modes[dataset.identifier] = dataset.merge_schema
        handed_to_iceberg[dataset.identifier] = source.schema
        return replace(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", observed_replace)
    ran.workflow()

    assert schema_modes == {"logs.messages": False, "fix.raw": True, "fix.refined": True}
    for name in ("fix.raw", "fix.refined"):
        fixes = ran.table(name)
        assert handed_to_iceberg[name].names == fixes.column_names
        assert fixes.num_rows == STORED[name]
        assert fixes.num_columns == 128
        # The native event row starts with its own clocks; a line's own
        # columns stay in logs.messages and provenance crosses only as srcuuids.
        assert fixes.column_names[0] == "currunix"
        assert not {"msgthreadid", "loglevel", "body"} & set(fixes.column_names)
        lines = ran.table("logs.messages")
        line_ids = set(lines.column("curruuid").to_pylist())
        # A `fix.raw` row is one frame read off one line, so it names exactly
        # that line. A `fix.refined` row is one event, and the walk folds every
        # observation of it into one row -- so it names every line the event
        # was logged on, which is what joins the event back to all its hops.
        held = [len(source) for source in fixes.column("srcuuids").to_pylist()]
        assert all(count >= 1 for count in held)
        assert (max(held) == 1) is (name == "fix.raw")
        assert set(sources(fixes)) <= line_ids
        assert {"msgtype", "msgseqnum", "curruuid", "crosscode", "srcuuids"} <= set(
            fixes.column_names
        )
        # One fact, one column: tags 44, 38 and 53 are their own columns and
        # no trait's column repeats them.
        assert {"price", "orderqty", "quantity"} <= set(fixes.column_names)
        assert not {"px", "qty", "prevpx", "prevqty"} & set(fixes.column_names)
        # A pair no dictionary explains is an entry of tag 0 in the arrival
        # record, so one record group named after itself closes the row, under
        # the counter that counts it.
        assert fixes.column_names[-3:] == ["metadata", "nofixentries", "fixentries"]
        assert not {
            "35",
            "30001",
            "entries",
            "unmapped",
            "msgCtxId",
            "uuid",
            "unixpartition",
        } & set(fixes.column_names)
        for required in ("beginstring", "currunix", "creaunix", "curruuid", "crossuuid"):
            assert fixes.schema.field(required).nullable is False
            assert fixes.column(required).null_count == 0
        assert {"8", "D"} <= set(fixes.column("msgtype").to_pylist())


def test_a_table_written_before_the_row_grew_gains_the_columns_and_keeps_its_rows(
    ran: Ran,
) -> None:
    """A FIX write merges schema, and the rows a table already held stay.

    A table created without five of the row's columns is written by the run:
    a table carries its own Iceberg ids, and a FIX write merges schema, so
    the table gains five columns and is not rewritten. The row it already
    held stays, read back with the new columns empty, and the run lands
    beside it. This pins `merge_schema` and no migration path: a warehouse
    written under an earlier yggdryl is dropped and replayed from capture,
    because 0.1.10 states another identity for every row.
    """
    grew = ("execunix", "recdunix", "refrecdunix", "noregulatorytradeids", "regulatorytradeids")
    full = fix_message_field().into_arrow_schema()
    before = iceberg_fix_field(
        pyarrow.schema([member for member in full if member.name not in grew], full.metadata),
        "FixMsg",
    )
    assert len(before) == len(full) - len(grew)

    held = before.into_arrow_schema()
    settled = {
        "beginstring": "FIX.4.4",
        EVENT_CLOCK: EPOCH,
        "creaunix": EPOCH,
        MESSAGE_KEY: bytes(15) + b"\x01",
        "crossuuid": bytes(15) + b"\x02",
        "currhashcode": 1,
        "crosshashcode": 2,
    }
    landed = pyarrow.RecordBatch.from_pylist(
        [{**{member.name: None for member in held}, **settled}], schema=held
    )
    store = IcebergCatalog.from_dict(ran.catalog)
    dataset = store.dataset("fix.raw", field=before)
    try:
        assert (
            dataset.append_arrow_reader(
                pyarrow.RecordBatchReader.from_batches(held, [landed]), before
            )
            == 1
        )
    finally:
        dataset.close()
        store.close()

    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    result = ran.task("parse_fix_raw", **WINDOW)

    raw = ran.table("fix.raw")
    assert counted(result) == FIRST["parse_fix_raw"]
    assert raw.num_columns == len(full)
    assert raw.num_rows == STORED["fix.raw"] + 1, "the row it already held is still there"
    assert settled[MESSAGE_KEY] in raw.column(MESSAGE_KEY).to_pylist()
    kept = raw.filter(pyarrow.compute.equal(raw.column(MESSAGE_KEY), settled[MESSAGE_KEY]))
    for column in grew:
        assert kept.column(column).to_pylist() in ([None], [[]]), column
    # And the walk reads the widened table back without noticing the seam.
    walked = ran.task("parse_fix_refined", **WINDOW)
    assert walked["read"] == STORED["fix.raw"] + 1
    assert ran.table("fix.refined").num_columns == len(full)


def test_the_clocks_and_the_group_the_row_grew_reach_the_stored_table(ran: Ran) -> None:
    """The three clocks and the regulatory group are the table's, not just the schema's.

    A column a capture never fills is a column nobody would notice going
    missing, so this reads the stored tables rather than the declaration:
    the execution clock the bridge states, the two recording clocks the
    line's own instant fills -- the parse dates each message's recording by
    the line it was read off, and the walk keeps the earliest observation in
    `recdunix` and the reference it merged on in `refrecdunix` -- and the
    regulatory identifiers it carries, a repeating group persisted whole,
    which is the one shape a scalar column cannot hold and the one Iceberg
    has to round-trip as written.
    """
    ran.workflow()

    for name in ("fix.raw", "fix.refined"):
        fixes = ran.table(name)
        for column in ("execunix", "recdunix", "refrecdunix"):
            assert fixes.schema.field(column).type == pyarrow.timestamp("us", tz="UTC"), column
        assert fixes.column("execunix").null_count < fixes.num_rows, "the bridge states these"
        recorded = fixes.select(("recdunix", "refrecdunix")).to_pylist()
        # A parsed row's recording is its one line's clock; a walked row's
        # is the earliest of the lines its event was logged on, and the
        # reference is the latest, so the two never cross. The one row no
        # line recorded is the expiry the walk emitted, and it states none.
        unrecorded = [row for row in recorded if row["recdunix"] is None]
        assert len(unrecorded) == (0 if name == "fix.raw" else 1)
        assert all(row["refrecdunix"] is None for row in unrecorded)
        assert all(
            row["recdunix"] <= row["refrecdunix"] for row in recorded if row["recdunix"] is not None
        )
        if name == "fix.raw":
            assert all(row["recdunix"] == row["refrecdunix"] for row in recorded)

        # The fourth group persisted whole, beside the counter that counts it.
        occurrences = fixes.column("regulatorytradeids").to_pylist()
        members = fixes.schema.field("regulatorytradeids").type.value_type
        assert "regulatorytradeid" in members.names
        assert any(occurrences), "this capture carries regulatory identifiers"
        for held, counted in zip(
            occurrences, fixes.column("noregulatorytradeids").to_pylist(), strict=True
        ):
            # A counter states what it counts; where the parse could describe
            # no occurrence the count goes to the residual record with them,
            # and the column reads back as the empty group it is.
            assert counted is None or len(held or ()) == counted


def test_both_fix_tables_are_laid_out_exactly_alike_by_the_event(ran: Ran) -> None:
    """Read back off Iceberg's own metadata and not off Arrow field metadata:
    the identifier fields are exactly `curruuid`, the partition spec is
    exactly one hour on `currunix`, and the sort order is the three columns
    the field declares, on both tables."""
    ran.workflow()

    for name in ("fix.raw", "fix.refined"):
        assert ran.layout(name) == {
            "key": {"curruuid"},
            "spec": [("currunix", "hour")],
            "sort": [("currunix", "identity"), ("seqnum", "identity"), ("curruuid", "identity")],
        }, name
        assert ran.partitions(name) == {"currunix": "hour"}
    # A stored line is laid out by the same clock under the same transform and
    # keyed by its own identity, at a different grain from the FIX identity.
    assert ran.layout("logs.messages")["key"] == {"curruuid"}
    assert ran.layout("logs.messages")["spec"] == [(EVENT_CLOCK, "hour")]


def test_the_lineage_holds_across_the_three_steps(ran: Ran) -> None:
    """The point of the split, over the committed capture.

    A message's `srcuuids` is the `curruuid` of the stored line it was parsed
    out of -- provenance, never lineage, and no walk moves it. A refined
    message's `prevuuid` and `parentuuids` are the `curruuid` of the messages
    before it in its chain. And `fix.raw` carries no chain at all: nothing has
    walked yet.
    """
    ran.workflow()
    lines = ran.table("logs.messages")
    raw = ran.table("fix.raw")
    refined = ran.table("fix.refined")
    named = set(lines.column("curruuid").to_pylist())
    assert len(named) == lines.num_rows, "a stored line has one identity"

    assert all(len(source) == 1 for source in raw.column(SOURCES).to_pylist()), (
        "one line per parsed message"
    )
    # The walk folds the observations of one event, so a refined row names
    # every line that event was logged on -- and every one of them is a line
    # this run stored.
    observed = refined.column(SOURCES).to_pylist()
    assert all(source for source in observed), "every event names the lines it was read from"
    assert {line for source in observed for line in source} <= named
    for rows in (raw, refined):
        assert set(sources(rows)) <= named
        assert not {"msgthreadid", "loglevel", "body"} & set(rows.column_names)

    # fix.raw: every row at step none, following nobody, at the instant the
    # parse settled -- the pin where the message stated no sending clock.
    assert raw.column("seqnum").null_count == raw.num_rows
    assert raw.column("prevuuid").null_count == raw.num_rows
    assert all(not parents for parents in raw.column("parentuuids").to_pylist())
    assert sum(1 for at in raw.column(EVENT_CLOCK).to_pylist() if at == UNDATED) == PINNED

    # fix.refined: the chain, and every step it names is a row this table holds.
    identities = set(refined.column(MESSAGE_KEY).to_pylist())
    followed = [
        row
        for row in refined.select((MESSAGE_KEY, "prevuuid", "parentuuids", "seqnum")).to_pylist()
        if row["prevuuid"] is not None
    ]
    assert followed
    assert all(row["prevuuid"] in identities for row in followed)
    assert all(row["prevuuid"] in row["parentuuids"] for row in followed)
    assert all(all(parent in identities for parent in row["parentuuids"]) for row in followed)
    assert all(row["seqnum"] >= 1 for row in followed)
    assert not any(at == UNDATED for at in refined.column(EVENT_CLOCK).to_pylist())
    # The walk folds the observations of one event into one row, re-settles
    # the identities it dates and emits the expiry the chain states, so the
    # two tables hold different identities.
    assert len(identities) == STORED["fix.refined"]
    assert len(set(raw.column(MESSAGE_KEY).to_pylist())) == STORED["fix.raw"]
    assert identities != set(raw.column(MESSAGE_KEY).to_pylist())
    # The state and the clocks a walk folds forward are filled on every row.
    for column in ("state", "creaunix"):
        assert refined.column(column).null_count == 0, column


def test_a_message_logged_at_every_hop_lands_once(ran: Ran) -> None:
    """The whole deduplication story, as a count.

    The capture states 79 messages; storage keeps 49 parsed events, and the
    lifecycle merges the observations of one event and adds one expiry event.
    The primary key is the event's identity, so what either table holds is the
    events -- and a second write of the same capture replaces them rather than
    adding a second copy of each.
    """
    ran.workflow()
    refined = ran.table("fix.refined")
    chain = refined.filter(pyarrow.compute.equal(refined.column("crosscode"), CHAIN))

    assert chain.num_rows == CHAIN_EVENTS
    assert len(set(chain.column(MESSAGE_KEY).to_pylist())) == CHAIN_EVENTS
    assert max(step or 0 for step in chain.column("seqnum").to_pylist()) == CHAIN_LAST_STEP
    lines = ran.table("logs.messages")
    assert lines.num_rows == STORED["logs.messages"]
    raw = ran.table("fix.raw")

    ran.workflow()

    assert ran.rows() == STORED, "a second write of the same capture adds no row"
    for name, before in (("fix.raw", raw), ("fix.refined", refined)):
        assert sorted(ran.table(name).column(MESSAGE_KEY).to_pylist()) == sorted(
            before.column(MESSAGE_KEY).to_pylist()
        ), f"the second write of {name} replaced its rows with the same identities"


def test_the_native_identity_tells_exact_repeats_apart(ran: Ran) -> None:
    """A line is an event and the table is keyed on its identity alone, so a
    capture that prints the same bytes three times has to land three rows.
    The native code digests the line's object and row number beside its body,
    so the code is the line's and not its bytes', and the identity derived
    from it tells the repeats apart."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    lines = ran.table("logs.messages")

    assert lines.num_rows == STORED["logs.messages"] == 144, "every physical line lands"
    codes = lines.column("currhashcode").to_pylist()
    assert lines.schema.field("currhashcode").type == pyarrow.int64()
    # Three lines repeat another's bytes exactly, and the code still tells
    # them apart: the read digests the line's row number and the object it
    # was read from beside its body, so a code is a line's and not its bytes'.
    assert len(set(codes)) == 144
    assert len(set(lines.column("curruuid").to_pylist())) == lines.num_rows
    # A key is scoped to its partition, which here is the hour the line was
    # printed in.
    within = lines.append_column(
        "hour", pyarrow.compute.floor_temporal(lines.column(EVENT_CLOCK), unit="hour")
    )
    grouped = within.group_by(["hour", "curruuid"]).aggregate([([], "count_all")])
    assert grouped.num_rows == lines.num_rows
    assert ran.partitions("logs.messages") == {EVENT_CLOCK: "hour"}
    assert ran.layout("logs.messages")["key"] == {"curruuid"}


def test_a_chain_read_back_in_order_states_what_each_step_follows(ran: Ran) -> None:
    """`currunix, seqnum` is the order the walk gave the chain, and a step read
    back in it names the step before it, which never comes later."""
    ran.workflow()
    refined = ran.table("fix.refined")
    chain = refined.filter(pyarrow.compute.equal(refined.column("crosscode"), CHAIN)).sort_by(
        [(EVENT_CLOCK, "ascending"), ("seqnum", "ascending")]
    )

    steps = chain.select((MESSAGE_KEY, "prevuuid", "seqnum", "state", EVENT_CLOCK)).to_pylist()
    assert len(steps) == CHAIN_EVENTS
    held = {step[MESSAGE_KEY]: step for step in steps}
    followed = [step for step in steps if step["prevuuid"] is not None]
    assert followed, "a walked chain has steps that follow one another"
    for step in followed:
        assert step["prevuuid"] in held, "a step follows one this table holds"
        before = held[step["prevuuid"]]
        assert before[EVENT_CLOCK] <= step[EVENT_CLOCK], "a step never precedes what it follows"
        assert before["seqnum"] is None or before["seqnum"] < step["seqnum"]


def test_the_refined_window_walks_what_the_parse_left_at_the_pin(ran: Ran) -> None:
    """A `fix.raw` row the parse could not date sits at the codec's pin, outside
    any day, and the refined window reads it there by the transaction time the
    walk dates it with: a day's run walks the day's events, dated or pinned."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    ran.task("parse_fix_raw", **WINDOW)
    raw = ran.table("fix.raw")
    pinned = raw.filter(pyarrow.compute.equal(raw.column(EVENT_CLOCK), UNDATED))
    assert pinned.num_rows == PINNED
    assert pinned.column("transacttime").null_count == 0, "each states the clock the walk reads"

    refined = ran.task("parse_fix_refined", **WINDOW)

    assert refined["read"] == raw.num_rows, "the pinned rows are the day's too"
    # A window the fixture falls outside walks nothing: neither the dated rows
    # nor the pinned ones belong to it.
    elsewhere = ran.task("parse_fix_refined", start="2026-08-15", end="2026-08-15")
    assert counted(elsewhere) == {"read": 0, "written": 0, "skipped": 0}


def test_maintenance_visits_every_table_and_reports_what_it_changed(ran: Ran) -> None:
    """The first pass settles what is fragmented; the second finds nothing."""
    ran.workflow()

    first = ran.task("optimize_iceberg")

    assert first["task"] == "optimize_iceberg"
    assert first["tables"] == len(STORED)
    assert set(first["reports"]) == set(STORED)
    # The capture spans several hours, so each table lands one small file per
    # hour and the first pass settles them.
    assert first["reports"]["logs.messages"]["rewritten"] > 0
    assert first["reports"]["fix.refined"]["rewritten"] > 0
    assert counted(first)["read"] == len(STORED)
    assert ran.rows() == STORED, "compaction rewrites rows, it never drops them"

    second = ran.task("optimize_iceberg")

    assert counted(second) == {
        "read": len(STORED),
        "written": 0,
        "skipped": len(STORED),
    }, json.dumps(second, indent=2)
    assert all(report["rewritten"] == 0 for report in second["reports"].values())
    assert ran.rows() == STORED, "a settled catalog is left as it was"
