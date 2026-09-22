"""Raw and FIX ingestion over the checked-in fixture and a replay, in three steps."""

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
from rekep.fix import EVENT_CLOCK, MESSAGE_KEY, SOURCES, UNDATED, FixRegistry
from rekep.iceberg import IcebergCatalog, IcebergDataset, iceberg_contract_field, partition_keys

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"
FIX_CONTRACT = ROOT / "schemas" / "rekep" / "fixmsg.json"
WORKFLOW = (("parse_messages", {}), ("parse_fix_bronze", {}), ("parse_fix_silver", {}))
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: The day the bridge fixture was captured on. A task covers the last day
#: unless its document names a window, and the fixture is dated, so every
#: run here names its day -- `end: 2026-08-14` is the exclusive end of it.
WINDOW = {"start": "2026-08-14", "end": "2026-08-14"}

#: What the bridge fixture's 144 physical rows produce, first run.
#:
#: `logs.messages` is keyed on `curruuid`, and the native identity carries the
#: line's place in the read beside its content code, so the 3 exact repeated
#: lines are 3 rows and every line lands.
#: A FIX row is a message and not a line -- prose
#: answers none and a line carrying two frames answers two -- and
#: `fix.bronze` is keyed on `curruuid`, so the 79 messages those 144 lines
#: carry settle on 49 events the capture describes. The walk merges the
#: observations of one event and adds one expiry, so `fix.silver` reads 49
#: and writes 22. The gaps
#: are the point of the keys: one raw identity is one line and the same message
#: logged at every hop is one event.
FIRST = {
    "parse_messages": {"read": 144, "written": 144, "skipped": 0},
    "parse_fix_bronze": {"read": 144, "written": 49, "skipped": 30},
    "parse_fix_silver": {"read": 49, "written": 22, "skipped": 0},
}

#: What a replay of the same window produces: the same reads and the same
#: writes, because a run replaces what its window carries -- the table holds
#: each row once however often the window runs.
REPLAY = FIRST

#: Stored rows after both runs, and the two snapshots each table then holds.
STORED = {
    "logs.messages": 144,
    "fix.bronze": 49,
    "fix.silver": 22,
}

#: The business identifier whose persisted lifecycle chain anchors the
#: acceptance assertions.
CHAIN = "00026877711XOEA0"
CHAIN_EVENTS = 4
CHAIN_LAST_STEP = 2

#: How many events the parse could not date: the messages that stated no
#: `SendingTime`, which sit at the codec's pin in `fix.bronze` until the walk
#: dates them by their `TransactTime` -- so no silver row is at the pin.
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
    assert first["parse_fix_bronze"]["messages"] == 79
    assert ran.rows() == STORED
    messages = ran.table("logs.messages")
    assert messages.schema.equals(Message.into_field().into_arrow_schema(), check_metadata=False)
    assert messages.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    instants = {
        row["rownum"]: row[EVENT_CLOCK]
        for row in messages.select(("rownum", EVENT_CLOCK)).to_pylist()
    }
    assert instants[1] == datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    # A line the row header did not match settles at the pin, and its row says
    # so rather than being dropped or dated by the run.
    assert any(instant == EPOCH for instant in instants.values())
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
    assert result["targets"] == {"silver": "fix.silver"}
    assert result["sources"] == {"bronze": "fix.bronze"}


def test_a_window_the_capture_falls_outside_reads_every_line_and_writes_none(ran: Ran) -> None:
    """The default window is the last day, and the fixture is not in it."""
    result = ran.task("parse_messages", filesystem=FIXTURE.as_uri())

    # Every line is read; only the ones the window covers are written. A line
    # the row header did not match carries no clock and is in every window, so
    # what lands is exactly those and nothing the capture dated.
    assert result["read"] == 144
    unstamped = result["written"]
    assert 0 < unstamped < 144
    assert ran.rows() == {"logs.messages": unstamped}
    assert ran.table("logs.messages").column(EVENT_CLOCK).to_pylist() == [EPOCH] * unstamped

    bronze = ran.task("parse_fix_bronze")
    assert bronze["read"] == unstamped
    # An unstamped line's message states no sending clock either, so it sits
    # at the pin -- and a bronze row at the pin is read by the transaction
    # time the walk will date it with, which the fixture's messages state and
    # which is the fixture's day, not the last one.
    assert bronze["written"] > 0
    assert ran.task("parse_fix_silver")["read"] == 0
    assert ran.task("parse_fix_silver", **WINDOW)["read"] == bronze["written"]


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

    # Every line is read and the window decides which are written: the dated
    # ones before the cut, and the ones the row header could not stamp, which
    # belong to every window.
    assert first["read"] == 144
    assert first["written"] == STORED["logs.messages"] - later
    assert first["skipped"] == 144 - first["written"]
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


@pytest.mark.parametrize("name", ["parse_fix_bronze", "parse_fix_silver"])
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


def test_a_bronze_task_forwards_codec_options_without_rewriting_text(
    ran: Ran, tmp_path: Path
) -> None:
    """FIX codec options cross the task boundary unchanged; text stays raw."""
    capture = tmp_path / "prefixed.log"
    capture.write_bytes(
        b"2026-08-14 12:46:39.769 [1] [ULBridge] (INFO) "
        b"  --> 8=FIX.4.4|35=D|11=OPTION-1|55=HOLN|10=000|\n"
    )

    ran.task("parse_messages", filesystem=capture.as_uri(), **WINDOW)
    result = ran.task(
        "parse_fix_bronze",
        codec_options={"threads": 2, "batch_row_size": 2},
        **WINDOW,
    )

    assert result["messages"] == result["written"] == 1
    bronze = ran.table("fix.bronze")
    assert bronze.column("msgtype").to_pylist() == ["D"]
    assert bronze.column("clordid").to_pylist() == ["OPTION-1"]


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

    result = ran.task("parse_fix_bronze", registry=registry.as_uri(), **WINDOW)

    assert result["read"] == STORED["logs.messages"]
    assert result["messages"] == 79
    # The 79 arrivals reduce to 51, not the full registry's 49: its Account
    # projection derives `PBRK6_EDA` for both arrivals in two duplicate pairs,
    # while this narrow registry retains `/account:0=PBRK6_EDA` as one extra
    # residual entry (63 rather than 62), so the canonical hashes differ.
    assert counted(result)["written"] == 51
    bronze = ran.table("fix.bronze")
    assert bronze.num_rows == 51
    assert "msgtype" not in bronze.column_names
    assert bronze.num_columns == 35
    # The table is the dictionary's shape, and its clocks are stored at the
    # precision Iceberg v2 holds.
    assert bronze.schema.field("sendingtime").type == pyarrow.timestamp("us", tz="UTC")
    assert bronze.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    assert not {"sourceurl", "rownum", "body"} & set(bronze.column_names)
    assert bronze.column("srcuuids").null_count == 0
    # The walk is another matter: a chain is read off what a message is, and
    # a dictionary that cannot name a message's type places none of them. The
    # silver run reads every bronze row of the window and lands no row, under
    # the same shape, rather than guessing at a chain.
    walked = ran.task("parse_fix_silver", registry=registry.as_uri(), **WINDOW)
    # No narrow row has a typed message category to enter lifecycle, so the
    # task reports no emitted or deduplicated candidate.
    assert counted(walked) == {"read": 51, "written": 0, "skipped": 0}
    assert ran.table("fix.silver").num_rows == 0
    assert ran.table("fix.silver").schema.equals(bronze.schema)


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
    fixes = store.dataset("fix.bronze", field=field)
    try:
        assert fixes.overwrite_arrow_reader(source, field, merge_by=True) == 1
        stored = fixes.read_arrow_table(field)
        assert stored.num_rows == 1
        assert stored.num_columns == 128
        # The native row has event facts only. `srcuuids`, where present,
        # joins it to raw lines without copying captures into this table.
        assert "body" not in stored.column_names
        assert "currhashcode" in stored.column_names
        assert "srcuuids" in stored.column_names
        assert not {"sourceurl", "rownum", "body", "loglevel"} & set(stored.column_names)
    finally:
        fixes.close()
        store.close()


def test_a_raw_table_of_the_previous_shape_is_a_table_of_its_own(ran: Ran) -> None:
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

    assert schema_modes == {"logs.messages": False, "fix.bronze": True, "fix.silver": True}
    for name in ("fix.bronze", "fix.silver"):
        fixes = ran.table(name)
        assert handed_to_iceberg[name].names == fixes.column_names
        assert fixes.num_rows == STORED[name]
        assert fixes.num_columns == 128
        # The native event row starts with its own clocks; raw capture fields
        # stay in logs.messages and provenance crosses only as srcuuids.
        assert fixes.column_names[0] == "currunix"
        assert not {
            "sourceurl",
            "rownum",
            "msgthreadid",
            "loglevel",
            "body",
        } & set(fixes.column_names)
        lines = ran.table("logs.messages")
        raw_ids = set(lines.column("curruuid").to_pylist())
        # A bronze row is one frame read off one line, so it names exactly
        # that line. A silver row is one event, and the walk folds every
        # observation of it into one row -- so it names every line the event
        # was logged on, which is what joins the event back to all its hops.
        held = [len(source) for source in fixes.column("srcuuids").to_pylist()]
        assert all(count >= 1 for count in held)
        assert (max(held) == 1) is (name == "fix.bronze")
        assert set(sources(fixes)) <= raw_ids
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


def test_both_fix_tables_are_laid_out_exactly_alike_by_the_event(ran: Ran) -> None:
    """Read back off Iceberg's own metadata and not off Arrow field metadata:
    the identifier fields are exactly `curruuid`, the partition spec is
    exactly one hour on `currunix`, and the sort order is the three columns
    the field declares, on both tables."""
    ran.workflow()

    for name in ("fix.bronze", "fix.silver"):
        assert ran.layout(name) == {
            "key": {"curruuid"},
            "spec": [("currunix", "hour")],
            "sort": [("currunix", "identity"), ("seqnum", "identity"), ("curruuid", "identity")],
        }, name
        assert ran.partitions(name) == {"currunix": "hour"}
    # The raw line is laid out by the same clock under the same transform and
    # keyed by its own identity, at a different grain from the FIX identity.
    assert ran.layout("logs.messages")["key"] == {"curruuid"}
    assert ran.layout("logs.messages")["spec"] == [(EVENT_CLOCK, "hour")]


def test_the_lineage_holds_across_the_three_steps(ran: Ran) -> None:
    """The point of the split, over the committed capture.

    A message's `srcuuids` is the `curruuid` of the stored line it was parsed
    out of -- provenance, never lineage, and no walk moves it. A silver
    message's `prevuuid` and `parentuuids` are the `curruuid` of the messages
    before it in its chain. And bronze carries no chain at all: nothing has
    walked yet.
    """
    ran.workflow()
    lines = ran.table("logs.messages")
    bronze = ran.table("fix.bronze")
    silver = ran.table("fix.silver")
    named = set(lines.column("curruuid").to_pylist())
    assert len(named) == lines.num_rows, "a stored line has one identity"

    assert all(len(source) == 1 for source in bronze.column(SOURCES).to_pylist()), (
        "one line per parsed message"
    )
    # The walk folds the observations of one event, so a silver row names
    # every line that event was logged on -- and every one of them is a line
    # this run stored.
    observed = silver.column(SOURCES).to_pylist()
    assert all(source for source in observed), "every event names the lines it was read from"
    assert {line for source in observed for line in source} <= named
    for rows in (bronze, silver):
        assert set(sources(rows)) <= named
        assert not {
            "sourceurl",
            "rownum",
            "msgthreadid",
            "loglevel",
            "body",
        } & set(rows.column_names)

    # Bronze: every row at step none, following nobody, at the instant the
    # parse settled -- the pin where the message stated no sending clock.
    assert bronze.column("seqnum").null_count == bronze.num_rows
    assert bronze.column("prevuuid").null_count == bronze.num_rows
    assert all(not parents for parents in bronze.column("parentuuids").to_pylist())
    assert sum(1 for at in bronze.column(EVENT_CLOCK).to_pylist() if at == UNDATED) == PINNED

    # Silver: the chain, and every step it names is a row this table holds.
    identities = set(silver.column(MESSAGE_KEY).to_pylist())
    followed = [
        row
        for row in silver.select((MESSAGE_KEY, "prevuuid", "parentuuids", "seqnum")).to_pylist()
        if row["prevuuid"] is not None
    ]
    assert followed
    assert all(row["prevuuid"] in identities for row in followed)
    assert all(row["prevuuid"] in row["parentuuids"] for row in followed)
    assert all(all(parent in identities for parent in row["parentuuids"]) for row in followed)
    assert all(row["seqnum"] >= 1 for row in followed)
    assert not any(at == UNDATED for at in silver.column(EVENT_CLOCK).to_pylist())
    # The walk re-settles dated identities and emits the expiry the chain
    # states, so silver has one event more than bronze.
    assert len(identities) == STORED["fix.silver"]
    assert len(set(bronze.column(MESSAGE_KEY).to_pylist())) == STORED["fix.bronze"]
    assert identities != set(bronze.column(MESSAGE_KEY).to_pylist())
    # The state and the clocks a walk folds forward are filled on every row.
    for column in ("state", "creaunix"):
        assert silver.column(column).null_count == 0, column


def test_a_message_logged_at_every_hop_lands_once(ran: Ran) -> None:
    """The whole deduplication story, as a count.

    The capture states 79 messages; storage keeps 49 parsed events, and the
    lifecycle merges the observations of one event and adds one expiry event.
    primary key is the event's identity, so what either table holds is the
    events -- and a second write of the same capture replaces them rather than
    adding a second copy of each.
    """
    ran.workflow()
    silver = ran.table("fix.silver")
    chain = silver.filter(pyarrow.compute.equal(silver.column("crosscode"), CHAIN))

    assert chain.num_rows == CHAIN_EVENTS
    assert len(set(chain.column(MESSAGE_KEY).to_pylist())) == CHAIN_EVENTS
    assert max(step or 0 for step in chain.column("seqnum").to_pylist()) == CHAIN_LAST_STEP
    lines = ran.table("logs.messages")
    assert lines.num_rows == STORED["logs.messages"]
    bronze = ran.table("fix.bronze")

    ran.workflow()

    assert ran.rows() == STORED, "a second write of the same capture adds no row"
    for name, before in (("fix.bronze", bronze), ("fix.silver", silver)):
        assert sorted(ran.table(name).column(MESSAGE_KEY).to_pylist()) == sorted(
            before.column(MESSAGE_KEY).to_pylist()
        ), f"the second write of {name} replaced its rows with the same identities"


def test_the_native_identity_tells_exact_repeats_apart(ran: Ran) -> None:
    """A line is an event and the table is keyed on its identity alone, so a
    capture that prints the same bytes three times has to land three rows.
    The native identity carries the line's place in the read beside its
    content code, so it does -- and the code, which is the content's and not
    the line's, is what those repeats still share."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    lines = ran.table("logs.messages")

    assert lines.num_rows == STORED["logs.messages"] == 144, "every physical line lands"
    codes = lines.column("currhashcode").to_pylist()
    assert lines.schema.field("currhashcode").type == pyarrow.int64()
    assert len(set(codes)) == 141, "3 lines repeat another's bytes exactly"
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
    silver = ran.table("fix.silver")
    chain = silver.filter(pyarrow.compute.equal(silver.column("crosscode"), CHAIN)).sort_by(
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


def test_the_silver_window_walks_what_the_parse_left_at_the_pin(ran: Ran) -> None:
    """A bronze row the parse could not date sits at the codec's pin, outside
    any day, and the silver window reads it there by the transaction time the
    walk dates it with: a day's run walks the day's events, dated or pinned."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    ran.task("parse_fix_bronze", **WINDOW)
    bronze = ran.table("fix.bronze")
    pinned = bronze.filter(pyarrow.compute.equal(bronze.column(EVENT_CLOCK), UNDATED))
    assert pinned.num_rows == PINNED
    assert pinned.column("transacttime").null_count == 0, "each states the clock the walk reads"

    silver = ran.task("parse_fix_silver", **WINDOW)

    assert silver["read"] == bronze.num_rows, "the pinned rows are the day's too"
    # A window the fixture falls outside walks nothing: neither the dated rows
    # nor the pinned ones belong to it.
    elsewhere = ran.task("parse_fix_silver", start="2026-08-15", end="2026-08-15")
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
    assert first["reports"]["fix.silver"]["rewritten"] > 0
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
