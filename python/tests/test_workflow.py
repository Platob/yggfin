"""Raw and FIX ingestion over the checked-in fixture and a replay."""

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
from rekep.fix import FixRegistry
from rekep.iceberg import IcebergCatalog, IcebergDataset, iceberg_contract_field, partition_keys

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"
FIX_CONTRACT = ROOT / "schemas" / "rekep" / "fix-message.json"
WORKFLOW = (("parse_messages", {}), ("parse_fix", {}))
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: The day the bridge fixture was captured on. A task covers the last day
#: unless its document names a window, and the fixture is dated, so every
#: run here names its day -- `end: 2026-08-14` is the exclusive end of it.
WINDOW = {"start": "2026-08-14", "end": "2026-08-14"}

#: What the bridge fixture's 144 physical rows produce, first run.
#:
#: `logs.messages` is keyed on `bodyhash`, so the 22 lines whose bodies repeat
#: another line's exactly are one row each with the line they repeat. A FIX row
#: is a message and not a line -- prose answers none and a line carrying two
#: frames answers two -- and `fix.messages` is keyed on `curruuid`, so the 76
#: messages those 122 lines carry settle on the 53 events the capture
#: describes. Both gaps are the point of the two keys: the same bytes are one
#: line and the same message logged at every hop is one event.
FIRST = {
    "parse_messages": {"read": 144, "written": 122, "skipped": 22},
    "parse_fix": {"read": 122, "written": 53, "skipped": 23},
}

#: What a replay of the same window produces: the same reads and the same
#: writes, because a run replaces what its window carries -- the table holds
#: each row once however often the window runs.
REPLAY = FIRST

#: Stored rows after both runs, and the two snapshots each table then holds.
STORED = {
    "logs.messages": 122,
    "fix.messages": 53,
}

#: The chain the acceptance numbers are read off: 49 of the capture's messages
#: state it, and they restate 31 events between them. `python/tests/test_fix.py`
#: holds every chain; this holds the one the table has to fold.
CHAIN = "00026877711XOEA0"
CHAIN_MESSAGES = 49
CHAIN_EVENTS = 31


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


def test_the_workflow_publishes_ulbridge_and_a_replay_writes_nothing(ran: Ran) -> None:
    first = ran.workflow()
    assert {name: counted(result) for name, result in first.items()} == FIRST
    assert ran.rows() == STORED
    messages = ran.table("logs.messages")
    assert messages.schema.equals(Message.into_field().into_arrow_schema(), check_metadata=False)
    assert messages.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
    timestamps = {
        row["rownum"]: row["timestamp"]
        for row in messages.select(("rownum", "timestamp")).to_pylist()
    }
    assert timestamps[1] == datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    # A line the row header did not match carries no clock, and its row says
    # so rather than being dropped or dated by the run.
    assert any(timestamp is None for timestamp in timestamps.values())

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
    assert ran.table("logs.messages").column("timestamp").null_count == unstamped

    fixes = ran.task("parse_fix")
    assert fixes["read"] == unstamped


def test_a_window_replaces_only_the_lines_it_covers(ran: Ran) -> None:
    """A run over the whole day and then one over its first part leave every
    line once: the second run replaces the lines its window covers and no
    other. The fixture's lines straddle one second, which is where it cuts."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    stored = ran.table("logs.messages")
    cut = datetime.datetime(2026, 8, 14, 14, 46, 40, tzinfo=UTC)
    clock = stored.column("timestamp")
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


def test_parse_fix_refuses_an_empty_registry_before_creating_a_table(
    ran: Ran, tmp_path: Path
) -> None:
    registry = tmp_path / "empty-fix-registry"
    registry.mkdir()
    argv = [
        "task",
        "run",
        str(ROOT / "tasks" / "parse_fix" / "parse_fix.json"),
        "--parameter",
        f"catalog={json.dumps(ran.catalog)}",
        "--parameter",
        f"registry={json.dumps(registry.as_uri())}",
    ]

    assert cli.main(argv) == 1
    assert ran.rows() == {}, "a missing dictionary cannot leave a narrow FIX table"


def test_a_dictionary_that_types_no_message_answers_no_event(ran: Ran, tmp_path: Path) -> None:
    """A venue stamps nanoseconds; Iceberg v2 holds microseconds. And a
    dictionary narrow enough to type no message type at all leaves a table
    with its shape and none of its rows.

    The second half is the message-type filter, which is on by default: the
    codec refuses `Heartbeat(0)`, `TestRequest(1)` and the untyped line, and
    every line of this capture is untyped to a dictionary that cannot resolve
    `MsgType(35)`. Turning it off is `exclude_msgtypes=[]` on the codec, which
    the Python bindings do not expose today, so a run under a dictionary this
    narrow reports zero rather than offering a way to keep them.
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

    result = ran.task("parse_fix", registry=registry.as_uri(), **WINDOW)

    assert result["read"] == STORED["logs.messages"]
    assert counted(result)["written"] == 0
    fixes = ran.table("fix.messages")
    assert fixes.num_rows == 0
    # The table is still the dictionary's shape, and its clocks are stored at
    # the precision Iceberg v2 holds.
    assert fixes.schema.field("sendingtime").type == pyarrow.timestamp("us", tz="UTC")
    assert fixes.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
    assert fixes.schema.field("unix").type == pyarrow.timestamp("us", tz="UTC")


def test_dumped_fix_schema_can_stream_a_mock_row_through_iceberg(ran: Ran) -> None:
    field = iceberg_contract_field(FIX_CONTRACT.read_text(encoding="utf-8"), "FixMsg")
    # The published contract carries the partition spec, so a table built from
    # the document alone is laid out the way `parse_fix` lays its own out.
    assert partition_keys(field) == {"unix": "hour"}
    schema = field.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [
            {
                "sourceurl": "file:///mock/fix.log",
                "rownum": 1,
                "bodyhash": uuid.UUID(int=3).bytes,
                "body": b"8=FIX.4.4|35=D|10=0|",
                "beginstring": "FIX.4.4",
                "timestamp": EPOCH,
                # The settled bundle a replayable row always carries. Only a
                # walk stamps `snapunix`, so this row leaves it null.
                "unix": EPOCH,
                "creatunix": EPOCH,
                "hashcode": 1,
                "crosshashcode": 2,
                "curruuid": uuid.UUID(int=1).bytes,
                "crossuuid": uuid.UUID(int=2).bytes,
            }
        ],
        schema=schema,
    )
    source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
    store = IcebergCatalog.from_dict(ran.catalog)
    fixes = store.dataset("fix.messages", field=field)
    try:
        assert fixes.overwrite_arrow_reader(source, field, merge_by=True) == 1
        stored = fixes.read_arrow_table(field)
        assert stored.num_rows == 1
        assert stored.num_columns == 130
        assert stored.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
        assert stored.select(("sourceurl", "rownum", "body")).to_pylist() == [
            {
                "sourceurl": "file:///mock/fix.log",
                "rownum": 1,
                "body": b"8=FIX.4.4|35=D|10=0|",
            }
        ]
    finally:
        fixes.close()
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
            ("timepartition_hour", "hour")
        ]
        assert {
            row["partition"]["timepartition_hour"] for row in messages.data_files().to_pylist()
        } == {int(first.timestamp() // 3600), int(second.timestamp() // 3600)}

        rows = []
        for timestamp in (first, second):
            assert messages.scan_plan(EqualTo("timepartition", timestamp))["skipped"] == 1
            reader = messages.read_arrow_reader(
                Message.into_field(), row_filter=EqualTo("timepartition", timestamp)
            )
            try:
                assert isinstance(reader, pyarrow.RecordBatchReader)
                rows.extend(row for batch in reader for row in batch.to_pylist())
            finally:
                reader.close()
    finally:
        messages.close()
        store.close()

    assert {row["timestamp"] for row in rows} == {first, second}
    assert {row["timepartition"] for row in rows} == {first, second}
    assert {row["pluginid"] for row in rows} == {"ULBridge"}


def test_ulbridge_messages_flow_directly_through_the_fix_codec(
    ran: Ran,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handed_to_iceberg: list[pyarrow.Schema] = []
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
        if dataset.identifier == "fix.messages":
            handed_to_iceberg.append(source.schema)
        return replace(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", observed_replace)
    ran.workflow()

    fixes = ran.table("fix.messages")
    assert schema_modes == {"logs.messages": False, "fix.messages": True}
    assert len(handed_to_iceberg) == 1
    assert handed_to_iceberg[0].names == fixes.schema.names
    assert fixes.num_rows == STORED["fix.messages"]
    assert fixes.num_columns == 130
    # The capture's own columns lead the row, the dictionary's follow. A
    # capture named after a field fills that field instead of leading, so
    # `sourceurl`, `msgsessionid`, `msgctxid`, `msgseqnum` and `pluginid` are
    # the message's own columns and `bodyhash` is the line's exact-byte
    # digest. The event's own clocks open the dictionary's half.
    assert fixes.column_names[:10] == [
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "level",
        "bodyhash",
        "body",
        "unix",
        "creatunix",
        "prevunix",
    ]
    assert {"msgtype", "msgseqnum", "curruuid", "crosscode", "sourceurl"} <= set(fixes.column_names)
    # One fact, one column: tags 44, 38 and 53 are read and written through
    # the crate's own `px` and `qty`, and no column repeats them.
    assert not {"price", "orderqty", "quantity"} & set(fixes.column_names)
    # A pair no dictionary explains is an entry of tag 0 in the arrival record,
    # so one record group named after itself closes the row, under the counter
    # that counts it.
    assert fixes.column_names[-3:] == ["metadata", "nofixentries", "fixentries"]
    assert not {"35", "30001", "entries", "unmapped", "msgCtxId", "uuid", "unixpartition"} & set(
        fixes.column_names
    )
    for required in ("beginstring", "unix", "creatunix", "curruuid", "crossuuid"):
        assert fixes.schema.field(required).nullable is False
        assert fixes.column(required).null_count == 0
    # Arrow field metadata is not what Iceberg stores: the hourly transform is
    # in the table's own partition spec, and that is where it is read back.
    assert ran.partitions("fix.messages") == {"unix": "hour"}
    store = IcebergCatalog.from_dict(ran.catalog)
    try:
        table = store.catalog.load_table("fix.messages")
        assert {
            table.schema().find_column_name(field_id)
            for field_id in table.schema().identifier_field_ids
        } == {"curruuid"}
        assert [str(field.transform) for field in table.sort_order().fields] == ["identity"] * 3
    finally:
        store.close()
    msgtypes = set(fixes.column("msgtype").to_pylist())
    assert {"8", "D"} <= msgtypes


def test_a_message_logged_at_every_hop_lands_once(ran: Ran) -> None:
    """The whole deduplication story, as a count.

    The capture states one chain 49 times and those statements are 31 events.
    The primary key is the event's identity, so what the table holds is the
    events -- and a second write of the same capture replaces them rather than
    adding a second copy of each.
    """
    ran.workflow()
    fixes = ran.table("fix.messages")
    chain = fixes.filter(pyarrow.compute.equal(fixes.column("crosscode"), CHAIN))

    assert chain.num_rows == CHAIN_EVENTS
    assert len(set(chain.column("curruuid").to_pylist())) == CHAIN_EVENTS
    # Every arrival is still accounted for: the messages the parse answered
    # minus the rows it wrote is exactly the restatements it folded.
    lines = ran.table("logs.messages")
    assert lines.num_rows == STORED["logs.messages"]

    ran.workflow()

    assert ran.rows() == STORED, "a second write of the same capture adds no row"
    again = ran.table("fix.messages")
    assert sorted(again.column("curruuid").to_pylist()) == sorted(
        fixes.column("curruuid").to_pylist()
    )


def test_two_lines_with_the_same_body_are_one_stored_line(ran: Ran) -> None:
    """`logs.messages` is keyed on the bytes, so a body logged on two sessions
    is one row and the session it kept is one of the two that carried it."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri(), **WINDOW)
    lines = ran.table("logs.messages")

    assert lines.num_rows == STORED["logs.messages"] < 144
    digests = lines.column("bodyhash").to_pylist()
    assert all(len(digest) == 16 for digest in digests)
    # The digest is of the body and of nothing else, so two lines the bridge
    # logged under different sessions collapse onto one row.
    assert len(set(lines.column("msgsessionid").to_pylist())) > 1
    # A key is scoped to its partition, which here is the hour the line was
    # printed in: the same bytes logged twice inside one hour are one row, and
    # the same bytes logged in two hours are two, one in each. So the digests
    # are unique per partition and not across the table.
    within = lines.append_column(
        "hour", pyarrow.compute.floor_temporal(lines.column("timepartition"), unit="hour")
    )
    grouped = within.group_by(["hour", "bodyhash"]).aggregate([([], "count_all")])
    assert grouped.num_rows == lines.num_rows
    assert len(set(digests)) < lines.num_rows, "one body reached two hours"
    assert ran.partitions("logs.messages") == {"timepartition": "hour"}
    store = IcebergCatalog.from_dict(ran.catalog)
    try:
        table = store.catalog.load_table("logs.messages")
        assert {
            table.schema().find_column_name(field_id)
            for field_id in table.schema().identifier_field_ids
        } == {"bodyhash"}
    finally:
        store.close()


def test_a_chain_read_back_in_order_states_what_each_step_follows(ran: Ran) -> None:
    """`unix, seqnum` is the order the walk gave the chain, and a step read
    back in it names the step before it and the price that step settled on."""
    ran.workflow()
    fixes = ran.table("fix.messages")
    chain = fixes.filter(pyarrow.compute.equal(fixes.column("crosscode"), CHAIN)).sort_by(
        [("unix", "ascending"), ("seqnum", "ascending")]
    )

    steps = chain.select(("curruuid", "prevuuid", "seqnum", "px", "prevpx", "unix")).to_pylist()
    assert len(steps) == CHAIN_EVENTS
    held = {step["curruuid"]: step for step in steps}
    followed = [step for step in steps if step["prevuuid"] is not None]
    assert followed, "a chain of 31 events has steps that follow one another"
    for step in followed:
        assert step["prevuuid"] in held, "a step follows one this table holds"
        before = held[step["prevuuid"]]
        assert before["unix"] <= step["unix"], "a step never precedes what it follows"
        if step["prevpx"] is not None:
            assert step["prevpx"] == before["px"]


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
    assert first["reports"]["fix.messages"]["rewritten"] > 0
    assert counted(first)["read"] == 2
    assert ran.rows() == STORED, "compaction rewrites rows, it never drops them"

    second = ran.task("optimize_iceberg")

    assert counted(second) == {
        "read": len(STORED),
        "written": 0,
        "skipped": len(STORED),
    }, json.dumps(second, indent=2)
    assert second["reports"]["logs.messages"]["rewritten"] == 0
    assert second["reports"]["fix.messages"]["rewritten"] == 0
    assert ran.rows() == STORED, "a settled catalog is left as it was"
