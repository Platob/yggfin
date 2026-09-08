"""Raw and FIX ingestion over the checked-in fixture and a replay."""

from __future__ import annotations

import datetime
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow
import pytest
from pyiceberg.expressions import EqualTo

from rekep import Field, Message, cli
from rekep.fix import FixRegistry
from rekep.iceberg import IcebergCatalog, IcebergDataset

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"
FIX_CONTRACT = ROOT / "schemas" / "rekep" / "fix-message.json"
WORKFLOW = (("parse_messages", {}), ("parse_fix", {}))

#: What the bridge fixture's 111 physical rows produce, first run. The FIX
#: codec preserves the one-input-row/one-output-row shape even for prose.
FIRST = {
    "parse_messages": {"read": 111, "written": 111, "skipped": 0},
    "parse_fix": {"read": 111, "written": 111, "skipped": 0},
}

#: What a replay of the same input produces: the same reads, no writes.
REPLAY = {
    name: {"read": counts["read"], "written": 0, "skipped": counts["read"]}
    for name, counts in FIRST.items()
}

#: Stored rows, and the one snapshot each table holds after both runs.
STORED = {
    "logs.messages": 111,
    "fix.messages": 111,
}


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
                first = {"filesystem": FIXTURE.as_uri()}
            else:
                first = {}
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
    assert messages.schema.equals(Message.field().into_arrow_schema(), check_metadata=False)
    assert messages.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
    timestamps = {
        row["rownum"]: row["timestamp"]
        for row in messages.select(("rownum", "timestamp")).to_pylist()
    }
    assert timestamps[1] == datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=datetime.UTC)
    assert all(timestamp is not None for timestamp in timestamps.values())

    replay = ran.workflow()
    assert {name: counted(result) for name, result in replay.items()} == REPLAY
    assert ran.rows() == STORED, "an idempotent replay adds no row"
    assert ran.snapshots() == {name: int(bool(rows)) for name, rows in STORED.items()}


def test_every_result_is_the_shape_a_route_reads(ran: Ran) -> None:
    from rekep.logs import Stage

    for name, result in ran.workflow().items():
        assert Stage.validated(result) == result
        assert result["task"] == name
        assert len(json.dumps(result)) < 4096, "XCom carries a summary, never a payload"


def test_an_empty_capture_is_read_and_produces_nothing(ran: Ran, tmp_path: Path) -> None:
    """Zero rows is a run, not a failure: the route skips what has no input."""
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "quiet.log").write_text("", encoding="utf-8")

    result = ran.task("parse_messages", filesystem=empty.as_uri())

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


def test_parse_fix_narrows_a_nanosecond_clock_iceberg_cannot_store(
    ran: Ran, tmp_path: Path
) -> None:
    """A venue stamps nanoseconds; Iceberg v2 holds microseconds."""
    ran.task("parse_messages", filesystem=FIXTURE.as_uri())
    registry = tmp_path / "nanosecond-fix-registry"
    registry.mkdir()
    sending_time = Field("sendingtime", pyarrow.timestamp("ns", tz="UTC"), nullable=True)
    sending_time.fix.tag = 52
    FixRegistry.from_fields([sending_time]).write_into(registry)

    result = ran.task("parse_fix", registry=registry.as_uri())

    assert counted(result) == {"read": 111, "written": 111, "skipped": 0}
    fixes = ran.table("fix.messages")
    # The dictionary's own clock and the crate's derived one alike.
    assert fixes.schema.field("sendingtime").type == pyarrow.timestamp("us", tz="UTC")
    assert fixes.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")


def test_dumped_fix_schema_can_stream_a_mock_row_through_iceberg(ran: Ran) -> None:
    field = Field.from_json(FIX_CONTRACT.read_text(encoding="utf-8"))
    schema = field.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [
            {
                "url": "file:///mock/fix.log",
                "rownum": 1,
                "body": b"8=FIX.4.4|35=D|10=0|",
                "beginstring": "FIX.4.4",
                "msghash": bytes(16),
                "timestamp": datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC),
                "unixpartition": 0,
            }
        ],
        schema=schema,
    )
    source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
    store = IcebergCatalog.from_dict(ran.catalog)
    fixes = store.dataset("fix.messages", field=field)
    try:
        assert fixes.append_arrow_reader(source, field, merge_by=True) == 1
        stored = fixes.read_arrow_table(field)
        assert stored.num_rows == 1
        assert stored.num_columns == 108
        assert stored.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
        assert stored.select(("url", "rownum", "body")).to_pylist() == [
            {
                "url": "file:///mock/fix.log",
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

    result = ran.task("parse_messages", filesystem=capture.as_uri())

    assert counted(result) == {"read": 111, "written": 111, "skipped": 0}
    assert ran.rows() == {"logs.messages": 111}


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
    append = IcebergDataset.append_arrow_reader

    def observed_append(
        dataset: IcebergDataset,
        source: pyarrow.RecordBatchReader,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        handed_to_iceberg.append(source.schema)
        return append(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "append_arrow_reader", observed_append)
    result = ran.task("parse_messages", filesystem=capture.as_uri())

    assert counted(result) == {"read": 2, "written": 2, "skipped": 0}
    assert handed_to_iceberg == [Message.field().into_arrow_schema()]

    first = datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=datetime.UTC)
    second = datetime.datetime(2026, 8, 14, 15, 46, 39, 769000, tzinfo=datetime.UTC)
    store = IcebergCatalog.from_dict(ran.catalog)
    messages = store.dataset("logs.messages", field=Message.field())
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
                Message.field(), row_filter=EqualTo("timepartition", timestamp)
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
    assert {row["plugin"] for row in rows} == {"ULBridge"}


def test_ulbridge_messages_flow_directly_through_the_fix_codec(
    ran: Ran,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handed_to_iceberg: list[pyarrow.Schema] = []
    schema_modes: dict[str, bool] = {}
    append = IcebergDataset.append_arrow_reader

    def observed_append(
        dataset: IcebergDataset,
        source: pyarrow.RecordBatchReader,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        schema_modes[dataset.identifier] = dataset.merge_schema
        if dataset.identifier == "fix.messages":
            handed_to_iceberg.append(source.schema)
        return append(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "append_arrow_reader", observed_append)
    ran.workflow()

    fixes = ran.table("fix.messages")
    assert schema_modes == {"logs.messages": False, "fix.messages": True}
    assert len(handed_to_iceberg) == 1
    assert handed_to_iceberg[0].names == fixes.schema.names
    assert fixes.num_rows == 111
    assert fixes.num_columns == 108
    # Source identity and header facts the fixed schema does not own lead the
    # row. `msgCtxId` and `timestamp` fold into the codec's own columns;
    # `bodyhash` remains distinct from the codec's parsed-message `msghash`.
    assert fixes.column_names[:10] == [
        "url",
        "rownum",
        "timepartition",
        "threadId",
        "sessionUid",
        "seqNum",
        "plugin",
        "level",
        "bodyhash",
        "body",
    ]
    assert {"msgtype", "msgseqnum", "msghash", "version", "timestamp", "unixpartition"} <= set(
        fixes.column_names
    )
    assert fixes.column_names[-2:] == ["nofixentries", "nounmappedfixentries"]
    assert not {"35", "30001", "entries", "unmapped", "msgCtxId"} & set(fixes.column_names)
    for required in ("beginstring", "msghash", "timestamp", "unixpartition"):
        assert fixes.schema.field(required).nullable is False
        assert fixes.column(required).null_count == 0
    messages = ran.table("logs.messages")
    assert fixes.column("bodyhash").equals(messages.column("bodyhash"))
    assert any(
        raw.as_py() != parsed.as_py()
        for raw, parsed in zip(fixes.column("bodyhash"), fixes.column("msghash"), strict=True)
    )
    # Arrow field metadata is not what Iceberg stores: the hourly transform is
    # in the table's own partition spec, and that is where it is read back.
    assert ran.partitions("fix.messages") == {"timepartition": "hour"}
    store = IcebergCatalog.from_dict(ran.catalog)
    try:
        table = store.catalog.load_table("fix.messages")
        assert {
            table.schema().find_column_name(field_id)
            for field_id in table.schema().identifier_field_ids
        } == {"url", "rownum"}
    finally:
        store.close()
    msgtypes = {
        row["rownum"]: None if row["msgtype"] is None else row["msgtype"].rstrip(b"\0").decode()
        for row in fixes.select(("rownum", "msgtype")).to_pylist()
    }
    assert msgtypes[2] == "8"
    assert msgtypes[5] == "UL"


def test_maintenance_visits_every_table_and_reports_what_it_changed(ran: Ran) -> None:
    """The first pass settles what is fragmented; the second finds nothing."""
    ran.workflow()

    first = ran.task("optimize_iceberg")

    assert first["task"] == "optimize_iceberg"
    assert first["tables"] == len(STORED)
    assert set(first["reports"]) == set(STORED)
    # The bridge second lands as one file in one hour in each table, so both
    # are already settled.
    assert first["reports"]["logs.messages"]["rewritten"] == 0
    assert first["reports"]["fix.messages"]["rewritten"] == 0
    assert counted(first) == {"read": 2, "written": 0, "skipped": 2}, json.dumps(first, indent=2)
    assert (first["expired"], first["deleted"], first["byte_size"]) == (0, 0, 0)
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
