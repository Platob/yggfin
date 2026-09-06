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
from yggdryl import DataType, Field
from yggdryl.fix import FixRegistry

from rekep import Message, cli
from rekep.iceberg import IcebergCatalog, IcebergDataset

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "app_messages_sample.txt"
FIX_CONTRACT = ROOT / "schemas" / "rekep" / "fix-message.json"

WORKFLOW = (("parse_messages", {}), ("parse_fix", {}))

#: What the fixture's fourteen physical rows produce, first run.
FIRST = {
    "parse_messages": {"read": 14, "written": 14, "skipped": 0},
    "parse_fix": {"read": 14, "written": 14, "skipped": 0},
}

#: What a replay of the same input produces: the same reads, no writes.
REPLAY = {
    name: {"read": counts["read"], "written": 0, "skipped": counts["read"]}
    for name, counts in FIRST.items()
}

#: Stored rows, and the one snapshot each table holds after both runs.
STORED = {
    "logs.messages": 14,
    "fix.messages": 14,
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

    def registry(self) -> str:
        """A tiny native dictionary sufficient to smoke the fixed schema."""
        location = self.root / "fix-registry"
        if not location.exists():
            location.mkdir()
            fields = []
            for tag, name in (
                (8, "beginstring"),
                (35, "msgtype"),
                (54, "side"),
                (55, "symbol"),
                (10, "checksum"),
            ):
                field = Field(name, DataType("utf8"), nullable=True)
                field.fix.tag = tag
                fields.append(field)
            FixRegistry.from_fields(fields).write_into(location)
        return location.as_uri()

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
            elif name == "parse_fix":
                first = {"registry": self.registry()}
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


def test_the_workflow_publishes_the_fixture_and_a_replay_writes_nothing(ran: Ran) -> None:
    first = ran.workflow()
    assert {name: counted(result) for name, result in first.items()} == FIRST
    assert ran.rows() == STORED
    messages = ran.table("logs.messages")
    assert messages.schema.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
    timestamps = {
        row["rownum"]: row["timestamp"]
        for row in messages.select(("rownum", "timestamp")).to_pylist()
    }
    assert timestamps[1] == datetime.datetime(2026, 8, 14, 0, 5, 1, 147250, tzinfo=datetime.UTC)
    assert timestamps[3] == datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=datetime.UTC)
    assert [timestamps[rownum] for rownum in (10, 11, 12)] == [None, None, None]

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


def test_parse_fix_refuses_a_registry_iceberg_cannot_store_before_creating_a_table(
    ran: Ran, tmp_path: Path
) -> None:
    ran.task("parse_messages", filesystem=FIXTURE.as_uri())
    registry = tmp_path / "nanosecond-fix-registry"
    registry.mkdir()
    sending_time = Field("sendingtime", pyarrow.timestamp("ns", tz="UTC"), nullable=True)
    sending_time.fix.tag = 52
    FixRegistry.from_fields([sending_time]).write_into(registry)
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
    assert ran.rows() == {"logs.messages": 14}


def test_dumped_fix_schema_can_stream_a_mock_row_through_iceberg(ran: Ran) -> None:
    field = Field.from_json(FIX_CONTRACT.read_text(encoding="utf-8"))
    schema = field.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [{"url": "file:///mock/fix.log", "rownum": 1, "body": b"8=FIX.4.4|35=D|10=0|"}],
        schema=schema,
    )
    source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
    store = IcebergCatalog.from_dict(ran.catalog)
    fixes = store.dataset("fix.messages", field=field)
    try:
        assert fixes.append_arrow_reader(source, field, merge_by=True) == 1
        stored = fixes.read_arrow_table(field)
        assert stored.num_rows == 1
        assert stored.num_columns == 95
        assert stored.schema.field("30004").type == pyarrow.timestamp("us", tz="UTC")
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
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    (capture / "a.log").write_text("".join(lines[:6]), encoding="utf-8")
    (capture / "b.log").write_text("".join(lines[6:]), encoding="utf-8")

    result = ran.task("parse_messages", filesystem=capture.as_uri())

    assert counted(result) == {"read": 14, "written": 14, "skipped": 0}
    assert ran.rows() == {"logs.messages": 14}


def test_messages_stream_through_hour_partitions(
    ran: Ran,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The application hands Iceberg a reader; partitioned reads stay readers."""
    capture = tmp_path / "hourly"
    capture.mkdir()
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    (capture / "00.log").write_text(lines[0], encoding="utf-8")
    (capture / "01.log").write_text(
        lines[1].replace("2026-08-14 00:", "2026-08-14 01:", 1),
        encoding="utf-8",
    )

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

    first = datetime.datetime(2026, 8, 14, 0, 5, 1, 147250, tzinfo=datetime.UTC)
    second = datetime.datetime(2026, 8, 14, 1, 5, 1, 147000, tzinfo=datetime.UTC)
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
    assert {row["branch"] for row in rows} == {
        "OMSSales_Enrichment",
        "ModuleMarketDataManager",
    }


def test_fix_parsing_preserves_source_identity_and_emits_native_columns(
    ran: Ran,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handed_to_iceberg: list[pyarrow.Schema] = []
    append = IcebergDataset.append_arrow_reader

    def observed_append(
        dataset: IcebergDataset,
        source: pyarrow.RecordBatchReader,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        if dataset.identifier == "fix.messages":
            handed_to_iceberg.append(source.schema)
        return append(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "append_arrow_reader", observed_append)
    ran.workflow()

    fixes = ran.table("fix.messages")
    assert len(handed_to_iceberg) == 1
    assert handed_to_iceberg[0].names == fixes.schema.names
    assert fixes.num_rows == 14
    assert {"url", "rownum", "timepartition", "35", "entries", "unmapped"} <= set(
        fixes.column_names
    )
    assert fixes.schema.field("timepartition").metadata[b"iceberg:partition_key"] == b"hour"
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
        row["rownum"]: row["35"]
        for row in fixes.select(("rownum", "35")).to_pylist()
    }
    assert msgtypes[3] == "D"
    assert msgtypes[5] == "8"


def test_maintenance_visits_every_table_and_reports_what_it_changed(ran: Ran) -> None:
    ran.workflow()

    result = ran.task("optimize_iceberg")

    assert result["task"] == "optimize_iceberg"
    assert result["tables"] == len(STORED)
    assert counted(result) == {
        "read": len(STORED),
        "written": 0,
        "skipped": len(STORED),
    }, json.dumps(result, indent=2)
    assert (result["expired"], result["deleted"], result["byte_size"]) == (0, 0, 0)
    assert set(result["reports"]) == set(STORED)
    assert result["reports"]["logs.messages"]["rewritten"] == 0
    assert result["reports"]["fix.messages"]["rewritten"] == 0
    assert ran.rows() == STORED, "a settled catalog is left as it was"
