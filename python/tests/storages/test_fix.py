"""The two FIX tasks: bronze `fix_messages` parsed from the lines, silver walked from bronze.

Both tables are the dictionary's one fixed row, narrowed for Iceberg: keyed
on `curruuid`, laid out by the hour of `currunix`, sorted by
`currunix, seqnum, curruuid`. A bronze row is what one message implied about
itself; a silver row is one event, placed in its chain.
"""

from __future__ import annotations

import datetime
import uuid
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.compute
import pytest

from rekep import Field, FixRegistry, State, Storages, pipeline
from rekep.fix import (
    EVENT_CLOCK,
    MESSAGE_KEY,
    SORT_COLUMNS,
    SOURCES,
    TRANSACTION_CLOCK,
    UNDATED,
    FixCodec,
    fix_lifecycle_arrow_reader,
    fix_message_field,
    fix_window_filter,
    iceberg_event_field,
)
from rekep.iceberg import IcebergDataset, iceberg_contract_field, partition_keys
from rekep.pipeline import (
    FIX_MESSAGES,
    FIX_MESSAGES_RAW,
    HISTORY,
    LOG_MESSAGES,
    SNAPSHOT_MILLIS,
    Landed,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.storages import LAYERS
from rekep.times import window_of, within

from .conftest import (
    CAPTURE,
    DAY,
    END,
    ROOT,
    START,
    WINDOW,
    Landing,
    graph,
    layout,
    planned,
    read,
    rows,
    snapshots,
    storages_mapping,
)
from .test_pipeline import LANDED

pytestmark = pytest.mark.integration

UTC = datetime.timezone.utc

#: The contract `tools/schemas_dump.py` publishes for both FIX tables.
FIX_CONTRACT = ROOT / "schemas" / "bronze" / "record_keeping" / "fix_messages.json"

#: The fixed row's width: the crate's own columns and the dictionary's.
COLUMNS = 133

#: How many messages a dictionary typing no message type answers off the
#: capture's 144 lines: the 79 frames, and the five executions it still
#: splits off the reports it can read as fills.
DAY_MESSAGES = 84

#: How many messages the parse dates by their sending clock, and how many by
#: their transaction clock, over the capture's day, per delay pin. A message
#: stating no `SendingTime` is measured against its line's clock: read in the
#: bridge's zone, that stands within the default second of the venue's clock
#: but never on it, so the default dates 58 such messages by their
#: transaction clock and the nought pin dates them by their line. An
#: execution split out of a report is dated as the report is.
DELAYS = {"default": (8, 73), "nought": (23, 4)}

#: The narrow dictionary's row: the crate's own columns, its clock and symbol.
NARROW_COLUMNS = 34


def test_the_fix_tables_are_laid_out_exactly_alike_by_the_event(landing: Landing) -> None:
    """Read back off Iceberg's own metadata and not off Arrow field metadata:
    the identifier fields are exactly `curruuid`, the partition spec is
    exactly one hour on `currunix`, and the sort order is the three columns
    the field declares, on both tables."""
    for table in (FIX_MESSAGES_RAW, FIX_MESSAGES):
        assert layout(landing.storages, table) == {
            "key": {"curruuid"},
            "spec": [(EVENT_CLOCK, "hour")],
            "sort": [(EVENT_CLOCK, "identity"), ("seqnum", "identity"), ("curruuid", "identity")],
        }, table


def test_the_lineage_holds_across_the_layers(landing: Landing) -> None:
    """A message's `srcuuids` is the `curruuid` of the lines it was parsed out
    of -- provenance, never lineage. A silver row's `prevuuid` is the
    `curruuid` of the step before it; bronze carries no chain at all."""
    lines = landing.table(LOG_MESSAGES)
    bronze = landing.table(FIX_MESSAGES_RAW)
    silver = landing.table(FIX_MESSAGES)
    named = set(lines.column("curruuid").to_pylist())
    assert len(named) == lines.num_rows, "a stored line has one identity"

    # A message names the one line it was parsed out of, and an execution
    # split out of a report names that report beside it.
    reports = set(bronze.column(MESSAGE_KEY).to_pylist())
    parsed = bronze.column(SOURCES).to_pylist()
    assert all(len(set(held) & named) == 1 and set(held) <= named | reports for held in parsed)
    assert any(set(held) & reports for held in parsed)
    observed = silver.column(SOURCES).to_pylist()
    assert all(set(held) & named for held in observed), "every event names its lines"
    for table in (bronze, silver):
        sources = {line for held in table.column(SOURCES).to_pylist() for line in held}
        assert sources <= named | reports
        assert not {"msgthreadid", "loglevel", "body"} & set(table.column_names)

    assert bronze.column("seqnum").null_count == bronze.num_rows
    assert bronze.column("prevuuid").null_count == bronze.num_rows
    assert bronze.column("prevunix").null_count == bronze.num_rows

    identities = set(silver.column(MESSAGE_KEY).to_pylist())
    followed = [row for row in silver.to_pylist() if row["prevuuid"] is not None]
    assert followed
    assert all(row["prevuuid"] in identities and row["seqnum"] >= 1 for row in followed)
    assert UNDATED not in silver.column(EVENT_CLOCK).to_pylist()
    # The walk re-settles the identities it dates and emits the expiry the
    # chain states, so the two tables hold different identities.
    assert identities != set(bronze.column(MESSAGE_KEY).to_pylist())
    for column in ("state", "creaunix"):
        assert silver.column(column).null_count == 0, column


def test_the_clocks_and_the_group_the_row_grew_reach_the_stored_tables(landing: Landing) -> None:
    """The execution clock the bridge states, the recording clock the line's
    own instant fills -- a bronze row's is its line's, a silver row's the
    earliest of the lines its event was logged on -- and the regulatory
    identifiers, a repeating group persisted whole beside its counter."""
    for table in (FIX_MESSAGES_RAW, FIX_MESSAGES):
        held = landing.table(table)
        for column in ("execunix", "recdunix", "creaunix", "exprunix"):
            assert held.schema.field(column).type == pyarrow.timestamp("us", tz="UTC"), column
        assert held.column("execunix").null_count < held.num_rows, "the bridge states these"
        # The one row no line recorded is the expiry the walk emitted.
        unrecorded = held.column("recdunix").null_count
        expired = held.column("state").to_pylist().count(State.EXPIRED)
        assert unrecorded == (0 if table == FIX_MESSAGES_RAW else expired)

        occurrences = held.column("regulatorytradeids").to_pylist()
        members = held.schema.field("regulatorytradeids").type.value_type
        assert "regulatorytradeid" in members.names
        assert any(occurrences), "this capture carries regulatory identifiers"
        for group, counted in zip(
            occurrences, held.column("noregulatorytradeids").to_pylist(), strict=True
        ):
            assert counted is None or len(group or ()) == counted


def test_fix_rows_cross_from_the_codec_straight_into_both_layers(
    storages: Storages, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each task hands Iceberg a reader and merges it on the table's primary
    key, appending or overwriting nothing; the FIX tables merge a grown row
    into the table, the log does not; and what lands is the fixed row, its
    native event columns first and its residual record last, with nothing of
    the line beside it."""
    handed: dict[str, pyarrow.Schema] = {}
    merged: dict[str, bool] = {}
    keyed: dict[str, Any] = {}
    merge = IcebergDataset.merge_arrow_reader

    def observed(dataset: IcebergDataset, source: Any, *args: Any, **kwargs: Any) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        table = f"{dataset.catalog_name}.{dataset.identifier}"
        merged[table] = dataset.merge_schema
        keyed[table] = kwargs.get("merge_by")
        handed[table] = source.schema
        return merge(dataset, source, *args, **kwargs)

    def refused(dataset: IcebergDataset, *args: Any, **kwargs: Any) -> int:
        raise AssertionError(f"{dataset.identifier} is merged, never appended or overwritten")

    monkeypatch.setattr(IcebergDataset, "merge_arrow_reader", observed)
    monkeypatch.setattr(IcebergDataset, "append_arrow_reader", refused)
    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", refused)
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, WINDOW)
    parse_fix_messages_refined(storages, WINDOW)

    assert merged == {LOG_MESSAGES: False, FIX_MESSAGES_RAW: True, FIX_MESSAGES: True}
    assert keyed == dict.fromkeys(merged), "each merges on the primary key it declares"
    for table in (FIX_MESSAGES_RAW, FIX_MESSAGES):
        fixes = read(storages, table)
        assert handed[table].names == fixes.column_names
        assert fixes.num_columns == COLUMNS
        assert fixes.column_names[0] == EVENT_CLOCK
        assert {"msgtype", "msgseqnum", "curruuid", "crosscode", "srcuuids"} <= set(
            fixes.column_names
        )
        # One fact, one column: tags 44, 38 and 53 are their own columns.
        assert {"price", "orderqty", "quantity"} <= set(fixes.column_names)
        assert not {"px", "qty", "prevpx", "prevqty"} & set(fixes.column_names)
        # A pair the dictionary names and no column projects is an entry of
        # the residual record, keyed `tag:name`, which closes the row.
        assert fixes.column_names[-2:] == ["metadata", "fixentries"]
        assert not {"35", "30001", "entries", "unmapped", "msgCtxId", "uuid"} & set(
            fixes.column_names
        )
        for required in ("beginstring", EVENT_CLOCK, "creaunix", "curruuid", "crossuuid"):
            assert fixes.schema.field(required).nullable is False
            assert fixes.column(required).null_count == 0
        assert {"8", "D"} <= set(fixes.column("msgtype").to_pylist())


@pytest.mark.parametrize(
    "task", [parse_fix_messages_raw, parse_fix_messages_refined], ids=["raw", "refined"]
)
def test_an_empty_registry_is_refused_before_a_fix_task_creates_a_table(
    storages: Storages, task: Any
) -> None:
    """A codec over a dictionary that defines nothing is refused where a FIX
    table's shape is built, so no FIX task opens a table under it: a missing
    dictionary cannot leave a narrow table."""
    with pytest.raises(ValueError, match="no specification fields"):
        task(storages, WINDOW, codec=FixCodec(FixRegistry()))
    assert list(storages.tables()) == []


def test_a_raw_task_parses_under_the_codec_it_is_handed_without_rewriting_text(
    storages: Storages, tmp_path: Path
) -> None:
    """Threads and batch bounds are codec pins; the text is not rewritten, and
    a new order states the state its message type asks for."""
    capture = tmp_path / "prefixed.log"
    capture.write_bytes(
        b"2026-08-14 12:46:39.769 [1] [ULBridge] (INFO) "
        b"  --> 8=FIX.4.4|35=D|11=OPTION-1|55=HOLN|10=000|\n"
    )
    parse_log_messages(capture.as_uri(), storages, DAY)

    landed = parse_fix_messages_raw(
        storages, DAY, codec=FixCodec.from_env(threads=2, batch_row_size=2)
    )

    assert landed == Landed(read=1, written=1)
    raw = read(storages, FIX_MESSAGES_RAW)
    assert raw.column("msgtype").to_pylist() == ["D"]
    assert raw.column("clordid").to_pylist() == ["OPTION-1"]
    assert raw.column("crosscode").to_pylist() == ["OPTION-1"]
    assert raw.column("state").to_pylist() == [State.PENDING_NEW]


def test_the_official_clock_delay_is_a_codec_pin(tmp_path: Path) -> None:
    """What dates a message is the venue's clock, within a stated distance.

    `SendingTime(52)` is when a session put the message on the wire, which is
    not when the thing it reports happened. The parse dates the message by the
    official transaction clock standing within `official_time_delay_ms` of
    that sending clock, and falls back to the sending clock where none stands
    that near; a message stating none is measured against its line's clock.
    Each pin gets catalogs of its own, because the instant a message settles
    on is what its identity is derived from.
    """

    def dated_by(name: str, **pinned: Any) -> tuple[int, int]:
        with Storages.from_dict(storages_mapping(tmp_path / name)) as held:
            parse_log_messages(CAPTURE.as_uri(), held, DAY)
            parse_fix_messages_raw(held, DAY, codec=FixCodec.from_env(**pinned))
            dated = read(held, FIX_MESSAGES_RAW).select(
                (EVENT_CLOCK, "sendingtime", TRANSACTION_CLOCK)
            )
        stamped = dated.to_pylist()
        return (
            sum(1 for row in stamped if row["sendingtime"] == row[EVENT_CLOCK]),
            sum(1 for row in stamped if row[TRANSACTION_CLOCK] == row[EVENT_CLOCK]),
        )

    default = dated_by("default")
    nought = dated_by("nought", official_time_delay_ms=0)
    wide = dated_by("wide", official_time_delay_ms=600_000)
    print(f"\n(by sending, by transaction): default {default}, 0 ms {nought}, 10 min {wide}")
    # The core's own second, which is what a codec takes when it pins nothing.
    assert default == DELAYS["default"]
    # A nonpositive delay admits only a clock equal to the sending one, so
    # every event a venue stamped a little apart falls back to the wire, or
    # to its line where it states no sending clock.
    assert nought == DELAYS["nought"]
    # And a wide one admits the clocks the default already did and no more:
    # no transaction clock in this capture stands between a second and ten
    # minutes from the clock it is measured against.
    assert wide == default


def test_a_narrow_dictionary_still_answers_every_message(
    storages: Storages, tmp_path: Path
) -> None:
    """A dictionary narrow enough to type no message type at all still answers
    a row per message: the identity, the instant and the chain are the
    crate's own columns, and a frame is a message whether or not a dictionary
    can name its type. The table takes the dictionary's shape -- no `msgtype`
    column -- and the walk, which reads a chain off what a message is, places
    none of them."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    registry = tmp_path / "nanosecond-fix-registry"
    registry.mkdir()
    sending_time = Field("sendingtime", pyarrow.timestamp("ns", tz="UTC"), nullable=True)
    sending_time.fix.tag = 52
    # A bare registry already seeds the standard clocks, so one specification
    # field beside the clock is what makes it a dictionary.
    symbol = Field("symbol", "utf8", nullable=True)
    symbol.fix.tag = 55
    FixRegistry.from_fields([sending_time, symbol]).write_into(registry)
    codec = FixCodec(FixRegistry.from_handle(registry), default_sending_time=UNDATED)

    landed = parse_fix_messages_raw(storages, DAY, codec=codec)

    print(f"\nnarrow raw over the day: {landed}")
    assert landed.read == 144
    assert landed.written + landed.skipped == DAY_MESSAGES
    raw = read(storages, FIX_MESSAGES_RAW)
    assert raw.num_rows == landed.written
    assert "msgtype" not in raw.column_names
    assert raw.num_columns == NARROW_COLUMNS
    assert raw.schema.field("sendingtime").type == pyarrow.timestamp("us", tz="UTC")
    assert raw.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    assert "body" not in raw.column_names
    assert raw.column(SOURCES).null_count == 0

    walked = parse_fix_messages_refined(storages, DAY, codec=codec)

    assert walked == Landed(read=landed.written, written=0)
    refined = read(storages, FIX_MESSAGES)
    assert refined.num_rows == 0
    assert refined.schema.equals(raw.schema)


def test_the_published_contract_streams_a_mock_row_through_iceberg(storages: Storages) -> None:
    field = iceberg_contract_field(FIX_CONTRACT.read_text(encoding="utf-8"), "fix_messages")
    # The published contract carries the partition spec, so a table built from
    # the document alone is laid out the way both FIX tasks lay theirs out.
    assert partition_keys(field) == {EVENT_CLOCK: "hour"}
    schema = field.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [
            {
                "beginstring": "FIX.4.4",
                EVENT_CLOCK: UNDATED,
                "creaunix": UNDATED,
                "currhashcode": 1,
                "crosshashcode": 2,
                "curruuid": uuid.UUID(int=1).bytes,
                "crossuuid": uuid.UUID(int=2).bytes,
            }
        ],
        schema=schema,
    )
    fixes = storages.dataset(FIX_MESSAGES_RAW, field=field)
    try:
        source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
        assert fixes.merge_arrow_reader(source, field) == 1
        stored = fixes.read_arrow_table(field)
    finally:
        fixes.close()
    assert stored.num_rows == 1
    assert stored.num_columns == COLUMNS
    assert {"currhashcode", SOURCES} <= set(stored.column_names)
    assert not {"body", "loglevel", "msgthreadid"} & set(stored.column_names)


def test_a_table_written_before_the_row_grew_gains_the_columns_and_keeps_its_rows(
    storages: Storages,
) -> None:
    """A FIX write merges schema, and the rows a table already held stay.

    A table created without five of the row's columns is written by the run:
    it gains the five, the row it held stays -- read back with them empty --
    and the run lands beside it. This pins `merge_schema` and no migration
    path: a warehouse written under an earlier native revision is replayed
    from capture, because that revision states other identities.
    """
    grew = ("execunix", "recdunix", "snapunix", "noregulatorytradeids", "regulatorytradeids")
    full = fix_message_field().into_arrow_schema()
    before = iceberg_event_field(
        pyarrow.schema([member for member in full if member.name not in grew], full.metadata),
        "fix_messages",
    )
    assert len(before) == len(full) - len(grew)
    held = before.into_arrow_schema()
    settled = {
        "beginstring": "FIX.4.4",
        EVENT_CLOCK: UNDATED,
        "creaunix": UNDATED,
        MESSAGE_KEY: bytes(15) + b"\x01",
        "crossuuid": bytes(15) + b"\x02",
        "currhashcode": 1,
        "crosshashcode": 2,
    }
    landed = pyarrow.RecordBatch.from_pylist(
        [{**{member.name: None for member in held}, **settled}], schema=held
    )
    dataset = storages.dataset(FIX_MESSAGES_RAW, field=before)
    try:
        source = pyarrow.RecordBatchReader.from_batches(held, [landed])
        assert dataset.append_arrow_reader(source, before) == 1
    finally:
        dataset.close()

    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    result = parse_fix_messages_raw(storages, WINDOW)

    raw = read(storages, FIX_MESSAGES_RAW)
    assert result == LANDED["parse_fix_messages_raw"]
    assert raw.num_columns == len(full)
    assert raw.num_rows == result.written + 1, "the row it already held is still there"
    kept = raw.filter(pyarrow.compute.equal(raw.column(MESSAGE_KEY), settled[MESSAGE_KEY]))
    assert kept.num_rows == 1
    for column in grew:
        assert kept.column(column).to_pylist() in ([None], [[]]), column
    # And the walk reads the widened table back without noticing the seam:
    # the held row sits at the pin, which every window reads.
    walked = parse_fix_messages_refined(storages, WINDOW)
    assert walked.read == result.written + 1
    assert read(storages, FIX_MESSAGES).num_columns == len(full)


HOUR = window_of("2026-08-14T10:00:00Z", "2026-08-14T11:00:00Z")


@pytest.mark.parametrize("expires", ["10:40:00", "11:40:00"])
@pytest.mark.parametrize("previous_clock", ["09:59:00", "08:59:00"])
def test_the_walk_reads_the_hour_before_its_window_and_writes_only_its_own(
    storages: Storages,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expires: str,
    previous_clock: str,
) -> None:
    """The hour before `start` places a chain that began there, and none of
    it is written but the view of it the hourly grid restates at `start`; an
    expiry past `end` waits for its own window. The lines arrive out of
    order, so arrival cannot stand in for the event's order. A replay of the
    window answers every row again, each a key the table holds, so it
    appends none and commits nothing."""
    from pyiceberg.io.pyarrow import PyArrowFile

    capture = tmp_path / "hours.log"
    capture.write_text(
        "\n".join(
            f"2026-08-14 {clock}.000 [77] [ULBridge] (INFO) Sending : "
            f"8=FIX.4.4|35=8|49=BUY|56=SELL|34={index}|52=20260814-{clock}|"
            f"37={order}|11={order}|39=0|150=0|55=AAPL|54=1|38=10|151=10|"
            f"126=20260814-{expires}|10=0|"
            for index, (clock, order) in enumerate(
                [
                    ("11:00:00", "later"),
                    ("10:10:00", "order-1"),
                    ("08:59:59", "older"),
                    (previous_clock, "order-1"),
                ],
                1,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    parse_log_messages(capture.as_uri(), storages, DAY)
    assert parse_fix_messages_raw(storages, DAY).written == 4

    paths: list[str] = []
    original = PyArrowFile.open

    def tracked(file: PyArrowFile, *args: Any, **kwargs: Any) -> Any:
        if file.location.endswith(".parquet") and "/bronze/" in file.location:
            paths.append(file.location)
        return original(file, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", tracked)
    landed = parse_fix_messages_refined(storages, HOUR)
    has_history = previous_clock == "09:59:00"
    assert landed.read == (2 if has_history else 1)
    assert landed.written == (2 if expires == "10:40:00" else 1) + has_history
    assert paths and all(
        "currunix_hour=2026-08-14-09" in path or "currunix_hour=2026-08-14-10" in path
        for path in paths
    )
    assert f"currunix_hour=2026-08-14-{'09' if has_history else '10'}" in paths[0]

    held = read(storages, FIX_MESSAGES)
    walked = [row for row in held.to_pylist() if row["snapunix"] is None]
    views = [row for row in held.to_pylist() if row["snapunix"] is not None]
    assert walked[0][EVENT_CLOCK] == datetime.datetime(2026, 8, 14, 10, 10, tzinfo=UTC)
    if has_history:
        # The order of 09:59 as it stood at 10:00: its chain's first step,
        # dated at the tick under the identity that instant derives.
        (view,) = views
        assert view[EVENT_CLOCK] == view["snapunix"] == HOUR[0]
        assert (view["crosscode"], view["seqnum"], view["prevuuid"]) == (
            walked[0]["crosscode"],
            None,
            None,
        )
        assert view["curruuid"] not in {walked[0]["curruuid"], walked[0]["prevuuid"]}
        assert walked[0]["prevuuid"] is not None
        assert walked[0]["prevunix"] == datetime.datetime(2026, 8, 14, 9, 59, tzinfo=UTC)
        assert walked[0]["seqnum"] == 1
    else:
        # The one-hour context is an explicit horizon, not a claim that a
        # business chain cannot have an older predecessor.
        assert views == []
        assert walked[0]["prevuuid"] is None
        assert walked[0]["prevunix"] is None
        assert walked[0]["seqnum"] is None
    if expires == "10:40:00":
        assert walked[1][EVENT_CLOCK] == datetime.datetime(2026, 8, 14, 10, 40, tzinfo=UTC)
        assert walked[1]["state"] == State.EXPIRED
    committed = snapshots(storages, FIX_MESSAGES)
    assert parse_fix_messages_refined(storages, HOUR) == Landed(
        read=landed.read, written=0, skipped=landed.written + landed.skipped
    )
    assert read(storages, FIX_MESSAGES).equals(held)
    assert snapshots(storages, FIX_MESSAGES) == committed


#: The grid the walk restates the chains on, in nanoseconds: one hour.
GRID_NS = SNAPSHOT_MILLIS * 1_000_000


def split(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Silver rows as the events the walk placed and the grid views of them."""
    return (
        [row for row in rows if row["snapunix"] is None],
        [row for row in rows if row["snapunix"] is not None],
    )


def alive_at(events: list[dict[str, Any]], tick: datetime.datetime) -> set[tuple[Any, ...]]:
    """The chains whose last event before `tick` leaves them alive, each as
    that event's place: what the grid restates at `tick`."""
    last: dict[bytes, dict[str, Any]] = {}
    for row in sorted(events, key=lambda row: (row[EVENT_CLOCK], row["seqnum"] or 0)):
        if row[EVENT_CLOCK] < tick:
            last[row["crossuuid"]] = row
    return {
        placed(row)
        for row in last.values()
        if State(row["state"]).is_live() and row["state"] != State.EXPIRED
    }


def placed(row: dict[str, Any]) -> tuple[Any, ...]:
    """What a view repeats of the live event it restates."""
    return (
        row["crossuuid"],
        row["crosscode"],
        row["seqnum"],
        row["prevuuid"],
        row["state"],
        tuple(row[SOURCES]),
    )


def test_the_walk_restates_every_chain_alive_on_each_whole_hour(landing: Landing) -> None:
    """At every whole hour the walk crosses, each chain still alive is
    restated as it stands: dated at the tick -- `currunix` and `snapunix`
    both -- under the identity that instant derives, so a view is a row of
    its own beside its event and the key folds nothing into another."""
    silver = landing.table(FIX_MESSAGES).filter(
        within(landing.table(FIX_MESSAGES).column(EVENT_CLOCK), WINDOW)
    )
    events, views = split(silver.to_pylist())
    refined = landing.landed["parse_fix_messages_refined"]
    print(f"\n{len(events)} events and {len(views)} views: {refined}")
    assert (len(events), len(views)) == (18, 12)
    assert refined.written == silver.num_rows and refined.skipped == 0
    assert all(view[EVENT_CLOCK] == view["snapunix"] for view in views)
    ticks = sorted({view["snapunix"] for view in views})
    assert all(tick.minute == tick.second == tick.microsecond == 0 for tick in ticks)
    identities = [row[MESSAGE_KEY] for row in events + views]
    assert len(set(identities)) == len(identities), "a view's identity is its tick's own"

    hours = [START + index * HISTORY for index in range(int((END - START) / HISTORY) + 1)]
    for tick in hours:
        restated = {placed(view) for view in views if view["snapunix"] == tick}
        print(f"{tick:%H:%M} {len(restated)} views")
        assert restated == alive_at(events, tick), tick
    assert ticks == hours[1:], "nothing is alive at 12:00: the hour before holds no chain"


def test_the_rows_at_start_are_the_chains_the_hour_before_left_alive(
    storages: Storages,
) -> None:
    """A window opening at 13:00 reads the hour before it, where the day's
    chains began, and answers at 13:00 exactly the views the walk over the
    whole window landed there -- every row it answers is a key that walk
    landed, so it appends none and commits nothing. The grid is the task's
    `snapshot_millis`, whatever the codec pins: zero lands the events alone,
    and a codec pinning a minute lands the hourly views. The walk runs on
    the task's own codec stating the read is sorted, so it holds an hour at
    a time."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, WINDOW)
    used: list[FixCodec] = []

    def spied(held: FixCodec, source: pyarrow.RecordBatchReader) -> pyarrow.RecordBatchReader:
        used.append(held)
        return fix_lifecycle_arrow_reader(held, source)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pipeline, "fix_lifecycle_arrow_reader", spied)
        assert parse_fix_messages_refined(storages, WINDOW) == LANDED["parse_fix_messages_refined"]
    # The task walks its sorted read an hour at a time, on the hourly grid.
    (task,) = used
    assert (task.sorted_lifecycle, task.snapshot_ns) == (True, GRID_NS)
    held = read(storages, FIX_MESSAGES)

    later = (START + HISTORY, END)
    committed = snapshots(storages, FIX_MESSAGES)
    landed = parse_fix_messages_refined(storages, later)
    stored = held.filter(within(held.column(EVENT_CLOCK), later))
    at_start = [row for row in stored.to_pylist() if row[EVENT_CLOCK] == later[0]]
    print(f"\nfrom 13:00 {landed}: {len(at_start)} rows at 13:00")
    assert landed == Landed(read=66, written=0, skipped=stored.num_rows)
    assert len(at_start) == 3 and all(row["snapunix"] == later[0] for row in at_start)
    assert read(storages, FIX_MESSAGES).equals(held)
    assert snapshots(storages, FIX_MESSAGES) == committed

    plain = parse_fix_messages_refined(
        storages, WINDOW, snapshot_millis=0, target="silver.record_keeping.plain_fix_messages"
    )
    assert plain == Landed(read=66, written=18)
    events, _ = split(held.to_pylist())
    assert read(storages, "silver.record_keeping.plain_fix_messages").to_pylist() == events

    minute = FixCodec.from_env(default_sending_time=UNDATED, snapshot_ns=60_000_000_000)
    pinned = parse_fix_messages_refined(
        storages, WINDOW, codec=minute, target="silver.record_keeping.pinned_fix_messages"
    )
    assert pinned == Landed(read=66, written=30)
    assert read(storages, "silver.record_keeping.pinned_fix_messages").equals(held)


def test_a_sorted_walk_answers_what_the_whole_walk_does(landing: Landing) -> None:
    """The task's read arrives in instant order, one hour partition after
    another, so its walk takes it as it comes and holds an hour at a time;
    over the capture it answers exactly what the walk collecting the whole
    read does, views included. The task's own pins are
    `test_the_rows_at_start_are_the_chains_the_hour_before_left_alive`'s."""
    codec = FixCodec.from_env(default_sending_time=UNDATED).with_snapshot_ns(GRID_NS)
    field = fix_message_field(codec)
    history = (START - HISTORY, END)

    def walked(sorted_lifecycle: bool) -> pyarrow.Table:
        raw = landing.storages.dataset(FIX_MESSAGES_RAW, field=field)
        try:
            scan = raw.read_arrow_reader(
                field, row_filter=fix_window_filter(history), order_by=SORT_COLUMNS
            )
            with fix_lifecycle_arrow_reader(
                codec.with_sorted_lifecycle(sorted_lifecycle), scan
            ) as reader:
                return reader.read_all()
        finally:
            raw.close()

    hourly = walked(True)
    whole = walked(False)
    print(f"\nthe hourly walk answers {hourly.num_rows} rows, the whole walk {whole.num_rows}")
    assert hourly.num_rows == whole.num_rows == 30
    assert hourly.equals(whole)


def test_maintenance_compacts_every_task_table_and_keeps_its_rows(storages: Storages) -> None:
    """`optimize` over what the tasks landed, in every layer: the first pass
    settles each table's small files without losing a row; the second finds
    nothing to do."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    graph(storages, WINDOW)
    stored = rows(storages)
    files = {table: len(planned(storages, table)) for table in stored}

    def optimized() -> dict[str, dict[str, Any]]:
        reports = {}
        for layer in LAYERS:
            for dataset in storages.catalog(layer).datasets():
                try:
                    reports[f"{layer}.{dataset.identifier}"] = dataset.optimize()
                finally:
                    dataset.close()
        return reports

    first = optimized()

    print(f"\ndata files {files}\nfirst pass {first}")
    # An hour partition hides which rows it holds, so a table compacts as a
    # whole, and only once it holds two files or more.
    assert {table: report["rewritten"] > 0 for table, report in first.items()} == {
        table: count >= 2 for table, count in files.items()
    }
    assert rows(storages) == stored, "compaction rewrites rows, it never drops them"

    second = optimized()

    assert all(report["rewritten"] == 0 for report in second.values()), second
    assert rows(storages) == stored, "a settled catalog is left as it was"
