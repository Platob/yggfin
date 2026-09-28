"""Three layer catalogs, and the shipped capture landed through every task once.

`storages` is a `Storages` over three local catalogs -- one SQLite database
and one file warehouse per layer, under the test's own directory -- closed
after the test. `landing` is the capture landed once for the session: bronze
`log_messages` for its whole day, then every later task over `EARLY` and over
the main window `[START, END)`, with the data files each main-window task
opened. A test reading `landing` may rerun a task over the window it ran
over, because a rerun lands the rows it replaces again; nothing else writes
to it.
"""

from __future__ import annotations

import collections
import contextlib
import dataclasses
import datetime
import re
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep import Storages
from rekep.pipeline import (
    EVENTS,
    FLATTENERS,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.storages import LAYERS
from rekep.times import window_of

from ..conftest import catalog_properties

UTC = datetime.timezone.utc

ROOT = Path(__file__).resolve().parents[3]

#: The shipped capture: 144 lines, dated 2026-08-14 by the bridge's clock.
CAPTURE = ROOT / "data" / "capture" / "ulbridge.log"

#: The capture's whole day, which bronze `log_messages` is backfilled over.
DAY = window_of("2026-08-14", "2026-08-14")

#: The main window every task after the log runs over.
#:
#: The bridge prints a Central European summer clock, two hours ahead of the
#: UTC its FIX frames state, and the read takes that zone by default: the 112
#: lines printed at 14:46 are dated 12:46, the hour of the events they carry.
#: So the window opens at 12:00, where both are, holds the day's one trade
#: report, whose line and message are dated 14:52, runs past 16:25, where the
#: walk expires the day's open order, and closes inside that hour, so no
#: task's window ends on a partition's bound. The trade report's side group
#: states no `Side(54)`, so it splits off no execution and books nothing,
#: which
#: `test_market.py::test_books_fold_a_cancel_reject_under_the_side_of_its_order`
#: pins.
START = datetime.datetime(2026, 8, 14, 12, tzinfo=UTC)
END = datetime.datetime(2026, 8, 14, 16, 30, tzinfo=UTC)
WINDOW = (START, END)

#: The window landed before it: the capture's sixteen lines of 01:00 and the
#: events they carry, dated in the same hour. Landed first, so every table a
#: main-window task reads holds a partition its window does not cover, and a
#: main-window rerun has neighbours to leave alone.
EARLY = (
    datetime.datetime(2026, 8, 14, tzinfo=UTC),
    datetime.datetime(2026, 8, 14, 2, tzinfo=UTC),
)

#: The columns every table is sorted by within a partition, and read back in.
ORDER = ("currunix", "seqnum", "curruuid")

#: Where a data file sits: its layer's warehouse, its table, its hour.
_LOCATION = re.compile(
    r"/(?P<layer>bronze|silver|gold)/(?P<namespace>\w+)/(?P<table>\w+)/data/"
    r"currunix_hour=(?P<hour>\d{4}-\d{2}-\d{2}-\d{2})/[^/]+\.parquet$"
)


def storages_mapping(root: Path) -> dict[str, dict[str, Any]]:
    """One `IcebergCatalog.from_dict` mapping per layer, each its own database and warehouse."""
    return {
        layer: {"name": layer, "properties": catalog_properties(root, layer)} for layer in LAYERS
    }


def located(location: str) -> tuple[str, str]:
    """A data file's table, `<layer>.<namespace>.<table>`, and its hour partition."""
    found = _LOCATION.search(location)
    assert found is not None, f"not a data file of an hour partition: {location}"
    return f"{found['layer']}.{found['namespace']}.{found['table']}", found["hour"]


def hours(locations: Iterable[str], table: str) -> list[str]:
    """The hour partitions of `table` that `locations` name, as `HH` of the day, sorted."""
    return sorted({hour[-2:] for held, hour in map(located, locations) if held == table})


def read(storages: Storages, table: str, **options: Any) -> pyarrow.Table:
    """One stored table, read whole in its sort order."""
    dataset = storages.dataset(table)
    try:
        return dataset.read_arrow_table(**options).sort_by([(name, "ascending") for name in ORDER])
    finally:
        dataset.close()


def rows(storages: Storages) -> dict[str, int]:
    """Every stored table's row count, by `<layer>.<namespace>.<table>`."""
    return {table: read(storages, table).num_rows for table in storages.tables()}


def planned(
    storages: Storages, table: str, row_filter: Any = None, snapshot_id: int | None = None
) -> list[str]:
    """The data files Iceberg plans for `row_filter` -- every one, without -- as locations."""
    dataset = storages.dataset(table)
    try:
        scan = dataset.iceberg_table.scan(
            snapshot_id=snapshot_id,
            **({"row_filter": row_filter} if row_filter is not None else {}),
        )
        return [task.file.file_path for task in scan.plan_files()]
    finally:
        dataset.close()


def layout(storages: Storages, table: str) -> dict[str, Any]:
    """What Iceberg itself records about one table: key, spec and sort order."""
    layer, _, name = table.partition(".")
    held = storages.catalog(layer).load_table(name)
    schema = held.schema()
    return {
        "key": {schema.find_column_name(field) for field in schema.identifier_field_ids},
        "spec": [
            (schema.find_column_name(field.source_id), str(field.transform))
            for field in held.spec().fields
        ],
        "sort": [
            (schema.find_column_name(field.source_id), str(field.transform))
            for field in held.sort_order().fields
        ],
    }


def snapshots(storages: Storages, table: str) -> int:
    """How many snapshots one table holds."""
    layer, _, name = table.partition(".")
    return len(storages.catalog(layer).load_table(name).metadata.snapshots)


class Opened:
    """The Parquet data files each task opened, by task name."""

    def __init__(self) -> None:
        self.files: dict[str, set[str]] = collections.defaultdict(set)
        self.task: str | None = None

    @contextlib.contextmanager
    def watching(self) -> Iterator[Opened]:
        """Record every data file PyIceberg's file IO opens while the block runs."""
        from pyiceberg.io.pyarrow import PyArrowFile

        original = PyArrowFile.open

        def tracked(file: PyArrowFile, *args: Any, **kwargs: Any) -> Any:
            if self.task is not None and file.location.endswith(".parquet"):
                self.files[self.task].add(file.location)
            return original(file, *args, **kwargs)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(PyArrowFile, "open", tracked)
            yield self

    def run(self, task: Callable[..., Landed], *args: Any, **kwargs: Any) -> Landed:
        """`task` called, and the files it opened recorded under its name."""
        self.task = task.__name__
        try:
            return task(*args, **kwargs)
        finally:
            self.task = None


def graph(
    storages: Storages,
    window: tuple[datetime.datetime, datetime.datetime],
    opened: Opened | None = None,
) -> dict[str, Landed]:
    """Every task after the log over `window`, in production order, by task name.

    The flatteners run one after another against the one snapshot the books
    committed; `test_market.py` runs them side by side and compares.
    """
    run = (opened or Opened()).run
    landed = {
        "parse_fix_messages_raw": run(parse_fix_messages_raw, storages, window),
        "parse_fix_messages_refined": run(parse_fix_messages_refined, storages, window),
        "parse_books": run(parse_books, storages, window),
    }
    books = landed["parse_books"].snapshot_id
    for task in FLATTENERS.values():
        landed[task.__name__] = run(task, storages, window, snapshot_id=books)
    return landed


@dataclasses.dataclass
class Landing:
    """The capture landed once: what each task answered, opened and stored."""

    #: The three catalogs it was landed in, open for the session.
    storages: Storages

    #: What `parse_log_messages` answered over `DAY`.
    log: Landed

    #: What every later task answered over `EARLY`, by task name.
    early: dict[str, Landed]

    #: What every later task answered over `WINDOW`, by task name.
    landed: dict[str, Landed]

    #: The data files each task opened over `WINDOW`, by task name.
    opened: dict[str, set[str]]

    #: The event tables as the sequential flatteners left them, by table.
    flattened: dict[str, pyarrow.Table]

    def table(self, table: str, **options: Any) -> pyarrow.Table:
        """One stored table, read whole in its sort order."""
        return read(self.storages, table, **options)


@pytest.fixture
def storages(tmp_path: Path) -> Iterator[Storages]:
    """Three empty local catalogs, one per layer, closed after the test."""
    held = Storages.from_dict(storages_mapping(tmp_path))
    try:
        yield held
    finally:
        held.close()


@pytest.fixture(scope="session")
def landing(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Landing]:
    """The capture's day in bronze, then `EARLY` and `WINDOW` through every later task."""
    held = Storages.from_dict(storages_mapping(tmp_path_factory.mktemp("landing")))
    try:
        log = parse_log_messages(CAPTURE.as_uri(), held, DAY)
        early = graph(held, EARLY)
        watched = Opened()
        with watched.watching():
            landed = graph(held, WINDOW, watched)
        yield Landing(
            storages=held,
            log=log,
            early=early,
            landed=landed,
            opened=dict(watched.files),
            flattened={table: read(held, table) for table in EVENTS.values()},
        )
    finally:
        held.close()
