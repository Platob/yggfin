"""What the package records, and what it stays quiet about.

The levels are a contract with an operator: INFO is what finished, DEBUG is
the detail under it. A write that commits forty chunks emitting forty INFO
records would be the flood these levels exist to prevent, so the count is
asserted, not just the content.
"""

from __future__ import annotations

import datetime
import logging
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import pyarrow
import pytest

from rekep import Storages, scalar
from rekep.fields import partition_key, primary_key
from rekep.pipeline import parse_log_messages

from .conftest import CAPTURE, DAY

pytestmark = pytest.mark.integration


@scalar
class Quote:
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, partition_key()]
    """Trading day."""


def quotes(count: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"S{index}" for index in range(count)],
            "day": [datetime.date(2026, 8, 14)] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


def test_importing_the_package_configures_nothing() -> None:
    """A library that installs a handler has decided for its caller. Without
    one the standard library's last resort carries WARNING and above, and a
    caller that wants the INFO records configures the `rekep` logger itself.

    In a subprocess, because this is a claim about a fresh interpreter and
    every other test here sets the level of the logger this one is looking at.
    """
    checked = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            "import logging, rekep, rekep.iceberg.dataset, rekep.pipeline, rekep.storages,"
            " rekep.text; root = logging.getLogger('rekep');"
            " print(bool(root.handlers), root.level)",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert checked.stdout.split() == ["False", "0"], checked.stdout


def test_every_emitting_module_is_named_by_its_own_path() -> None:
    """A record says which module wrote it, and a grep for that name reaches
    the line. One shared `getLogger("rekep")` would say nothing."""
    source = Path(__file__).resolve().parents[2] / "src" / "rekep"
    declared = {
        path: text
        for path in source.rglob("*.py")
        if "LOGGER = logging.getLogger(" in (text := path.read_text(encoding="utf-8"))
    }

    assert declared, "no module emits records"
    for path, text in declared.items():
        assert "LOGGER = logging.getLogger(__name__)" in text, path


def test_a_write_is_one_record_however_many_chunks_it_commits(
    storages: Storages, caplog: pytest.LogCaptureFixture
) -> None:
    dataset = storages.dataset("bronze.trading.quotes", field=Quote.into_field(), commit_row_size=2)
    try:
        with caplog.at_level(logging.INFO, logger="rekep"):
            dataset.append_arrow_table(quotes(9))
    finally:
        dataset.close()

    wrote = [record for record in caplog.records if "wrote" in record.message]
    assert len(wrote) == 1, [record.getMessage() for record in caplog.records]
    assert "trading.quotes created" in caplog.text, "and the table creation is its own record"


def test_the_detail_under_it_is_debug(storages: Storages, caplog: pytest.LogCaptureFixture) -> None:
    """Writing an output file is the detail beneath the INFO summary."""
    dataset = storages.dataset("bronze.trading.quotes", field=Quote.into_field())
    try:
        with caplog.at_level(logging.INFO, logger="rekep"):
            dataset.append_arrow_table(quotes(4), merge_by=False)
        assert not [record for record in caplog.records if record.levelno == logging.DEBUG]

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="rekep"):
            dataset.append_arrow_table(quotes(4), merge_by=False)
    finally:
        dataset.close()
    assert " output " in caplog.text and ".parquet" in caplog.text


def test_maintenance_records_what_it_returned(
    storages: Storages, caplog: pytest.LogCaptureFixture
) -> None:
    """Maintenance reports what it changed, so the record and the return value
    are the same numbers or one of them is wrong."""
    dataset = storages.dataset("bronze.trading.quotes", field=Quote.into_field())
    try:
        dataset.append_arrow_table(quotes(4))
        with caplog.at_level(logging.INFO, logger="rekep"):
            report = dataset.cleanup(retain=1)
    finally:
        dataset.close()

    assert f"expired {report['expired']} snapshots" in caplog.text
    assert f"swept {report['deleted']} files ({report['bytes']} bytes)" in caplog.text


def test_a_task_records_its_one_write(storages: Storages, caplog: pytest.LogCaptureFixture) -> None:
    """A task is one write of its target, so it is one INFO record of what it
    wrote -- and one of the table it created -- whatever it read."""
    with caplog.at_level(logging.INFO, logger="rekep"):
        parse_log_messages(CAPTURE.as_uri(), storages, DAY)

    infos = [record.getMessage() for record in caplog.records if record.levelno == logging.INFO]
    assert len([message for message in infos if "wrote" in message]) == 1, infos
    assert len([message for message in infos if "created" in message]) == 1, infos
