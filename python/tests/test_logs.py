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

from rekep import scalar
from rekep.fields import partition_key, primary_key
from rekep.iceberg import IcebergDataset

from .conftest import catalog_properties


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
            "import logging, rekep, rekep.iceberg.dataset, rekep.pipeline, rekep.text.message;"
            " root = logging.getLogger('rekep');"
            " print(bool(root.handlers), root.level)",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert checked.stdout.split() == ["False", "0"], checked.stdout


@pytest.mark.integration
def test_a_write_is_one_record_however_many_chunks_it_commits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        commit_row_size=2,
    )

    with caplog.at_level(logging.INFO, logger="rekep"):
        dataset.append_arrow_table(quotes(9))

    wrote = [one for one in caplog.records if "wrote" in one.message]
    assert len(wrote) == 1, [one.getMessage() for one in caplog.records]
    assert "trading.quotes created" in caplog.text, "and the table creation is its own record"


@pytest.mark.integration
def test_the_detail_under_it_is_debug(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Writing an output file is the detail beneath the INFO summary."""
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )

    with caplog.at_level(logging.INFO, logger="rekep"):
        dataset.append_arrow_table(quotes(4))
    assert not [one for one in caplog.records if one.levelno == logging.DEBUG]

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="rekep"):
        dataset.append_arrow_table(quotes(4))
    assert " output " in caplog.text and ".parquet" in caplog.text


@pytest.mark.integration
def test_maintenance_records_what_it_returned(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """AGENTS.md requires maintenance to report what it changed, so the record
    and the return value are the same numbers or one of them is wrong."""
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    dataset.append_arrow_table(quotes(4))

    with caplog.at_level(logging.INFO, logger="rekep"):
        report = dataset.cleanup(retain=1)

    assert f"expired {report['expired']} snapshots" in caplog.text
    assert f"swept {report['deleted']} files ({report['bytes']} bytes)" in caplog.text


def test_every_emitting_module_is_named_by_its_own_path() -> None:
    """A record says which module wrote it, and a grep for that name reaches
    the line. One shared `getLogger("rekep")` would say nothing."""
    source = Path(__file__).resolve().parents[1] / "src" / "rekep"
    declared = {
        path: text
        for path in source.rglob("*.py")
        if "LOGGER = logging.getLogger(" in (text := path.read_text(encoding="utf-8"))
    }

    assert declared, "no module emits records"
    for path, text in declared.items():
        assert "LOGGER = logging.getLogger(__name__)" in text, path
