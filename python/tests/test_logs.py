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
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, scalar
from rekep.fields import partition_key, primary_key
from rekep.iceberg import IcebergDataset
from rekep.logs import COMMAND_LEVEL, ROOT, TASK_LEVEL, Stage, configure

from .conftest import catalog_properties


@scalar
class Quote(Convertible):
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
        schema=Quote.field().into_arrow_schema(),
    )


@pytest.fixture(autouse=True)
def _unconfigured() -> Iterator[None]:
    """Leave the package's logger as this module found it.

    `configure` is global by design, so a test that calls it would otherwise
    decide the level for everything that ran after it.
    """
    logger = logging.getLogger(ROOT)
    held = (list(logger.handlers), logger.level, logger.propagate)
    yield
    logger.handlers, logger.level, logger.propagate = held


def test_importing_the_package_configures_nothing() -> None:
    """A library that installs a handler has decided for its caller. Without
    one the standard library's last resort carries WARNING and above, which is
    what every consumer saw before this module existed.

    In a subprocess, because this is a claim about a fresh interpreter and
    every other test here configures the logger this one is looking at.
    """
    checked = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            "import logging, rekep, rekep.iceberg.dataset, rekep.text.message;"
            " root = logging.getLogger('rekep');"
            " print(bool(root.handlers), root.level)",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert checked.stdout.split() == ["False", "0"], checked.stdout


def test_configure_is_idempotent_and_scoped() -> None:
    """A process runs two tasks; the second must not print everything twice."""
    logger = configure("DEBUG")
    again = configure("INFO")

    assert logger is again
    assert len(again.handlers) == 1, "one handler, however many times it is configured"
    assert again.level == logging.INFO
    assert not again.propagate, "the root handler would print these a second time"
    assert logging.getLogger().level != logging.DEBUG, "the root logger is not ours to set"


def test_the_two_defaults_differ_because_the_readers_do() -> None:
    """A person at a terminal is reading `Console`; a task log is read after
    the fact by somebody asking what happened."""
    assert TASK_LEVEL == "INFO"
    assert COMMAND_LEVEL == "WARNING"


@pytest.mark.integration
def test_a_write_is_one_record_however_many_chunks_it_commits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        commit_row_size=2,
    )

    with caplog.at_level(logging.INFO, logger=ROOT):
        dataset.append_arrow_table(quotes(9))

    wrote = [one for one in caplog.records if "wrote" in one.message]
    assert len(wrote) == 1, [one.getMessage() for one in caplog.records]
    assert "trading.quotes created" in caplog.text, "and the table creation is its own record"


@pytest.mark.integration
def test_the_detail_under_it_is_debug(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Writing a file and casting a stream are what INFO is a summary of."""
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )

    with caplog.at_level(logging.INFO, logger=ROOT):
        dataset.append_arrow_table(quotes(4))
    assert not [one for one in caplog.records if one.levelno == logging.DEBUG]

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=ROOT):
        dataset.append_arrow_table(quotes(4))
    assert " output " in caplog.text and ".parquet" in caplog.text
    assert "casting a stream" in caplog.text


@pytest.mark.integration
def test_maintenance_records_what_it_returned(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """AGENTS.md requires maintenance to report what it changed, so the record
    and the return value are the same numbers or one of them is wrong."""
    dataset = IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    dataset.append_arrow_table(quotes(4))

    with caplog.at_level(logging.INFO, logger=ROOT):
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


# -- a task's run ------------------------------------------------------------


def test_every_task_returns_the_same_keys(caplog: pytest.LogCaptureFixture) -> None:
    """A task-specific result extends the shared stage shape."""
    with caplog.at_level(logging.INFO, logger=ROOT):
        stage = Stage(
            "parse_messages",
            sources={"capture": "data/capture"},
            targets={"messages": "logs.messages"},
            window=(1_755_000_000_000_000_000, 1_755_003_600_000_000_000),
        )
        result = stage.finished(read=11, written=9, media="text")

    assert set(result) == {
        "task",
        "read",
        "written",
        "skipped",
        "sources",
        "targets",
        "window",
        "elapsed_ms",
        "media",
    }
    assert result["task"] == "parse_messages"
    assert result["skipped"] == 2, "what was read and not written, unless a task says otherwise"
    assert result["window"] == {
        "start": 1_755_000_000_000_000_000,
        "end": 1_755_003_600_000_000_000,
    }
    assert isinstance(result["elapsed_ms"], int)
    assert result["media"] == "text", "what a task alone knows keeps its own name"


def test_a_stage_records_the_numbers_it_returns(caplog: pytest.LogCaptureFixture) -> None:
    """The record and the result are the same numbers or one of them is wrong."""
    with caplog.at_level(logging.INFO, logger=ROOT):
        stage = Stage("parse_messages", sources={"capture": "data/capture"})
        stage.targets["messages"] = "logs.messages"
        stage.says("parsed %d messages from the capture", 2)
        result = stage.finished(read=2, written=2)

    opened, said, closed = (one.getMessage() for one in caplog.records)
    assert opened == "parse_messages reading capture=data/capture"
    assert said == "parse_messages parsed 2 messages from the capture"
    assert closed.startswith(
        f"parse_messages finished: {result['read']} read, {result['written']} written, "
        f"{result['skipped']} skipped"
    )
    assert "messages=logs.messages" in closed, "and where it put them"


def test_a_run_with_no_interval_says_so_rather_than_spelling_two_nulls(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An open window is every row the source holds; a reader should not have
    to decode `None` twice to learn that."""
    with caplog.at_level(logging.INFO, logger=ROOT):
        Stage("parse_messages", sources={"capture": "data/capture"})

    assert caplog.records[0].getMessage() == "parse_messages reading capture=data/capture"


def test_a_stage_that_read_nowhere_still_says_which_task_it_is(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger=ROOT):
        Stage("optimize_iceberg")

    assert caplog.records[0].getMessage() == "optimize_iceberg reading its source"
