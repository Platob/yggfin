"""The yggdryl seam that produces raw Message batches."""

from __future__ import annotations

import gzip
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep.resources import resource
from rekep.text import Message

ROOT = Path(__file__).resolve().parents[3]
APPLICATION = ROOT / "tasks" / "parse_messages" / "parse_messages.py"


@pytest.fixture(scope="module")
def task_setup() -> dict[str, Any]:
    """Import the application and expose its setup-owned integration functions."""
    specification = importlib.util.spec_from_file_location("test_parse_messages_task", APPLICATION)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return dict(module.app._setup._glbls)


def test_selected_files_are_raw_physical_message_batches(
    tmp_path: Path, task_setup: dict[str, Any]
) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (tmp_path / "a.log").write_bytes(
        b"2026-08-14 00:05:01.167_520 [worker] [feed] (INFO) first\r\n  traceback\r\n"
    )
    (nested / "b.log.gz").write_bytes(
        gzip.compress(b"20260824-10:00:01.123 [worker-2] [feed-2] second\n")
    )
    (nested / "ignored.txt").write_bytes(b"not selected\n")

    location = resource(tmp_path)
    sources = task_setup["message_sources"]((location,), "*.log*", True)
    options = task_setup["text_options"](None, 1)
    field = Message.into_field()
    batches = list(task_setup["message_batches"](sources, options, field))
    table = pyarrow.Table.from_batches(batches, schema=field.into_arrow_schema())

    assert table.schema.equals(field.into_arrow_schema(), check_metadata=True)
    assert table.column("sourcerownum").to_pylist() == [1, 2, 1]
    assert table.column("timestamp").to_pylist() == [
        "2026-08-14 00:05:01.167_520",
        None,
        "20260824-10:00:01.123",
    ]
    assert table.column("threadname").to_pylist() == ["worker", None, "worker-2"]
    assert table.column("plugin").to_pylist() == ["feed", None, "feed-2"]
    assert table.column("level").to_pylist() == ["INFO", None, None]
    assert table.column("body").to_pylist() == [b"first", b"  traceback", b"second"]
    assert all(value.startswith("file://") for value in table.column("sourceurl").to_pylist())


def test_selection_respects_recursion_and_refuses_a_missing_root(
    tmp_path: Path, task_setup: dict[str, Any]
) -> None:
    (tmp_path / "top.log").write_bytes(b"top\n")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "nested.log").write_bytes(b"nested\n")

    direct = list(task_setup["message_sources"]((resource(tmp_path),), "*.log", False))
    assert [source.name for source in direct] == ["top.log"]

    missing = task_setup["message_sources"]((resource(tmp_path / "missing"),), "*.log", True)
    with pytest.raises(FileNotFoundError):
        next(missing)
