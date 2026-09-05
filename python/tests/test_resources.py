"""Resource binding at the yggdryl boundary."""

from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow.fs
import pytest

from rekep.resources import read_bytes, resource


def test_a_relative_resource_derives_from_its_bound_root(tmp_path: Path) -> None:
    target = resource("nested/payload.bin", root=tmp_path)
    try:
        target.write_bytes(b"payload")
    finally:
        target.close()

    assert read_bytes("nested/payload.bin", root=tmp_path.as_uri()) == b"payload"


def test_an_injected_filesystem_owns_the_resource_path() -> None:
    filesystem = pyarrow.fs._MockFileSystem()
    filesystem.create_dir("bucket")
    with filesystem.open_output_stream("bucket/payload.bin") as stream:
        stream.write(b"payload")

    opened = resource("payload.bin", filesystem, root="bucket")
    try:
        assert str(opened.url) == "mock://bucket/payload.bin"
    finally:
        opened.close()
    assert read_bytes("bucket/payload.bin", filesystem) == b"payload"


def test_a_required_resource_refuses_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="missing.bin"):
        read_bytes(tmp_path / "missing.bin")


def test_a_required_resource_uses_yggdryls_content_codec(tmp_path: Path) -> None:
    compressed = tmp_path / "payload.txt.gz"
    compressed.write_bytes(gzip.compress(b"payload"))

    assert read_bytes(compressed) == b"payload"
