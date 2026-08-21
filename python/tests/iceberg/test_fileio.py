"""`ArrowFileIO.parse_location`, both hosts' answers on either host.

The Windows branch is data (`_WINDOWS`), so a POSIX runner can pin what a
Windows one would do and the other way round -- the whole point of the class
is behaviour CI's two legs do not share.
"""

import pytest

from rekep.iceberg import fileio
from rekep.iceberg.fileio import ArrowFileIO


@pytest.fixture
def windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fileio, "_WINDOWS", True)


@pytest.fixture
def posix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fileio, "_WINDOWS", False)


def test_a_file_uri_with_a_drive_sheds_the_leading_slash(windows: None) -> None:
    assert ArrowFileIO.parse_location("file:///C:/warehouse/t") == ("file", "", "C:/warehouse/t")


def test_a_bare_drive_path_is_local_not_a_scheme(windows: None) -> None:
    assert ArrowFileIO.parse_location("C:/warehouse/t") == ("file", "", "C:/warehouse/t")
    assert ArrowFileIO.parse_location("C:\\warehouse\\t") == ("file", "", "C:\\warehouse\\t")


def test_everything_without_a_drive_is_the_parents_answer(windows: None) -> None:
    assert ArrowFileIO.parse_location("file:///data/t") == ("file", "", "/data/t")
    assert ArrowFileIO.parse_location("file:/data/t") == ("file", "", "/data/t")
    assert ArrowFileIO.parse_location("s3://bucket/t") == ("s3", "bucket", "bucket/t")


def test_a_posix_directory_named_like_a_drive_keeps_meaning_what_it_says(posix: None) -> None:
    assert ArrowFileIO.parse_location("file:///C:/warehouse/t") == ("file", "", "/C:/warehouse/t")
