"""`Console`: styling that a terminal gets and a pipe never does."""

from __future__ import annotations

import io
import sys

import pytest

from rekep.console import ASCII_GLYPHS, CODES, GLYPHS, Console, supports_colour, supports_unicode


class Terminal(io.StringIO):
    """A stream that claims to be one, which is the whole of what colour asks."""

    encoding = "utf-8"

    def isatty(self) -> bool:
        return True


@pytest.fixture(autouse=True)
def isolated_colour_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Host colour preferences must not decide these explicit stream tests."""
    for name in ("NO_COLOR", "FORCE_COLOR", "TERM"):
        monkeypatch.delenv(name, raising=False)


def test_a_pipe_gets_the_same_text_without_the_escapes() -> None:
    """A CLI whose output is unreadable once redirected is one nobody can script."""
    written = io.StringIO()
    console = Console(stream=written)
    console.ok("landed")
    printed = written.getvalue()
    assert "\033" not in printed
    assert "landed" in printed


def test_a_terminal_gets_them() -> None:
    written = Terminal()
    Console(stream=written).ok("landed")
    assert "\033[" in written.getvalue()


def test_the_terminal_palette_matches_the_documentation() -> None:
    styles = {"reset", "bold", "dim", "italic", "underline"}
    assert set(CODES) - styles == {"white", "red", "orange", "yellow", "grey"}


@pytest.mark.parametrize(
    ("environment", "expected"),
    [({"NO_COLOR": "1"}, False), ({"FORCE_COLOR": "1"}, True), ({"TERM": "dumb"}, False)],
)
def test_the_environment_decides_before_the_stream_does(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str], expected: bool
) -> None:
    """`NO_COLOR` is a convention, and one that wins over being a terminal."""
    for name in ("NO_COLOR", "FORCE_COLOR", "TERM"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    assert supports_colour(Terminal()) is expected


def test_a_stream_that_cannot_encode_the_box_drawing_gets_ascii() -> None:
    """A table of mojibake is worse than a table of dashes."""

    class Narrow(io.StringIO):
        encoding = "ascii"

    assert not supports_unicode(Narrow())
    assert Console(stream=Narrow()).glyph("check") == ASCII_GLYPHS["check"]
    assert Console(stream=Terminal()).glyph("check") == GLYPHS["check"]


def test_a_named_stream_is_resolved_when_a_line_is_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A console built at import must not hold whatever `sys.stderr` was then."""
    console = Console(stream="stderr", colour=False)
    replaced = io.StringIO()
    monkeypatch.setattr(sys, "stderr", replaced)
    console.line("after")
    assert replaced.getvalue() == "after\n"


def test_a_table_is_sized_to_its_contents_and_says_when_there_are_none() -> None:
    written = io.StringIO()
    console = Console(stream=written, colour=False)
    console.table(("tag", "name"), [("8", "BeginString"), ("10", "CheckSum")])
    lines = written.getvalue().splitlines()
    assert lines[0].split() == ["tag", "name"]
    assert lines[1] == "  8    BeginString", "every column as wide as its widest cell"
    written.truncate(0), written.seek(0)
    console.table(("tag",), [])
    assert "nothing to show" in written.getvalue()


def test_a_panel_is_measured_on_the_text_and_not_on_the_escapes() -> None:
    """Otherwise a coloured row makes its own box three times too wide."""
    written = Terminal()
    console = Console(stream=written)
    console.panel("title", [console.style("row", "red")])
    widths = {len(line) for line in written.getvalue().splitlines()}
    assert len(widths) > 1, "the coloured row is longer in bytes than the borders"
    plain = io.StringIO()
    Console(stream=plain, colour=False).panel("title", ["row"])
    lines = plain.getvalue().splitlines()
    assert len({len(line) for line in lines}) == 1, "and every line of the box is one width"
    assert all(line[0] and line[-1] not in " " for line in lines), "closed on both sides"


def test_a_spinner_writes_nothing_a_pipe_would_have_to_read() -> None:
    """A few thousand carriage returns is not a progress report."""
    written = io.StringIO()
    with Console(stream=written).spinner("working"):
        pass
    assert "\r" not in written.getvalue()
    assert "working" in written.getvalue()


def test_a_spinner_on_a_terminal_animates_and_clears_after_itself() -> None:
    written = Terminal()
    with Console(stream=written).spinner("working"):
        pass
    assert written.getvalue().endswith("\033[2K")
