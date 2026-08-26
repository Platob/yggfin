"""Terminal styling: colour, box drawing, and a spinner, with no dependency.

One place decides what `rekep` looks like, so a prompt, a table and an error
agree. Everything degrades to plain ASCII on its own: a pipe, a dumb terminal
or `NO_COLOR` gets the same text without the escapes, because a CLI whose
output is unreadable once redirected is a CLI nobody can script.
"""

from __future__ import annotations

import contextlib
import itertools
import os
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from typing import Any

#: SGR parameters, by the name the rest of this package uses. Kept as numbers
#: rather than as whole escapes so `style` can combine them in one sequence.
CODES: dict[str, str] = {
    "reset": "0",
    "bold": "1",
    "dim": "2",
    "italic": "3",
    "underline": "4",
    "white": "97",
    "red": "91",
    "orange": "38;5;208",
    "yellow": "93",
    "grey": "90",
}

#: What a box, a rule and a bullet are drawn with. Two sets, because a Windows
#: console page or a redirected file may carry no box drawing at all, and a
#: table of mojibake is worse than a table of dashes.
GLYPHS: dict[str, str] = {
    "top_left": "╭",
    "top_right": "╮",
    "bottom_left": "╰",
    "bottom_right": "╯",
    "horizontal": "─",
    "vertical": "│",
    "tee_left": "├",
    "tee_right": "┤",
    "bullet": "•",
    "arrow": "→",
    "check": "✓",
    "cross": "✗",
    "warn": "▲",
    "prompt": "❯",
    "ellipsis": "…",
}
ASCII_GLYPHS: dict[str, str] = {
    "top_left": "+",
    "top_right": "+",
    "bottom_left": "+",
    "bottom_right": "+",
    "horizontal": "-",
    "vertical": "|",
    "tee_left": "+",
    "tee_right": "+",
    "bullet": "*",
    "arrow": "->",
    "check": "v",
    "cross": "x",
    "warn": "!",
    "prompt": ">",
    "ellipsis": "...",
}

#: Frames the spinner cycles, and how long each is shown. Braille because it
#: animates in one cell; the ASCII fallback keeps the same cadence.
FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
ASCII_FRAMES = "|/-\\"
FRAME_SECONDS = 0.08


def supports_colour(stream: Any = None) -> bool:
    """Whether `stream` should be written to with escapes at all.

    `NO_COLOR` wins over everything, as its convention requires; `FORCE_COLOR`
    is what a CI that renders escapes sets. Otherwise it is a terminal or it
    is not.
    """
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    if os.environ.get("TERM", "") == "dumb":
        return False
    stream = sys.stdout if stream is None else stream
    return bool(getattr(stream, "isatty", lambda: False)())


def supports_unicode(stream: Any = None) -> bool:
    """Whether the stream's encoding can carry the box drawing above."""
    stream = sys.stdout if stream is None else stream
    encoding = getattr(stream, "encoding", None) or ""
    try:
        GLYPHS["top_left"].encode(encoding or "ascii")
    except (LookupError, UnicodeEncodeError):
        return False
    return True


class Console:
    """Where output goes, and what it is allowed to look like there.

    `stream` is a file, or one of the names `"stdout"` and `"stderr"`. A name
    and not the object it stands for, because a console built at import time
    would otherwise hold whatever `sys.stderr` was *then* -- which is not what
    a test harness, a shell redirect or a captured subprocess is writing to.
    Everything derived from the stream is therefore decided per write.
    """

    #: The standard streams, by the name a console may be built with.
    STANDARD: dict[str, str] = {"stdout": "stdout", "stderr": "stderr"}

    def __init__(self, stream: Any = None, colour: bool | None = None, glyphs: bool | None = None):
        self._stream = "stdout" if stream is None else stream
        self._colour = colour
        self._glyphs = glyphs

    @property
    def stream(self) -> Any:
        """Where this writes, resolved now rather than when it was built."""
        named = self.STANDARD.get(self._stream) if isinstance(self._stream, str) else None
        return getattr(sys, named) if named else self._stream

    @property
    def colour(self) -> bool:
        """Whether escapes are written at all, decided against the live stream."""
        return supports_colour(self.stream) if self._colour is None else self._colour

    @property
    def glyphs(self) -> dict[str, str]:
        """Which drawing set the live stream can carry."""
        drawable = supports_unicode(self.stream) if self._glyphs is None else self._glyphs
        return GLYPHS if drawable else ASCII_GLYPHS

    # -- writing ------------------------------------------------------------

    def style(self, text: str, *names: str) -> str:
        """`text` wrapped in the named styles, or unchanged where there is no colour."""
        if not self.colour or not names:
            return text
        codes = ";".join(CODES[name] for name in names if name in CODES)
        return f"\033[{codes}m{text}\033[0m" if codes else text

    def glyph(self, name: str) -> str:
        """One drawing character, in whichever set this stream can carry."""
        return self.glyphs[name]

    def line(self, text: str = "") -> None:
        """One line out, flushed, so a prompt after it is not stranded in a buffer."""
        print(text, file=self.stream, flush=True)

    def rule(self, title: str = "", width: int = 0) -> None:
        """A horizontal rule, with a title sitting in it when there is one."""
        span = width or self.width
        bar = self.glyph("horizontal")
        if not title:
            self.line(self.style(bar * span, "grey"))
            return
        lead = bar * 2
        rest = max(0, span - len(title) - 4)
        self.line(
            self.style(lead, "grey")
            + " "
            + self.style(title, "bold", "orange")
            + " "
            + self.style(bar * rest, "grey")
        )

    def panel(self, title: str, rows: Sequence[str]) -> None:
        """A titled box around `rows`, sized to the widest of them.

        Every line is `inner + 4` wide -- two borders and the two spaces
        inside them -- so the box closes on both sides. Widths are measured on
        `_plain`, because a coloured row is longer in bytes than on screen and
        measuring the bytes makes its own box three times too wide.
        """
        glyph = self.glyph
        inner = max([len(_plain(row)) for row in rows] + [len(title) + 2, 8])
        inner = min(inner, max(self.width - 4, 8))
        bar = glyph("horizontal")
        self.line(
            self.style(glyph("top_left") + bar, "grey")
            + " "
            + self.style(title, "bold", "white")
            + " "
            + self.style(bar * max(0, inner - len(title) - 1) + glyph("top_right"), "grey")
        )
        for row in rows:
            pad = " " * max(0, inner - len(_plain(row)))
            edge = self.style(glyph("vertical"), "grey")
            self.line(f"{edge} {row}{pad} {edge}")
        self.line(
            self.style(glyph("bottom_left") + bar * (inner + 2) + glyph("bottom_right"), "grey")
        )

    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        """Columns sized to their contents, header dimmed, nothing wrapped."""
        if not rows:
            self.line(self.style(f"  {self.glyph('bullet')} nothing to show", "grey"))
            return
        columns = [
            max(len(_plain(str(header))), *(len(_plain(str(row[index]))) for row in rows))
            for index, header in enumerate(headers)
        ]
        self.line(
            "  "
            + "  ".join(
                self.style(str(header).ljust(width), "bold", "grey")
                for header, width in zip(headers, columns, strict=True)
            )
        )
        for row in rows:
            self.line(
                "  "
                + "  ".join(
                    str(cell) + " " * max(0, width - len(_plain(str(cell))))
                    for cell, width in zip(row, columns, strict=True)
                )
            )

    def ok(self, text: str) -> None:
        """One thing that worked."""
        self.line(f"  {self.style(self.glyph('check'), 'yellow')} {text}")

    def fail(self, text: str) -> None:
        """One thing that did not."""
        self.line(f"  {self.style(self.glyph('cross'), 'red')} {text}")

    def warn(self, text: str) -> None:
        """One thing worth saying before it becomes a failure."""
        self.line(f"  {self.style(self.glyph('warn'), 'orange')} {text}")

    def note(self, text: str) -> None:
        """Context nobody has to read."""
        self.line(self.style(f"  {text}", "grey"))

    @property
    def width(self) -> int:
        """How wide the terminal is, with a sane width when it will not say."""
        try:
            return max(40, min(os.get_terminal_size().columns, 120))
        except OSError:
            return 80

    # -- waiting ------------------------------------------------------------

    @contextlib.contextmanager
    def spinner(self, text: str) -> Iterator[None]:
        """Animate `text` while the block runs, and clear the line after it.

        Only on a terminal: a spinner written to a pipe is a few thousand
        carriage returns in whatever read it.
        """
        if not self.colour:
            self.note(f"{text}{self.glyph('ellipsis')}")
            yield
            return
        frames = FRAMES if self.glyphs is GLYPHS else ASCII_FRAMES
        stop = threading.Event()

        def spin() -> None:
            for frame in itertools.cycle(frames):
                if stop.is_set():
                    return
                self.stream.write(
                    f"\r  {self.style(frame, 'orange')} {text}{self.glyph('ellipsis')}"
                )
                self.stream.flush()
                time.sleep(FRAME_SECONDS)

        thread = threading.Thread(target=spin, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=1.0)
            self.stream.write("\r\033[2K")
            self.stream.flush()


def _plain(text: str) -> str:
    """`text` with any escapes removed, which is what its width is measured on."""
    if "\033" not in text:
        return text
    out: list[str] = []
    skipping = False
    for character in text:
        if skipping:
            skipping = character not in "m"
            continue
        if character == "\033":
            skipping = True
            continue
        out.append(character)
    return "".join(out)
