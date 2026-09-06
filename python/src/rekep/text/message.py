"""One physical text record before protocol parsing."""

from __future__ import annotations

from typing import Annotated, Any, Self

from yggdryl import scalar

from rekep.convert import Convertible
from rekep.fields import primary_key


@scalar(slots=True)
class Message(Convertible):
    """One source line and the header fields yggdryl captured from it."""

    sourceurl: Annotated[str, primary_key()] = ""
    """Yggdryl URL of the source text object."""

    sourcerownum: Annotated[int, primary_key()] = 0
    """1-based physical line number within the source object."""

    timestamp: str | None = None
    """Timestamp spelling captured from the line header."""

    threadname: str | None = None
    """Thread spelling captured from the line header."""

    plugin: str | None = None
    """Plugin spelling captured from the line header."""

    level: str | None = None
    """Severity spelling captured from the line header."""

    body: bytes = b""
    """Exact bytes after the matched line-header prefix."""

    def __post_init__(self) -> None:
        """Normalize the raw scalar values once."""
        self.sourceurl = str(self.sourceurl)
        self.sourcerownum = int(self.sourcerownum)
        if isinstance(self.body, str):
            self.body = self.body.encode("utf-8")
        elif not isinstance(self.body, bytes):
            self.body = bytes(self.body)

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)
