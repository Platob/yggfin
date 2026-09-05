"""One physical text record before protocol parsing."""

from __future__ import annotations

import functools
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Self

from rekep.convert import Convertible
from rekep.fields import Field, scalar

_CONTRACT_METADATA = MappingProxyType({"version": "2"})


@scalar(slots=True)
class Message(Convertible):
    """One source line and the header fields yggdryl captured from it."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with raw-message schemas."""
        return _CONTRACT_METADATA

    sourceurl: Annotated[str, Field.primary_key(), Field.column("SourceURL")] = ""
    """Absolute URI of the source text object."""

    sourcerownum: Annotated[int, Field.primary_key(), Field.column("SourceRownum")] = 0
    """1-based physical line number within the source object."""

    timestamp: str | None = None
    """Timestamp spelling captured from the line header."""

    threadname: Annotated[str | None, Field.column("ThreadName")] = None
    """Thread spelling captured from the line header."""

    plugin: Annotated[str | None, Field.column("Plugin")] = None
    """Plugin spelling captured from the line header."""

    level: Annotated[str | None, Field.column("Level")] = None
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
