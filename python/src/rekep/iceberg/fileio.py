"""Arrow's FileIO, taught to read a Windows drive letter.

`PyArrowFileIO.parse_location` splits a URI and glues netloc and path back
together, so `file:///C:/warehouse` comes out as `/C:/warehouse` -- a spelling
`pyarrow`'s local filesystem refuses on Windows with `WinError 123`. The same
split hands a bare `C:/warehouse` to `urlparse`, which reads the drive as a
one-letter URI scheme and refuses it as a filesystem. Both are the standard
spellings a Windows path arrives in, so the default FileIO cannot write a
local warehouse there at all.
"""

from __future__ import annotations

import os
import re

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties

#: A path whose first segment is a drive letter -- `C:/x` or `C:\x`.
_DRIVE = re.compile(r"^[A-Za-z]:[/\\]")

#: The slash a URI split leaves in front of one -- `/C:/x`.
_ROOTED_DRIVE = re.compile(r"^/+(?=[A-Za-z]:[/\\])")

#: Decided per host, held as data so a test can exercise the other answer.
_WINDOWS = os.name == "nt"


class ArrowFileIO(PyArrowFileIO):
    """`PyArrowFileIO` whose locations survive Windows.

    Only the parsing changes, and only where the default parse is wrong on
    the host itself: on Windows, `/C:/x` sheds the slash the URI split left
    in front of the drive, and a bare `C:/x` is a local path rather than a
    URI with scheme `c`. Everywhere else the parent's answer stands, so a
    POSIX directory literally named `C:` keeps meaning what it says.
    """

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        if _WINDOWS and _DRIVE.match(location):
            return "file", "", location
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties)
        if _WINDOWS and scheme == "file":
            path = _ROOTED_DRIVE.sub("", path)
        return scheme, netloc, path
