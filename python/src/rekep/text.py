"""The bridge text read, and the `log_messages` row it lands.

A captured line is read by the native text reader under the bridge's row
header, and the row it answers is the whole of the table: the sixteen event
columns the read settles over every line, the line past its header as
`body`, and one column per capture the header declares. Nothing here
declares a column; the read's own schema is narrowed to what Iceberg stores
and laid out like every other table of the graph.
"""

from __future__ import annotations

from yggdryl import TextOptions

from rekep.fields import Field
from rekep.fix import iceberg_event_field
from rekep.times import ULBRIDGE_ROWHEADER

#: The capture the read settles `currunix` from rather than storing it: the
#: record clock, which `parse_mtime` consumes at its native default.
RECORD_CLOCK = "mtime"

#: What a row header captures, and the one set a header must capture: the
#: record clock, and six columns named for what the read fills from them --
#: `msgpluginid`, `msgsessionid`, `msgctxid` and `msgseqnum` fill the FIX
#: fields of the same names when a line is parsed, and `msgthreadid` and
#: `loglevel` stay on the line.
CAPTURES = frozenset(
    {
        RECORD_CLOCK,
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    }
)

#: The name the text row's field carries.
LOG_MESSAGES_NAME = "log_messages"


def text_options(rowheader: str | None = None, timezone: str = "UTC") -> TextOptions:
    """The bridge read: the row header, the record clock and the line numbering.

    `rowheader` reads a bridge writing the same facts in a layout of its own:
    another clock precision, another bracket order, other text around them.
    Its capture names must be `CAPTURES` exactly, because the read drops a
    capture it fills nothing from in silence -- a renamed one lands as a
    column of nulls, an omitted one as a column no bridge fills -- so a
    header that differs is refused here, where the mismatch is still legible.

    `parse_mtime` stays at its native default, on: the header's `mtime`
    capture dates the line into `currunix`, read in `timezone` -- the zone
    the bridge prints its clock in, an IANA name -- and a line the header did
    not date takes its object's modification time. `start_rownum` numbers the
    first line 1, which is what `seqnum` states.
    """
    options = TextOptions()
    options.start_rownum = 1
    options.rowheader = ULBRIDGE_ROWHEADER if rowheader is None else rowheader
    options.timezone = timezone
    options.safe = False
    found = frozenset(options.capture_names)
    if found != CAPTURES:
        missing = sorted(CAPTURES - found)
        unknown = sorted(found - CAPTURES)
        said = [f"captures nothing for {', '.join(missing)}" if missing else ""]
        said += [
            f"captures {', '.join(unknown)}, which the read fills nothing from" if unknown else ""
        ]
        raise ValueError(f"row header {' and '.join(part for part in said if part)}")
    return options


def log_message_field(rowheader: str | None = None) -> Field:
    """The `log_messages` row: the native read's own schema, laid out for Iceberg.

    Keyed on `curruuid`, the line's identity; partitioned by the hour of
    `currunix`, the instant the read settled over the line; sorted within a
    partition by `currunix, seqnum, curruuid`.
    """
    schema = text_options(rowheader).source_field().into_arrow_schema()
    return iceberg_event_field(schema, LOG_MESSAGES_NAME)


__all__ = [
    "CAPTURES",
    "LOG_MESSAGES_NAME",
    "RECORD_CLOCK",
    "log_message_field",
    "text_options",
]
