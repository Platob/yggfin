# Parse messages

`tasks/parse_messages/parse_messages.ipynb` streams text files through
`TextFile` or `TextFiles` and writes `logs.messages`. It reads the log record,
not the protocol inside it.

Each `Message` row contains:

- the recording time in `unix`, with its `unix_partition`;
- `source_url` and the 1-based physical `source_rownum`;
- `thread_name` and `plugin_code` from the configured header;
- the unsplit payload in `message`;
- `hash`, the stable identity of that source row.

FIX tags, field names, protocol versions, event categories, components and
typed values do not belong to this stage. A payload beginning with `8=FIX` is
stored exactly like any other payload. `parse_fix` owns the first protocol
read.

## Why the table is retained

`logs.messages` is the protocol-neutral source for later parsers. A dictionary,
field rule or protocol rule can change without reopening compressed logs or
listing the source object-store prefix again. Re-running a protocol parser does
read `message` again because this table deliberately stores no partial parse.

The row identity includes its source location and row number, so identical
payloads in two captures remain two source records. The table is sorted by
`(unix, hash)` and partitioned from the recording time; a later parser may move
its own event time without changing which source interval owns the row.

## Configuration

The adjacent `parse_messages.yml` selects the source, filename pattern, header
regex, timezone, static columns, catalog, branch and batch sizes. Its
`exclude_plugins` list removes exact, case-sensitive `plugin_code` values before
message identities are built; an empty list keeps every plugin. It contains no
FIX dictionary or protocol rules. Those parameters live beside
`parse_fix.ipynb`, where they are used.
