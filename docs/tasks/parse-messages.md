# Parse messages

`tasks/parse_messages/parse_messages.ipynb` streams text files through
`TextFile` or `TextFiles` and writes `text.messages`: one row per line,
structured and **not** resolved.

What a row carries:

- the header the regex found -- `unix`, `thread_name`, `plugin_code`, and the
  level, with the raw payload in `message`;
- provenance: `source_url` names the file and `source_rownum` the 1-based
  physical line, so `sed -n '<source_rownum>p' <source_url>` is the line;
- `protocol_code`, and `protocol_version` with `protocol_version_source`
  beside it;
- `msg_type`, which is one tag off the front of a message;
- `kwargs` at its unresolved fill level.

## What "unresolved" means

`kwargs` is the same Arrow struct the resolved rows use -- `tag`, `key`,
`value`, `namespace`, `comp` -- filled in only as far as structuration goes:

| member | at the message stage |
| --- | --- |
| `key` | the last path segment, as the line spells it |
| `value` | the raw text, exactly as written |
| `namespace` | the lead where it is a vendor prefix (`TECH.CLIENTID`) |
| `comp` | the lead where it is a group entry (`NoPartyIDs[0]`) |
| `tag` | the number only where the line spells one; `0` otherwise |

Telling a group entry from a vendor prefix needs no dictionary: an entry of a
repeating group is what carries a subscript, and everything else in front of a
name is somebody's own prefix. That is settled here and `parse_fix` never
revises it, so `namespace` and `comp` come through the second stage
byte-identical.

`tag == 0` is what says an entry is unresolved. It is `NOT NULL`, so a reader
splits resolved from unresolved with one predicate and never a null check.

## What it does read

The stage boundary holds with one honest amendment: `parse_messages` reads
`registry.versions` -- the version list, one small document -- because
resolving a protocol version needs to know which versions exist. It resolves
no field, no component and no enumerated value, and it opens none of the
sharded field documents.

It also has to **categorise**, because splitting a line into pairs needs the
rule that says which separator and which key style the line uses. The
categorised protocol is stored, so `parse_fix` knows how the row was split
rather than guessing.

## Why it is a stage of its own

A line is tokenised and structured once instead of once per parse. Re-running
`parse_fix` after a dictionary or a rules update -- a real operation, since
the dictionary changes and the rows do not -- skips both entirely and only
refills three members.

Measured on a 20,011-row capture, best of three:

| | seconds | rows/s |
| --- | ---: | ---: |
| full re-parse from text | 1.39 | 14,398 |
| `parse_messages` alone | 0.63 | 32,005 |
| `parse_fix` alone, over stored rows | 0.90 | 22,332 |

A re-parse is **1.55x** faster than going back to the text: 36% of the work
is gone, and it is the tokenising and structuring half. The `parse_fix`
figure is over rows already in memory, so a real re-parse adds the cost of
reading `text.messages` back.

## Retained, not transient

`text.messages` is kept. It is what a re-parse reads, so deleting it would
mean going back to the capture -- which is the thing this stage exists to
avoid. It is named `text.messages` rather than `fixmessage.messages`
deliberately: nothing in it has been read as FIX yet.

The adjacent `parse_messages.yml` selects the capture, the rules, the offline
FIX registry, the catalog, batch and commit sizes, and an optional
`[start, end)` interval.
