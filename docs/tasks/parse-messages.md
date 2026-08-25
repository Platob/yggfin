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

End to end through the tasks, one hour out of a capture, the saving is what
the text scan costs -- and that grows with the archive while `parse_fix` stays
proportional to the interval:

| archive | full re-parse | `parse_fix` alone | saved |
| --- | ---: | ---: | ---: |
| 1 file, 20,011 lines, 4.9 MiB | 15.1 s | 11.8 s | 22% |
| 8 files, 170,784 lines, 40 MiB | 62.4 s | 55.4 s | 11% |

Each figure includes about 2.9 s of interpreter and kernel start-up, which is
what a task costs before it reads anything.

## One identity, whichever route the capture took

`hash` is what every merge upserts on, so it is not the parser's to invent.
Both routes end on `FixMessage.identified` over the same columns -- the parser
reading a capture whole, and `parse_fix` reading stored message rows -- and
the digest is taken over the parsed values, plus `source_url` and
`source_rownum`. Those two are what keep two byte-identical lines in two
captures two rows rather than one.

## What the pipeline produced, twice

Two consecutive hourly intervals of a 20,011-line capture, then the first
interval again. The third run read the same rows and wrote nothing anywhere;
rows, file counts and the digest of every table's sorted hashes came back
unchanged.

| table | rows | files |
| --- | ---: | ---: |
| `fixmessage.market` | 3,607 | 4 |
| `fixmessage.misc` | 1,028 | 2 |
| `fixmessage.unknown` | 514 | 2 |
| `market.books` | 3,072 | 2 |
| `market.executions` | 1,545 | 2 |
| `market.instruments` | 6 | 2 |
| `market.orders` | 3,602 | 2 |
| `text.messages` | 5,143 | 2 |

`parse_market` is the one stage that costs more on the second interval --
11.6s against 19.9s -- because it seeds from the previous hour's books,
which is the incremental path doing its job.

## Retained, not transient

`text.messages` is kept. It is what a re-parse reads, so deleting it would
mean going back to the capture -- which is the thing this stage exists to
avoid. It is named `text.messages` rather than `fixmessage.messages`
deliberately: nothing in it has been read as FIX yet.

The adjacent `parse_messages.yml` selects the capture, the offline FIX
registry, the catalog, batch and commit sizes, and an optional `[start, end)`
interval. What the capture itself looks like -- its header, which lines carry
a message, how a field's values read, what counts as absent -- is declared
there too: see [Configuring a parse](../configuring.md).
