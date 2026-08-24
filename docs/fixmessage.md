# FixMessage

`FixMessage` is the one stored row a parsed log line becomes, whatever the line
carried. `TextFile` streams one log; `TextFiles` streams naturally sorted
rotations with one file open at a time. Local paths and any `pyarrow.fs` URI
share the API.

```python
from rekep import FixMessage, TextFiles

source = TextFiles.from_folder(
    "s3://bucket/capture",
    pattern="*.log*",
    timezone="Europe/Paris",
    static_values={"source": "bridge-1"},
)

reader = source.read_arrow_reader(
    schema=FixMessage.into_field(),
    batch_row_size=65_536,
    read_byte_size=4 << 20,
)
```

Header parsing extracts the timestamp, thread, plugin, level, and message.
Continuation lines may be folded into the prior event. Compression is inferred
from the filename by Arrow. Every row keeps where it was read from:
`source_url` names the file and `source_rownum` the 1-based physical line its
header sat on, so a folded continuation does not shift the rows after it.

## Parsed record

`FixMessage` reuses the generic `Event` envelope. The raw-line digest is its stable
version identity. When a protocol key exists, `code` and `xhash` provide
readable and hashed correlation without changing the raw digest.

For market rows, `symbol` is the best available instrument spelling: the FIX
symbol first, then a security/ISIN identifier when the source omitted one.
`code` is the readable **lifecycle** identifier, and prefers the key that
survives an amendment: `OrderID <37>`, then `OrigClOrdID <41>`, then
`ClOrdID <11>`, then execution and quote keys, then instrument keys.
`codes` is the map beside it, for an identifier the source spelled that has no
column of its own -- empty on a parsed FIX line, where each has one.

Important FIX fields are promoted to snake-case columns. Their metadata keeps
the canonical registry name, tag, datatype, description, version, and values.
Examples include:

- `msg_seq_num` / `MsgSeqNum`, retained only on the raw FixMessage;
- `msg_type` / `MsgType`;
- `orig_cl_ord_id` / `OrigClOrdID`;
- `transact_time` / `TransactTime`;
- order, execution, quote, price, quantity, instrument, and clock fields.

FIX UTC timestamps are stored as real Arrow timestamps at microsecond
precision. A timestamp whose timezone is not documented remains naive rather
than being guessed.

## Lossless protocol residue

- `kwargs`: every field the message carried and no column took, one entry each:
  - `tag`: what the dictionary answers for the key, or `0` where nothing does.
  - `key`: the field's own name, and nothing else.
  - `value`: what the line wrote for it.
  - `comp`: the FIX component or repeating-group entry it sat in, where the
    dictionary declares that container -- `NoPartyIDs[0]`.
  - `namespace`: whatever stood in front of the name where it does not, which
    is what a vendor prefix is -- `TECH` in `TECH.CLIENTID`.
- `parties`: structured FIX Parties entries with a flexible buffer for new
  members.
- `trd_reg_timestamps`: structured FIX TrdRegTimestamps entries -- the
  regulatory clock, with the same buffer. Both columns are filled by a
  `ComponentGroup` reading its component's own declaration; see
  [FIX](fix.md#groups-and-components).

These are lists, not maps, because repeated keys and wire order are data. At
most one of `comp` and `namespace` is set, and either one joined to `key` by a
dot is the key exactly as the line rendered it, so the split loses nothing.

What a value *means* is not stored beside it. It is a fact about the
dictionary and the value rather than about the row, so it is derived at read
time -- `FieldAccess(...).reading(row.kwargs, 54).meaning` is `"Buy"` -- and a
row read under a newer dictionary says what that dictionary says.

## Categorization

Rules accept several regex patterns and use first match wins. Trading messages
share the `market` table; known operational protocols use `misc`; an
unmatched protocol lands in `unknown`. No parsed line is dropped.

## One stored shape

`fixmessage.market`, `fixmessage.misc` and `fixmessage.unknown` hold the same
`FixMessage` class under one contract: a reader unions the three tables with
one schema and no cast. A row that could not be used as a FIX message is the
same `FixMessage` with the FIX columns null -- not a second model. The content
lives in two columns, and one of them has two fill levels:

| column | what it holds | `market` | `misc` / `unknown` |
| --- | --- | --- | --- |
| `message` | the raw line, unsplit | null | populated |
| `kwargs` | `tag`, `key`, `value`, `namespace`, `comp` per field, in wire order | populated, resolved | populated, unresolved |

There is no separate column for the message-level split: the unresolved and the
FIX-resolved form are the same struct at two fill levels. `tag` is `NOT NULL`
and `tag == 0` is what says an entry is unresolved -- the line did not spell a
tag and no dictionary answered for its key. On `market` rows `kwargs` carries
everything the raw line held, so `message` is null there; on `misc` and
`unknown` rows the raw string is still the content of record, so `message` is
populated and `kwargs` stays at its unresolved fill level. An all-null
`message` column run-length and dictionary encodes to nothing on disk, which
is what makes the single shape affordable.

## Benchmark

`python/benchmarks/bench_text_file.py --quick` measures file/folder streaming,
batch sizes, and parser memory. `bench_fix.py --quick` isolates protocol
translation.
