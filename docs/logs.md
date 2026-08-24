# Logs

`TextFile` streams one log; `TextFiles` streams naturally sorted rotations with
one file open at a time. Local paths and any `pyarrow.fs` URI share the API.

```python
from rekep import Log, TextFiles

source = TextFiles.from_folder(
    "s3://bucket/capture",
    pattern="*.log*",
    timezone="Europe/Paris",
    static_values={"source": "bridge-1"},
)

reader = source.read_arrow_reader(
    schema=Log.into_field(),
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

`Log` reuses the generic `Event` envelope. The raw-line digest is its stable
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

- `msg_seq_num` / `MsgSeqNum`, retained only on the raw Log;
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
  - `trans`: what that value means, where the field enumerates its values.
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

## Categorization

Rules accept several regex patterns and use first match wins. Trading messages
share the `market` log table; known operational protocols use `misc_logs`; an
unmatched protocol lands in `unknown_logs`. No parsed line is dropped.

## Benchmark

`python/benchmarks/bench_text_file.py --quick` measures file/folder streaming,
batch sizes, and parser memory. `bench_fix.py --quick` isolates protocol
translation.
