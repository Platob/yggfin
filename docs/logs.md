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

Header parsing extracts the timestamp, thread, driver, level, and message.
Continuation lines may be folded into the prior event. Compression is inferred
from the filename by Arrow.

## Parsed record

`Log` reuses the generic `Event` envelope. The raw-line digest is its stable
version identity. When a protocol key exists, `code`, `xcode`, and `xhash`
provide readable and hashed correlation without changing the raw digest.

For market rows, `symbol` is the best available instrument spelling: the FIX
symbol first, then a security/ISIN identifier when the source omitted one.
`code` remains the best record identifier, preferring order, client-order,
original-order, execution, and quote lifecycle keys before instrument keys.

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

- `fix_tags`: ordered repeated numeric tag/value entries.
- `keyval`: ordered non-FIX key/value entries.
- `fix_miss_tags`: unresolved raw keys.
- `parties`: structured FIX Parties entries with a flexible buffer for new
  members.

These are lists, not maps, because repeated keys and wire order are data.

## Categorization

Rules accept several regex patterns and use first match wins. Trading messages
share the `market` log table; known operational protocols use `misc_logs`; an
unmatched protocol lands in `unknown_logs`. No parsed line is dropped.

## Benchmark

`python/benchmarks/bench_text_file.py --quick` measures file/folder streaming,
batch sizes, and parser memory. `bench_fix.py --quick` isolates protocol
translation.
