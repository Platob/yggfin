# parse_fix_bronze

Parse one capture window into settled FIX events and write `fix.bronze`.
Nothing in this task walks lifecycle chains.

## Task document

```json
{
  "name": "parse_fix_bronze",
  "application": "parse_fix_bronze.py",
  "parameters": {
    "messages": "logs.messages",
    "registry": null,
    "codec_options": null,
    "start": null,
    "end": null,
    "catalog": {
      "name": "rekep",
      "properties": {
        "type": "sql",
        "uri": "sqlite:///data/catalog.db",
        "warehouse": "data/warehouse"
      }
    }
  }
}
```

## The parse, and nothing after it

The task prunes `logs.messages` to `[start, end)` on `currunix`, the event the
read settled over each line, and reads the lines at the epoch pin beside them.
`currunix` is the partition column itself and is never null, so the predicate
names it and Iceberg projects the bounds through the hour transform: the
window's partitions and the pin's own hour are opened, and nothing else.
`fix_parse_arrow_reader` is the batch form of the
native `FixCodec.parse_text_arrow_reader`; one line may answer zero, one, or
several messages. Parsing and local enrichment are independent per event and
may execute concurrently. `threads` defaults to the available CPU count and
zero becomes one. Output order remains input order.

`codec_options: null` delegates every native default. An object is forwarded
unchanged to `FixCodec`; Python keeps no whitelist or second interpretation.
Common pins include
`batch_row_size`, `batch_byte_size`, `include_msgtypes`, `exclude_msgtypes`,
`threads`, and `snapshot_ns`; native construction rejects unknown names.
Batching defaults to 32,768 rows and 128 MiB.
`snapshot_ns` is normally zero for bronze because snapshots belong to a
lifecycle walk.
`lstrip` belongs to the raw reader's `TextOptions`, changes retained raw bytes,
and is neither a codec option nor enabled by this task.

The default absence values are empty text, `null`, `<null>`, `none`, `n/a`,
and `[n/a]`, after trimming and case folding. `null_values` replaces that set.

## Read, parse, narrow, write

`fix_message_field(codec)` is the native 123-column row used directly by the
parse door and `fix.bronze`. `sourceurl`, `rownum`, `threadId`, `level` and
`body` are raw to `logs.messages` and remain there, so no carried or unstored
schema is constructed. The reviewed **FixMsg** contract is
[`schemas/rekep/fixmsg.json`](../../contracts/index.md).

The dataset is keyed by `curruuid`, partitioned by hour of `currunix`, and sorted by
`currunix, seqnum, curruuid`. An overwrite is scoped to matching identifiers
inside affected partitions; append remains a blind generic Iceberg append.

## A row is an event, not a line

The line identity becomes provenance in `srcuuids`. The event key is
`curruuid`, so repeated captures of the same event meet while
distinct messages from one line remain distinct.

`crosscode` is the first non-empty business identifier in this order:
`OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID`, `MDReqID`.
Capture session and context do not replace it. When both capture values exist,
`identifiers["msgsectxid"]` records `session:context` without changing event
content identity.

## Schema and precision

The row includes `msgcat` (`MsgCat`), `exprtime`, and the normalized code columns
`isincode`, `cficode`, `cusipcode`, `sedolcode`, `bloombergcode`, `figicode`,
and `miccode`. Their native types include `FIGICode`. Code vocabularies are
stored once in the registry and referenced through `FIX:codeset`.

`fixentries` contains residual protocol data only. Lifted scalars and complete
groups are omitted once the output columns prove they represent them. Unknown,
ambiguous, failed-fit, and partial group content remains residual, and
`nofixentries` counts it. A row reconstructs canonical message semantics, not
the original pair order or framing bytes.

## Registry override

Set `registry` to a directory or URI only when the task must use an explicit
venue dictionary. The same registry must declare the table field and parse its
rows; it is never inferred from a batch.

## Row behavior

- `seqnum`, `prevuuid`, and `parentuuids` remain empty because bronze has not
  walked a chain.
- `srcuuids` preserves capture provenance by joining to raw
  `logs.messages.curruuid`; the FIX row carries no raw source columns.
- Undated messages use the codec's deterministic epoch floor until lifecycle
  can date them from message facts.
- Unknown names remain residual tag-zero entries.
- A replay of one window overwrites the same partition-scoped `curruuid` rows.

## Sample rows

The checked sample is regenerated from the current codec and contract; the
include owns its measured counts.

--8<-- "docs/pipeline/tasks/samples/parse-fix-bronze.md"

## Run

```bash
uv run --project python rekep task run tasks/parse_fix_bronze/parse_fix_bronze.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
```
