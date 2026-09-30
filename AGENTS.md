# Coding patterns

Optimize Rust core behavior in Yggdryl first, then Python and JavaScript
bindings, then rekep documentation. Keep one obvious implementation per
behavior.

## Writing

- Write for an agent searching for one fact.
- Prefer deletion to compatibility layers or deprecation.
- Keep docstrings synthetic: what the object is, then only hidden constraints.
- Put a contract beside its owner and link to it instead of repeating it.
- Keep examples executable and prose connective.

## Ownership

- The native dependency is the exact Yggdryl 0.1.18 release declared in
  `python/pyproject.toml` and locked in `python/uv.lock`. Public applications
  and documentation import only `rekep`, and documentation never names the
  dependency: a capability a reader needs is re-exported through `rekep`.
  The one exception is the command lines of the serving pages -- those
  `SERVING_PAGES` in `python/tests/test_docs.py` lists -- which name the
  serving binary in bash fences.
- Yggdryl owns `Field`, scalar compilation, resource binding, filesystems,
  streams, codecs, decompression, text media and the text row's event
  columns, FIX registries and their process default, FIX batch parsing, the
  fixed `fixmsg` row, the lifecycle stage after the parse, the `State`
  lifecycle enum, the `Side` and `MarketDataKind` enums, each event's place
  among the events of its instant, the split of a fill report into its
  execution and of a trade into one execution per side, and the book fold
  with its `MIC:CFI` key.
- Arrow owns columnar shape conversions and kernels.
- PyIceberg owns table conversion, ids, snapshots, scan planning, and commits.
- Rekep owns `Storages` (one catalog per layer), the bridge read's options and
  capture contract in `rekep.text`, the narrow PyArrow/PyIceberg seam, and the
  `rekep.pipeline` tasks that compose them into tables.
- Never add a second Field class, filesystem/path layer, text reader, codec, or
  registry in rekep, and never declare a table column the native read, the
  registry or the book fold already states.

The deleted Rekep FIX and market implementation is not a compatibility target.

## Fields and Arrow

- `rekep.Field is yggdryl.Field`.
- Use native `@yggdryl.scalar` and `Annotated` options for declarations.
- Arrow schema metadata is authoritative; portable JSON derives from it.
- `Field` JSON is the runtime declaration form; `iceberg_contract` is the
  published one, and it states only what an Iceberg schema, spec and sort
  order can.
- Use Yggdryl `Field.apply_arrow_*` at producer and consumer boundaries so
  cast, derived partitions, and digests run in their native order.
- Absence is the target field's own nullability at every Arrow boundary: a
  column may be absent or null only where the field declares it nullable, and
  a table's key column is required even where a native row states it nullable.
- Do not use Python row loops for Arrow shape conversion.

## Resources and text

- Bind paths and URIs with `IOBase`; preserve injected Arrow filesystem
  identity and opaque paths.
- `IOBase` and `TextOptions` own traversal, header capture, decompression, and
  physical-line batching.
- A text row names its source only through the event columns the native
  read states: `crosscode` is the object it was read from, as the identifier
  the read was addressed under, and `seqnum` is its row number. Neither is a
  column of its own beside the event, and `body` is the line past its header.
- Every capture a row header declares is named for what the native read fills
  from it -- one lowercase column for each of the bracket's own facts, named
  for the fact it holds, and the settled `currunix` for `mtime`, the record
  clock, which `parse_mtime` consumes at its native default and never lands
  beside it -- so `capture_names` alone tells the codec which bracket part is
  which. Never map a capture spelling onto a tag, and never turn `parse_mtime`
  off: every line would then take the handle's modification time, one instant
  for a whole day of lines, with no error anywhere.
- `ULBRIDGE_ROWHEADER` is the default and the only one here: the core's
  `yggdryl.fix.ULBRIDGE_ROWHEADER`, which `rekep.times` re-exports
  unchanged, its clock captured as `mtime` and its level as `loglevel`. A
  bridge writing the same facts in a layout of its own is read by naming its
  header as the `rowheader` that `parse_log_messages` and
  `rekep.text.text_options` take, never by a second constant: the layout is a
  parameter and the capture names are the contract.
- The default clock takes the core's fraction: three digits under a point or
  a comma, the micros some of the bridge's loggers group after them as
  `.524_315`, or none at all. The `mtime` capture is consumed at nanoseconds
  UTC whatever the expression spells, so its width types no column; what the
  width decides is which lines the header matches. Another fraction -- six
  digits straight on -- is left unmatched, and a line a header misses is dated
  by its object's modification time with every capture null, and reaches the
  walk with no session, context or sequence to fold on -- so the count of lines
  a header matches moves the count of events a walk answers. A bridge spelling
  another fraction is read under a header of its own. `_` groups a fraction's
  digits in the core and never opens one: a header matching `01_147` hands the
  read a located refusal that fails the batch. `rekep.text.text_options`
  refuses a header that renames or omits a capture, because the read drops a
  capture it fills nothing from in silence -- a table that lands complete,
  keyed and empty down one column, or one whose clock settled nothing.
- The header's clock states no offset, so `text_options` and
  `parse_log_messages` read it in `timezone`, the zone the bridge prints in,
  `rekep.times.TIMEZONE` -- `Europe/Zurich` -- unless stated: the shipped
  capture's bridge prints a Central European clock, two hours ahead of its
  frames' UTC in summer, so read in its zone a line lands in the hour of the
  message it carries. A capture read in another zone dates each line hours
  away from its message: the whole walk still dates every copy by its
  transaction time and folds them, but an hourly silver window holds a
  copy's bronze row or the instant the walk dates it at, never both, leaves
  it out, and merges only the copies it holds. The suites read the capture
  at the default and pin its explicit `timezone="UTC"` reading beside it.
- Streams open one leaf at a time with bounded transport read-ahead and
  row-bounded batches. One record is unbounded until Yggdryl provides an
  error-on-overflow byte limit that preserves exact bodies.
- Never stage a remote file locally as the production path.
- Size parameters state their unit (`batch_row_size`, `read_byte_size`).

## Iceberg

- Primary APIs consume and return `RecordBatchReader`; table helpers explicitly
  require memory-sized data.
- Three verbs write; each creates a missing table and returns the rows it
  wrote. `append_*` appends the differences: the rows whose key their
  transformed partition does not hold, by default whenever the field declares
  a primary key (`merge_by=None`); `merge_by=False`, or no primary key, is a
  blind append. It never rewrites or deletes a stored file, and returns the
  rows added.
- `merge_*` upserts the differences, keyed by the required `merge_by` -- the
  primary key unless it names other columns: a row whose key is absent is
  inserted, one whose stored row holds other values replaces it, rewriting
  only the files holding such a row, and one stored as it is is left alone. A
  key stored twice is collapsed. Values are compared as a staged file would
  hold them: PyIceberg stores a null list of structs as an empty one, so the two are
  equal. It returns the rows inserted or replaced.
- `overwrite_*` replaces and takes no `merge_by`: `row_filter` replaces
  exactly the predicate's rows in one commit; without one, every row of the
  partitions the stream touches. An unpartitioned table needs `row_filter`.
- `merge_by=True` means the native Field's declared primary key, and a list
  names columns. A key is scoped to its partition: the same key on two days is
  two rows. A key that recurs within a stream keeps its first row in the
  table's sort order, across chunks too: a merge carries the keys it settled
  in the partition it is writing to the next chunk of that partition. A keyed
  append or merge of a replay writes nothing and commits nothing.
- Commit after `commit_row_size` rows or `commit_batch_num` input batches,
  whichever comes first. Either may be None, and with neither a stream is one
  commit. Under `row_filter` the bounds size staging chunks, not commits.
- Every write spills its stream to a local Arrow IPC folder under the
  dataset's `spill_directory` (None: the system temporary directory) -- one
  sorted run per bounded chunk and partition, one folder per partition --
  then merges each partition back, at most 16 runs open and a window of each
  sorted per step, in partition order, and re-cuts it into commits of
  `commit_row_size` rows, or of the largest spilled chunk when only
  `commit_batch_num` bounds. So each partition's files hold disjoint ranges
  in the table's sort order, memory holds one chunk and one merge step of at
  most half a chunk however many runs overlap, and a stream is consumed
  before its first commit. Dictionary columns spill as their values. A table
  with no partitions and no sort order streams through.
- Every commit streams one transformed partition at a time through
  PyIceberg's file-format writer on the table's `FileIO` and commits it by
  path, so a commit holds its chunk and not a multiple of it, and no data file
  touches local disk. Never hand a whole chunk to a writer that splits it.
- A keyed write never scans the table: one plan per chunk over the
  partitions it carries, each bounded by its own key range, reading key
  columns first and whole rows only of the files holding a carried key, and
  only to merge.
- Every write but a blind append declares what it read or takes out as a
  predicate -- a keyed write its partition and key ranges, an overwrite its
  partitions or `row_filter` -- and PyIceberg validates a retried commit
  against it: a concurrent commit elsewhere lands, and one under it is a
  conflict, `CommitFailedException`, for a fresh plan, under the table's
  `write.delete.isolation-level` -- `serializable` by default, where
  `snapshot` admits a concurrent same key. A keyed chunk that commits nothing
  still validates the head it read.
- Push filters, projections, ordering, and limits into storage planning.
- A null is the least value of every sort key: first ascending, last
  descending, the null order Iceberg records for each direction by default
  and the only one a shape holds. A NaN follows every number either way.
- Every verb accepts `branch`; every read accepts `snapshot_id`.
- Preserve supplied Iceberg ids and assign missing ids.
- Keep PyIceberg's configured `FileIO` and native PyArrow streams at the table
  boundary. Standard `s3.*` catalog properties own endpoints and credentials.
- `type: s3tables` is the one catalog type resolved here, because a table
  bucket is served by an Iceberg REST catalog AWS hosts at two endpoints and
  the `warehouse` is what picks one: a bucket ARN is the S3 Tables endpoint
  signed for `s3tables`, and `<account>:s3tablescatalog/<name>` is the Glue
  endpoint signed for `glue`, under Lake Formation. The warehouse is read as
  the `yggdryl.Uri` it is, never by a regular expression of rekep's: an ARN
  redirects through `Arn.locator()` to the `s3tables:` URL it names, and that
  locator is the second spelling of the S3 Tables door -- `s3tables://<name>`
  with `region`, `account` and, outside `aws`, `partition` in its query --
  and the one that says where the endpoint is, in its host or under
  `endpoint_override` and `scheme` exactly as an `s3:` URL does; the ARN the
  endpoint takes is spelled back from it. The ARN states its region; a
  locator states one or takes it from the configuration, as the Glue name
  does from `rest.signing-region` or the AWS environment, and nothing
  guesses. The environment is the third way to state the endpoint, from the
  AWS SDK's endpoint variables alone, never a profile's `endpoint_url`: the
  `uri` is the one stated outright, else the locator's, else
  `AWS_ENDPOINT_URL_S3TABLES` or `AWS_ENDPOINT_URL_GLUE` for the door the
  warehouse names -- with `/iceberg` added once, judged on the URL's path --
  else the partition's regional one. The generic `AWS_ENDPOINT_URL` never
  moves the `uri`, because one value cannot be the right host for both doors;
  the files are read through S3 alone, so `s3.endpoint`, where none is
  stated, takes `AWS_ENDPOINT_URL_S3` and then `AWS_ENDPOINT_URL`, and
  `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` in the environment turns all of
  them off. Every other property stays as written. The service owns the files
  under a table bucket, so no sweep deletes one there and a drop purges.
- Maintenance reports settled changes and never deletes a file whose ownership
  is ambiguous.

## Pipeline

Rekep is a processing library. Commands, schedules, catalogs and run windows
belong to its callers; the package holds only the table names its tasks
write. The supported graph is one function of `rekep.pipeline` per table --
a task -- each over one `Storages` and one window:

```text
capture URI                         -> parse_log_messages         -> bronze.record_keeping.log_messages
bronze.record_keeping.log_messages  -> parse_fix_messages_raw     -> bronze.record_keeping.fix_messages
bronze.record_keeping.fix_messages  -> parse_fix_messages_refined -> silver.record_keeping.fix_messages
silver.record_keeping.fix_messages  -> parse_books                -> silver.record_keeping.books
silver.record_keeping.books         -> parse_orders               -> silver.record_keeping.orders
                                    -> parse_quotes               -> silver.record_keeping.quotes
                                    -> parse_executions           -> silver.record_keeping.executions
```

A table is named `<layer>.<namespace>.<table>`. `rekep.storages.Storages`
holds one `IcebergCatalog` per layer -- `bronze`, `silver`, `gold` -- built by
`Storages.from_dict` from one `IcebergCatalog.from_dict` mapping each, and the
layer is the catalog that holds the table. Bronze holds what was read: the
lines and every FIX frame parsed out of them. Silver holds what a walk
settled: one row per FIX event, the books folded from them and the events
flattened out of the books. Gold is the consumers' layer and no task writes
it. Bronze and silver both hold `record_keeping.fix_messages`, so two layers
never share a catalog.

A task writes its `target`, and every task after `parse_log_messages` reads a
`source` table, both defaulted to the module's constants (`LOG_MESSAGES`,
`FIX_MESSAGES_RAW`, `FIX_MESSAGES`, `BOOKS`, `ORDERS`, `QUOTES`,
`EXECUTIONS`; `EVENTS` and `FLATTENERS` by kind). It opens and closes its
datasets through the `Storages` it is handed, never closes a catalog, creates
a missing target, and answers `Landed`. Each takes `commit_row_size`,
`COMMIT_ROW_SIZE` (128 Ki rows) unless stated -- at most that many rows per
commit of a keyed task, staging chunks of that size for a market task -- and
opens its target with `commit_batch_num=None`, so rows alone cut its commits.
`rekep.deploy.deploy(storages)` creates the tables ahead of a first run, for
catalogs the runner may not create tables in; `TABLES` is that layout.

Every task takes the window `[start, end)` over `currunix`, as
`rekep.times.window_of` answers it -- given neither bound, the last day up to
now -- and every scan hands it to Iceberg as a predicate on that column, so a
task plans only the hour partitions of its window. The three tasks keyed on
`curruuid` -- `parse_log_messages`, `parse_fix_messages_raw` and
`parse_fix_messages_refined` -- write with one `merge_arrow_reader`, upserting
the differences: a replay of a window writes nothing and commits no
snapshot, a row whose content changed under the same identity is replaced,
and a task whose write fails after a commit keeps it for its rerun to
complete. The market tasks write with one
`overwrite_arrow_reader(row_filter=...)`: a run over a window replaces what an
earlier run of it landed, so a replay commits one snapshot per market table.

`parse_log_messages(source, storages, window)` takes the capture first: it
binds a URI with `IOBase.from_uri` or reads the `IOBase` it is handed, frames
each line under `rekep.text.text_options(rowheader, timezone)`, hands the read
the window as its `where` -- the decode cuts every line and the record surface
answers the clause over the rows they become, so nothing is filtered after the
read -- and writes one schema-bearing reader directly to Iceberg under
`rekep.text.log_message_field()`, the read's own field narrowed for Iceberg:
the fifteen event columns the read settles over every line, `body`, and one
column per capture. Nothing there is hand-declared. `currunix` is the instant
the read settles over the line, read off the header's `mtime` capture in
`timezone`; a line the header could not date takes the modification time of
the object it was read from, and `EPOCH` only where a handle has none -- so a
header that matched nothing loses no line. An identity is derived from that
instant and the object the line was read from, so a capture is replayed from
where it was read, never from a copy. The table is keyed on `curruuid` alone,
the line identity the read states, which the row number reaches;
`currhashcode` is its content code over the object and the body, shared by
the lines of one body, and not a second key. A text row names its source
through the read's own `crosscode` and `seqnum`, its `creaunix` and `prevunix`
are the earliest instant the read dated a line of the object by and the one
it dated the line before by, and its `state` is `UNKNOWN`. Given no window, it
reads every line, observes the span of their `currunix` as they stream past
-- the write's spill is the only local stage -- lands them, and answers
`Landed.window` -- `rekep.times.hour_window` over the earliest and
latest `currunix` not pinned at `EPOCH`: `start` truncated to its hour, `end`
truncated and one hour later unless it already stands on a whole hour past
`start` -- the window the tasks after it run over.

Every `curruuid` and `currhashcode` is the installed native revision's value.
Rekep never reimplements identity derivation or translates old identities.
An identity contract change requires rebuilding affected tables from their
source under one native revision; mixing old and new keys leaves duplicate
logical events. Market window replacement removes superseded keys inside its
exact predicate, but it cannot repair earlier input tables or obsolete
schemas. A table whose shape changed incompatibly is dropped with every table
after it and replayed, never evolved in place.

The two FIX tasks are the two native stages one codec exposes, in this order
and no other:

```text
parse -> bronze.record_keeping.fix_messages, lifecycle -> silver.record_keeping.fix_messages
```

`parse_fix_messages_raw` reads the stored lines of the window off `currunix`,
with the epoch-pinned lines beside them, in the table's `SORT_COLUMNS` order
-- the order they were printed in, so the place the parse gives a message
among the messages of its instant, which reaches its identity, never depends
on file layout -- hands the parse `PARSE_COLUMNS`, the seven it consumes, and
parses them, and only that. The place counts the messages the window's lines
handed over at that instant, so it depends on the window's bounds: a bound
between the lines of one instant's messages restarts the part after it at
place zero, and a later run over other bounds lands those messages again under
identities of their own. Bronze FIX is rerun over the bounds it first ran, or
over bounds that split no instant's messages. A bronze row is what the message
implied about itself, `prevuuid` and `prevunix` are empty on every one because
nothing has walked yet, and `seqnum` is its place in its run -- the messages
the parse handed over at one instant, one after another -- null at place zero.
The parse dates a message by the transaction clock standing within
`official_time_delay_ms` of the `SendingTime` it states, else by that
`SendingTime`; a message stating none, by the transaction clock standing
within that delay of the line it was read off, else by that line; a frame read
off no line takes the codec's `default_sending_time`, which the tasks pin at
`UNDATED`. A message logged again at every hop is a copy placed apart at its
instant, so every copy is a bronze row under an identity of its own, and the
walk folds them -- except that an identity keeps only the millisecond of its
instant, so copies of one content dated apart within one millisecond, each
place zero of its own instant, share one and the key folds them.
`parse_fix_messages_refined` reads `[start - HISTORY, end)` of bronze --
`HISTORY` is one hour -- with `fix_window_filter`, which adds the `UNDATED`
rows whose `TransactTime` the window holds, in `SORT_COLUMNS` order, walks the
chains, and lands the rows it places in the window. The hour before is context
only, and an expiry past `end` waits for its own window. `snapshot_millis`,
`SNAPSHOT_MILLIS` (one hour) unless stated and zero for none, is the walk's
grid whatever the codec pins: at every whole hour each live chain is restated
as a view, `currunix == snapunix ==` the tick, under the identity that instant
derives -- a row of its own under the silver key -- with the live event's
content and place, moving no chain on. The rows at `start` are the chains the
hour before left alive. A silver row differs from the bronze rows it merges in
what the walk filled -- its place, its predecessor, the merged `srcuuids`, the
earliest `recdunix`, the folded `creaunix`, `exprunix` and `state` -- and in
the identity those re-settle to, so silver is written from bronze and never in
place. An event lands only in a refined window holding both its bronze row and
the instant the walk dates it at: a capture read in a zone other than its
bridge's dates its lines away from the messages they carry by the offset
between the two.

Both FIX tables use `rekep.fix.fix_message_field(codec)` -- the dictionary's
fixed row, 133 columns, 41 of them crate fields -- directly, without a rekep
FIX model. Parse, storage, reconstruction and lifecycle all use that one row.
`msgthreadid`, `loglevel` and `body` remain only in `log_messages`;
`msgsessionid`, `msgctxid`, `msgseqnum` and `msgpluginid` are FIX fields a
line fills, under one spelling; `srcuuids` joins a FIX row to the `curruuid`
of the lines its event was logged on -- on silver, the lines of the
observations of its session event, `msgsesseventid`, which the walk merges; a
copy another session event delivered at its instant with its content is folded
into it and named only by its bronze row -- and an execution split out of a
report to that report's `curruuid` beside them. PyIceberg reads a null list of
structs back as an empty one, so `rekep.fix` reads a stored group that came
back empty beside a counter stating nothing as absent before the walk or the
book fold reads the row, and one beside a stated zero as the empty group it
states. `exprunix` (65007) is the deadline a chain folds forward. `state`
(65029) is an `int32` code of the intrinsic `statecodeset`, which
`rekep.State` enumerates -- code = rank * 100 + place, `UNKNOWN` 0 -- set by
the first of tags 39, 150, 1036, 939, 297, 87, 665, 940, 1375 and 531 a
message states, else by what its message type asks for, else `UNKNOWN`. The
parse splits a report of a fill into the report and the execution it reports,
one per side for a trade report, each execution `FILLED`, and a quote stating
both sides into one quote per side. A trade side stating no `Side(54)` leaves
the FIX row's `side` null, and the market rows state it `UNKN` (0).
`crosscode` takes the first non-empty `OrderID`, `ClOrdID`, `OrigClOrdID`,
`QuoteID`, `QuoteReqID` or `MDReqID`; an execution split out of a fill report
takes its `ExecID`, else `TradeID=` and its `TradeID`, else the report's bare
code, `|Execution=` and its content code in hex; one split out of a trade
takes its side of the trade, `{len}:{own}|{tag}:{len}:{id}` -- `own` the
side's first `OrderID`, `ClOrdID` or `OrigClOrdID`, else the trade's code, and
`id` its first `SideExecID`, `SideTradeID`, `SideTradeReportID`, `OrderID` or
`ClOrdID`. An order's, a quote's or an execution's is prefixed with the
four-letter code of the side it states (`BUYS:`, `SELL:`), every other kind's,
and one stating no side, bare; `msgsesseventid` joins the message type, the
capture session, context and sequence. Default absence spellings are empty
text, `null`, `<null>`, `none`, `n/a` and `[n/a]`, trimmed and compared
case-insensitively. `fixentries` is residual and does not duplicate
successfully lifted scalars or complete groups; a reconstructed row promises
canonical message semantics, not arrival pair order or bytes.

The FIX registry is the process default: importing `rekep.fix` installs the
bundled dictionary under `rekep/_data/fix` with `FixRegistry.install_env`,
unless the environment variable `REGISTRY_VARIABLE` names
(`YGGDRYL_FIX_REGISTRY`) points at another folder, and
`FixRegistry.from_env()` and `FixCodec.from_env(**pins)` answer it. The
default is resolved once per process. `parse_fix_messages_raw`,
`parse_fix_messages_refined` and `parse_books` take the `codec` a caller
pinned, `FixCodec.from_env(default_sending_time=UNDATED)` when None, and it is
one codec for all three, because each task after the parse reads a row back
as the message the same dictionary wrote. The parse reads each frame on its
own and may use `threads`, and neither they nor the batch sizes change an
answer; its one piece of stream state is the place it gives each message
among the messages of its instant, in the order it is handed them, so the
input's order does. Every other cross-event state and order belongs to
lifecycle enrichment. Native construction validates every pin.

The refined and the books scans read the hour partitions in ascending
order, finishing one before opening the next, merge at most 16 overlapping
file streams at once and form no Python `read_all` union. The walk runs on
`codec.with_sorted_lifecycle(True)`, so native lifecycle takes that sorted
read as it comes and holds one epoch hour at a time, answering what a walk
collecting the whole read answers; the book fold streams.

`parse_books` reads silver events in `[start - HISTORY, end)`, ordered by
`SORT_COLUMNS`, clears every row's `symbol` with `categorized_symbol_reader`,
restores them through `fix_row_messages`, and delegates to native
`FixCodec.book_arrow_reader`, so every book is one `MIC:CFI` category per
instant: the fold owns that key, `{miccode}:{cficode}` over the detailed
classification a message's chain reaches, which no row cell holds.
Lifecycle is not repeated. Native code owns admission, operation kinds,
continuation, matching, expiration and book identity, and never fails on
what a message states: an entry it cannot place is left out with a warning
through `logging`. The fold takes the silver views of one instant as
the whole membership of their book there, adding no delta, so a book at
`start` holds every chain the walk that landed those views saw alive;
beyond them books start with no depth before `start - HISTORY` -- a warmed
window fold, not checkpoint reconstruction. `snapshot_millis`,
`SNAPSHOT_MILLIS` (one hour) unless stated, restates every book on that grid
without repeating a delta or an execution. Filter generated book times to the same strict window, including
expirations and grid books.

`parse_orders`, `parse_quotes` and `parse_executions` (`FLATTENERS`) run after
books commit, every one against the book snapshot `parse_books` answered in
`Landed.snapshot_id`, and project only the book columns their kind flattens
(`FLATTENED`: `deltas`, or `executions`); they are independent and may run
in parallel. Arrow kernels select deltas by `marketdatakind` (`ORDR`, `QUOT`,
the `MarketDataKind` members `EVENT_KINDS` names) or flatten the book's
execution list. Never flatten `alive` into event history, derive child
identities, split fills again, or perform a Python loop over rows. Preserve
each child's own facts.

`book_field()` derives from the native empty book reader's schema;
`market_event_field()` derives from its execution child. `iceberg_event_field`
narrows every shape recursively: timestamp ns to us, UUID to fixed bytes,
semantic extensions to storage, uint64 to signed bit views; and declares the
layout once: key `curruuid`, partition `hour(currunix)`, sort
`currunix, seqnum, curruuid`, where a null `seqnum` -- place zero -- sorts
first. The four fields -- the text read's row, the
fixed FIX row, the book, the market event -- make the seven tables.

The market tasks atomically replace exactly their strict window using bounded
staging and one Iceberg snapshot commit. An empty rerun removes old rows in
the window; a failure leaves the prior snapshot visible; rows outside it are
preserved. A partition-scoped keyed merge and whole-hour replacement do not
implement this contract for partial-hour windows.

Every task answers `Landed`: `read` source rows its window selected,
`written` target rows -- those a keyed task inserted or replaced, 0 on a
replay, or those a market task replaced its window with -- `skipped` answered
rows the target's key folded into another, a row an earlier run already
landed as it is or one the stream answered twice under one identity, and
`snapshot_id` the books snapshot
`parse_books` committed or a
flattener read. Records go to the `rekep.*` loggers -- INFO for each
completed operation, DEBUG for scans and files -- and the caller configures
`logging`; nothing here configures it.

## Schemas and documentation

- `schemas/<layer>/<namespace>/<table>.json` is each table's Iceberg contract
  as PyIceberg's own model JSON -- `schema` with `identifier-field-ids`,
  `partition-spec`, `sort-order` -- and `schemas/<layer>/schema.yml` the layer
  as dbt sources, version 2. `tools/schemas_dump.py` writes both, the column
  pages under `docs/tables/` and `docs/tables/states.md` from the fields the
  tasks declare; `python/tests/test_schemas.py` fails on drift.
- `docs/samples/` is a real landing of the capture: `tools/samples_dump.py`
  writes it, and `python/tests/test_docs.py` fails on drift under
  `-m integration`, where it also runs every documentation example that
  asserts.
- `docs/assets/fix-*.json` is `tools/fix_registry_dump.py`'s projection of the
  process registry, regenerated by hand when the registry changes.
- Documentation -- `docs/`, `README.md`, `schemas/`, the skill -- names
  capabilities through `rekep` and never names the native dependency, save
  the one exception [Ownership](#ownership) states.
- `docs/assets/market-server/` holds the market server's screenshots: the
  page it serves over the feed the market server page exports, clipped clear
  of the header's brand, which names the dependency in pixels no test reads.
  `docs/assets/README.md` says how they are taken; retake them when that
  page's view or the served page changes.

## Tests and benchmarks

- A test file mirrors the source it pins: `python/tests/test_<module>.py` for
  `rekep/<module>.py`, `python/tests/<package>/test_<module>.py` for a
  package's module. Test reusable internals there.
- Pin the application contract once as integration, in `python/tests/storages/`:
  the shipped capture through every task, over three local catalogs.
- `python/tests/test_docs.py` pins the documentation and
  `python/tests/test_schemas.py` the published contracts.
- Mark long Iceberg transactions `integration`; default CI excludes them.
- Cross zero/one rows, batch bounds, nulls, retries, and alternate Arrow types.
- Compare optimized code with a reference before timing it.
- Keep focused component benchmarks. Do not add million-row development or
  duplicate pipeline benchmarks.

## Layout

```text
python/src/rekep/
  fields/       native Field metadata helpers
  iceberg/      catalog, dataset, schema bridge, and PyIceberg FileIO
  storages.py   Storages: one Iceberg catalog per layer, tables named across them
  text.py       the bridge read: its options, its capture contract, its row
  pipeline.py   the tasks: one function per table the graph writes, and Landed
  deploy.py     the tables the graph writes, created ahead of a first run
  fix.py        the bundled registry and the two FIX stages over two tables
  market.py     the book fold and the event flattening over its snapshot
  times.py      instant readings, the run window, the ULBridge row header and its zone
python/tests/   unit tests mirroring the modules; storages/ for the integration contract
.claude/skills/rekep/SKILL.md  how an agent uses, tests and extends the library
data/capture/   the ULBridge capture every documented count reads
docs/           the mkdocs site; tables/ and samples/ are generated
schemas/        generated: <layer>/<namespace>/<table>.json and <layer>/schema.yml
tools/          schemas_dump.py, samples_dump.py and fix_registry_dump.py
```
