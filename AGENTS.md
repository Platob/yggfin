# Coding patterns

House style for `rekep`. Keep one obvious implementation for each behavior.

## Writing for the next agent

This file, every docstring and every description is read far more often by an
agent scanning for one fact than by a person reading a chapter. Write for that
reader.

- Ship the simple implementation. One pass, one obvious data structure, no
  layer that exists in case something later needs it. A shorter diff is a
  cheaper diff to scan.
- Docstrings are synthetic: one line saying what the thing *is*, and a second
  paragraph only for a constraint or a measurement the code cannot show. Never
  restate the signature, never narrate the steps.
- State facts, not history or hedging. "One tag is one identity" beats "we
  decided that it is probably best if each tag maps to a single identity".
- Put the answer where it will be looked for: on the class that owns the data,
  next to the constant it constrains, in the one guide that owns the topic.
  Two half-answers in two files cost more than one whole answer in one.
- Name things so a search finds them. A grep for a column name must reach its
  declaration, its parser and its test.
- Delete rather than deprecate. There are no compatibility shims here, so a
  renamed thing has exactly one name.

## Documentation

Documentation is example-first. A reader should reach the working shape before
the explanation of it.

- Prefer executable Python, command lines, YAML and Arrow schemas to prose.
  Never replace a concrete example with several paragraphs describing it.
- Keep prose connective: one explicit fact per sentence and the fewest
  sentences that make the example safe to use. Delete introductions,
  repetition and implementation narration.
- Put commands in fenced blocks that can be copied unchanged. Put configuration
  beside the command that consumes it and show the resulting schema or table
  when that is the contract.
- Keep each fact in the one page that owns it; link there instead of repeating
  a shortened explanation elsewhere.
- The site is dark-first and uses black, white, red, orange and yellow. New
  diagrams and page assets use that palette except for third-party brand marks.
- Top-level guide families are horizontal tabs. Pages within the selected
  family remain a categorized vertical navigation; do not flatten the site
  into one long menu.

## Command line

`rekep` is read at a terminal, so it is styled like one written this decade --
and every bit of that degrades on its own.

- `rekep.console.Console` owns colour, box drawing, spinners and tables.
  Nothing else writes an escape sequence.
- `Console` renders for the person who just typed a command; `rekep.logs`
  records what the library did for whoever reads the run afterwards. One fact
  belongs to one of them. INFO is a completed operation -- one record per
  public verb, whatever it commits inside; DEBUG is the detail under it, per
  stream and per file, never per batch. Modules hold
  `logging.getLogger(__name__)` and nothing configures logging at import.
- The shell uses the documentation palette: white for primary values, yellow
  for success and selected values, orange for interaction and warnings, red
  for failures, and grey only for secondary context. Do not add another hue.
- Colour is off without a TTY, under `NO_COLOR`, and on `TERM=dumb`. Box
  drawing falls back to ASCII where the stream cannot encode it.
- Styling goes to `stderr`; the payload -- a dumped document, a report -- goes
  to `stdout` alone, so a redirect gets data and never decoration.
- A step that can take a second animates. A step that writes gets a `✓` or a
  `✗`, never a bare return.
- Interactive verbs ask one question at a time, offer the stored value as the
  default, show the whole change back, and write only after a yes.
- Every interactive path takes its answers through an injected reader, so it is
  testable without a terminal.

## API

- Put behavior on the class that owns the data. Keep justified helpers private.
- `from_*` builds an instance; `into_*` converts or emits it.
- Generic `from_`/`into_` dispatch through cached `into_redirects()` mappings.
- Prefer cached class methods over class variables for derived declarations.
- Dataclass `__post_init__` normalizes state once.
- Expensive handles are `cached_property`; teardown pops `__dict__` and never
  opens a lazy resource.

## Declarations

`@scalar` makes a dataclass an Arrow `StructField`, returned by
`Class.into_field()`.

- Type hints declare nullability, including list items.
- `__`-prefixed annotations are private state, not columns.
- Use `Annotated[..., Field(...)]` for Arrow types, metadata, keys, partitions,
  and sort order.
- Container behavior belongs to its `Field` subclass. Do not branch on Arrow
  kinds at call sites.
- Refuse recursive classes, ambiguous unions, unknown leaves, and missing
  non-null columns.
- Protocol metadata is prefixed (`fix:`, `iceberg:`, `enum:`).
- Nested fields never publish table keys.
- Portable contracts live in `schemas/<namespace>/<name>.yaml` and round-trip
  losslessly through Arrow.

### Column names

Every column name is folded: `column_name` lowercases it and drops everything
that is not a letter or a digit. `OrigClOrdID` is `origclordid`, `source_url`
is `sourceurl`. One name serves the Arrow column, the Python attribute and the
stored document, and there is no snake-case alias beside it.

The fold is also the match: a spelling resolves against the FIX registry by
what it folds to, so `MsgType`, `msgtype` and `MSGTYPE` are one field.

Every column carries `fix:display`, the name a reader is shown -- the
dictionary's spelling for a FIX column, `display_name`'s title case for
everything else. `tests/test_schemas.py` holds both halves for every published
contract.

A column that reads a FIX field is *named after that field*: `ClOrdID <11>` is
`clordid`, `CumQty <14>` is `cumqty`, `MinPriceIncrement <969>` is
`minpriceincrement`. Two exceptions, both deliberate:

- `px` and `qty` are the abstract slot every market row shares. One attribute,
  and which FIX field it holds is the subclass's to declare -- `Price <44>` on
  an order, `LastPx <31>` on a report, `MDEntryPx <270>` on a level.
- A nested struct takes the *generic* spelling, because the nesting already
  says whose it is: a leg's `LegCFICode <608>` is `cficode`, the same name its
  instrument's `CFICode <461>` has, not `legcficode`.

### Description budget

Descriptions are contract text, not tutorials. Use one short factual sentence
that adds information such as units, source, null meaning, or derivation.
Delete text that repeats the name, type, signature, or implementation. Put a
non-obvious constraint in one nearby `#` comment and longer rationale in the
single guide that owns it. Optimize descriptions whenever touching a field.

## Arrow is the hub

- Arrow schema metadata is the authority; other representations derive from it.
- Cast at producer and consumer boundaries. Recursive casts reorder columns,
  fill only nullable omissions, drop extras unless `merge_schema=True`, and
  handle nested shapes with Arrow kernels.
- Never use a Python row loop for an Arrow shape conversion. Column
  comprehensions are fine.
- Let Arrow own codecs, filesystems, decompression, and kernels. Let pyiceberg
  own table conversion, ids, snapshots, planning, and commits.
- Dictionary-encode repeated code columns; store enum values as integers so
  unknown future codes survive.

## Streaming datasets

`Dataset` is a stream in and a stream out.

- Primary APIs use `RecordBatchReader`; table helpers explicitly require data
  to fit in memory.
- `overwrite_*` replaces the rows whose keys match and inserts the rest;
  `append_*` inserts, skipping stored keys when `merge_by` names them. Both
  create a missing table. `merge_by=True` means the declared primary key, and
  an overwrite has no keyless mode.
- Accumulate `commit_row_size`; an input batch is not a storage commit.
- Push filters, projections, limits, and ordering to the storage engine.
- File sets open one naturally sorted path at a time and combine short batches.
- Nothing names a source except caller-supplied `static_values`.
- Size parameters state their unit (`batch_row_size`, `read_byte_size`).

## Iceberg

- Every verb accepts `branch`; every read accepts `snapshot_id`.
- Preserve field ids when present and assign them when absent.
- Read planned partitions in deterministic order and stream one partition at a
  time when ordered input is requested.
- Merge scan filters are safe supersets; deletion matching remains exact.
- Compare filesystem paths only after resolving both through the same
  filesystem.
- Maintenance must settle and report what it changed.
- Tests assert planned files, snapshots, and stored results, not only returned
  rows.

## Market data

- Events are immutable versions. `vhash` identifies a clock-free value,
  `hash` anchors it to one event time, `xhash` identifies a lifecycle,
  `prevhash` its previous version, and `linkedhashes` relates lifecycles by
  `xhash`.
- Composite identity is the cross-language `rekep-identity-v1` frame: signed
  little-endian `int64` lengths, `-1` for null, typed payload bytes and XXH3-64.
  `vhash` and lifecycle identities are signed `int64`; `hash` composes epoch
  microseconds over `vhash` and is stored as sixteen big-endian bytes. Numbers
  are never formatted as text.
- Store market notions as ASCII codes packed into one integer, left-justified
  and padded with trailing NULs, so the value orders as its text does. Ranks
  carry the band order, so live and terminal checks compare ranks and a storage
  scan pushes the finite code set `ranked_at_least` names.
- Nest nothing a reader filters on. Keep instrument identity and book summary
  values flat.
- `Instrument` is flat reference data keyed by canonical `symbolticker`, with
  `xhash = hash_of(symbolticker)`. It has no versions or snapshots.
- FIX transcription preserves repeated tags and wire order in lists, not maps.
- The registry owns FIX names, types, descriptions, tags, and values across
  versions. Hard-code only normalization rules the registry cannot express.
- Generic `Event` owns snapshot and idle-expiry behavior. Finished states do
  not keep producing snapshots.
- `BookIterator` consumes time-sorted parsed `FixMsg` records and emits only
  `Book` rows. Keep state mutation single-threaded and bounded. `purge_alive`
  decides whether orders still resting when the stream ends are expired.
- A structured FIX component is a `ComponentGroup` subclass naming its
  component, its group and the members that earn a column; everything else in
  an entry lands in `buffer`. The spec's own `required` rules decide member
  nullability -- `FixRegistry.component_field` reads them.

## Workflow ownership

Concrete jobs are notebooks under `tasks/<name>/`, beside a YAML document that
points to the notebook. Package code contains reusable parsing and model logic,
not project-specific job classes. `Task` only defines/serializes notebook
configuration. Airflow executes the notebooks through its Papermill provider.

The supported graph is:

```text
parse_messages -> parse_fix -> parse_market -> flatten_orders
                      |             `-------> flatten_executions
                      `------------> market.instruments
```

`parse_fix` writes flat Instrument records directly to `market.instruments`.

With `parse_market.books: false`, the market task bypasses Book construction
and writes the FIX-carried Order and Execution rows itself; the two flatten
tasks are skipped by the Airflow result route.

Inputs are text files. Persisted outputs are `logs.messages`, the three
`fix.*` tables, and the `market.*` instrument, book, order and execution
tables.

## Tests and benchmarks

- Test reusable internal logic; do not mirror notebooks with packaged task
  tests.
- Mark long Iceberg transactions `integration`. Default CI excludes them; the
  integration workflow opts in explicitly.
- Derive expectations from fixtures, then pin counts so broken producers cannot
  move both sides of an assertion.
- Cross every control-flow boundary: zero/one rows, batch sizes, thresholds,
  nulls, and alternate types.
- Compare optimized paths with a reference implementation before timing them.
- Keep focused internal benchmarks. Do not add full-pipeline or million-row
  development benchmarks.

## Layout

```text
python/src/rekep/
  fields/       declarations and recursive Arrow casts
  fix/          messages, registry, components, protocol rules, and the shell
  enums/        every persisted market code, over two ASCII bases
  market/       event, instrument, order, execution, and book logic
  iceberg/      catalog, dataset, and the schema bridge
  text/         FixMsg plus streamed text files
  tasks/        notebook configuration only
  arrow_file_io.py  the Iceberg FileIO: locations, spills, content cache
  console.py    terminal styling: colour, boxes, tables, spinners
  logs.py       the level policy, and where a run's records go
  times.py      one reading of "an instant", whatever spelled it
schemas/rekep/  the six persisted output contracts
data/fix/       the FIX dictionary: tag-range shards, components, messages
tasks/          notebooks, adjacent YAML, and Airflow DAG
```

Comments explain why a constraint exists. Docstrings and descriptions never
restate what code already says.
