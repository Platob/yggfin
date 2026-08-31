# Notebook workflow

Pipeline implementations live in `tasks/<name>/<name>.ipynb`. Each adjacent
YAML file points to its notebook and supplies parameters:

```yaml
name: parse_fix
notebook: parse_fix.ipynb
parameters:
  source: logs.messages
```

Run the same task document locally that Airflow gives Papermill:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --output parse_fix.executed.ipynb
```

`Task` only resolves the configuration. Papermill owns execution; the package
contains no prebuilt pipeline jobs or task reports.

## Flow

```text
Text files -> parse_messages -> logs.messages -> parse_fix -+-> fix.misc
                                                   |        `-> fix.unknown
                                                   `-> fix.market -+-> parse_instruments -> market.instruments
                                                                   `-> parse_market -+-> Book -+-> Order
                                                                                     |         `-> Execution
                                                                                     `---------> Order + Execution (books: false)
```

`parse_messages` tokenizes once and assigns `MsgType` and `eventtype`;
`parse_fix` pushes the market-event selection to Iceberg and owns protocol and
dictionary resolution from there. Keeping `logs.messages` is what lets a field
or protocol change rerun FIX resolution without reopening the source logs --
only a MsgType event-metadata change rebuilds it, because that changes its
stored `eventtype`.

`parse_instruments` reads the rows `parse_fix` wrote to `fix.market` and
versions `market.instruments` from their nested `Instrument` components. One
current `InstrumentUpdate` is keyed by its sixteen-byte `xhash`, with
`instrument.symbolticker` as its readable identity. It is a second reader of
that table rather than a second writer of it: the FIX stage owns translation
and the clock, while the model owns ticker derivation and reference-data
versioning.

`books: false` skips the fold and writes only the Order and Execution events
each FIX message carries -- and so creates no snapshots, no synthetic
expiries, no book validation changes and no carrying `Book.hash` parent:

```yaml
# tasks/parse_market/parse_market.yml
parameters:
  books: false
```

The six [products](../products/index.md) are what the pipeline persists.
Arrow readers carry each stream; Iceberg stores the boundaries.

- [Deploy from scratch](operations/deploy.md)
- [Parse messages](tasks/parse-messages.md)
- [Parse FIX](tasks/parse-fix.md)
- [End-to-end run](operations/run.md)
- [Deploy and operate with Airflow](operations/airflow.md)
- [Parse market](tasks/parse-market.md)
- [Flatten orders](tasks/flatten-orders.md)
- [Flatten executions](tasks/flatten-executions.md)
