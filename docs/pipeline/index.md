# Marimo task workflow

Pipeline implementations live in `tasks/<name>/<name>.py`, one Marimo
application each. The adjacent YAML file names that application and supplies
its parameters. One FIX task document is reused for every routed category:

```yaml
name: parse_fix
application: parse_fix.py
parameters:
  source: logs.messages
  category: market
```

Run the same task document locally that Airflow runs on a schedule:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --parameter category=market
```

Open the same application interactively:

```bash
uv run --project python --group runner marimo edit tasks/parse_fix/parse_fix.py
```

`Task` only resolves the configuration: it reads the document and resolves the
application beside it. The application's `parameters` cell defines exactly the
keys the YAML declares and reads its defaults out of that document, so an
interactive session and a scheduled run are configured by the same file. The
package contains no prebuilt pipeline jobs or task reports.

## Flow

```text
Text objects -> parse_messages -> logs.messages -+-> parse_fix_market -> fix.market -+-> parse_instruments -> market.instruments
                                               |                                  `-> parse_market -+-> Book -+-> Order
                                               |                                                    |         `-> Execution
                                               |                                                    `---------> Order + Execution (books: false)
                                               +-> parse_fix_misc -> fix.misc
                                               `-> parse_fix_unknown -> fix.unknown
```

`parse_messages` uses yggdryl to retain one seven-column raw `Message` per
physical line. Airflow runs the one `parse_fix` definition three times. Each
run parses the raw body, applies one mutually exclusive event-category mask,
and owns one `fix.*` table. Keeping `logs.messages` lets field, protocol,
timezone, plugin-alias, null-value, and MsgType metadata changes rerun FIX
resolution without reopening the source objects. Only source selection or
header-capture changes rebuild the raw table.

`parse_instruments` reads the rows `parse_fix_market` wrote to `fix.market` and
versions `market.instruments` from their nested `Instrument` components. One
current `InstUpdate` is keyed by its sixteen-byte `xhash`, with
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
