# Local data

This directory is what the shipped task defaults point at, so a clone runs
the pipeline without being configured first; `rekep tasks <name> show` prints
them.

| path | tracked | what it is |
| --- | :---: | --- |
| `capture/ulbridge.log` | yes | the [144-line ULBridge capture](#the-capture); `filesystem: file:data/capture` resolves to the directory holding it |
| `catalog.db` | no | the default SQLite catalog, `sqlite:///data/catalog.db` |
| `warehouse/` | no | the default Iceberg warehouse, `data/warehouse` |
| `dbt/` | yes | the [dbt project](dbt/README.md) `build_dbt` runs: its models, schemas, macros and profile |
| `dbt/target/`, `dbt/logs/` | no | what a dbt build leaves: artifacts, staged Parquet, and dbt's own log |

Both defaults are relative, so they resolve against the working directory: run
the tasks from the repository root, or override `catalog` to name absolute
locations. Airflow's operator starts its child with the checkout as the working
directory, so the same relative defaults land here.

`parse_messages` reads captures through the native text reader and requires no
bundled protocol registry; the FIX dictionary the two FIX tasks type against
ships inside the package.

`dbt/` is the exception to the rule above: it is not local data but the project
that derives the order and execution products from the stored FIX rows. It
lives here because its own defaults are these defaults -- it reads the catalog
this directory holds and commits into the warehouse beside it -- and because
DuckDB, the engine dbt runs its SQL in, keeps nothing of its own between runs.

## The capture

`capture/ulbridge.log` is the byte-exact, anonymized 144-line ULBridge capture
of the native core -- `rust/tests/fix/ulbridge.log` in Yggdryl 0.1.11, the
release `python/pyproject.toml` pins -- so the shipped default, the test suite
and every documented count read the same bytes the core's own acceptance
example does. The digest below is what pins them. It holds ordinary prose,
numeric FIX, ULLINK, FIXML, bridge configuration rows, printed separators, and
control-byte separators, and it logs several messages again at every hop they
passed.

Its content SHA-256, as `sha256sum` prints it, is:

```text
4cc928ade1c8975701e4ab3908d5aa2fb53f8c919eb3ffc8faef10102cb6a94e
```

Every line sits under the bridge's bracket and is dated 2026-08-14: 129 spell
the fraction as three digits after a point, `.769`, and 15 group the micros
after them, `.524_315`.

### What it answers

| reading | count | why |
| --- | ---: | --- |
| physical lines | 144 | one row of the text read each |
| stored lines | 144 | `logs.messages` is keyed on [`curruuid`](../docs/products/message.md), whose content code digests the line's row number, so the 3 exact repeats answer 3 identities and no line is lost |
| lines the row header does not date | 0 | the shipped header reads every fraction this bridge writes, the fifteen grouped micros included |
| messages the codec answers | 79 | 65 lines carry no frame and each of the other 79 carries one; a message is a row, not a line |
| duplicate frame arrivals | 30 | the same message is logged again at several hops |
| `fix.raw` rows | 49 | one row per distinct parsed event, keyed on `curruuid` |
| `fix.refined` rows | 19 | the 49 `fix.raw` rows walked, those of one event merged into one row; every line dated means every observation carries the session, context and sequence the fold merges on |

`python/tests/test_fix.py` holds every chain's walked counts;
`python/tests/test_workflow.py` runs the capture through `parse_messages`,
`parse_fix_raw` and `parse_fix_refined`, `python/tests/test_dbt.py` on through
`build_dbt`, and `python/tests/test_eks_rekep_operator.py` through those four
in the task image a pod runs. `python/tests/test_docs.py` pins the digest
above.
In a core checkout at that release, `cargo run --example fix_capture` prints
the same 79 frame arrivals as `parsed 79 messages`.

The pipeline task pages under `docs/pipeline/tasks/` show the chain the walk
names `00026877711XOEA0`, including the partial fill and the fill that closed
the order, as each of the four tasks lands it. `tools/pipeline_samples.py`
renders those tables from a run over this file into
`docs/pipeline/tasks/samples/`, and `--check` renders them again
from a throwaway catalog and fails on any difference.
