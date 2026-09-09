# Local data

This directory is what the checked-in task documents point at, so a clone runs
the pipeline without being configured first.

| path | tracked | what it is |
| --- | :---: | --- |
| `capture/app_messages_sample.txt` | yes | a 14-line ULBridge sample; `filesystem: file:data/capture` resolves to the directory holding it |
| `catalog.db` | no | the default SQLite catalog, `sqlite:///data/catalog.db` |
| `warehouse/` | no | the default Iceberg warehouse, `data/warehouse` |

Both defaults are relative, so they resolve against the working directory: run
the tasks from the repository root, or override `catalog` to name absolute
locations. Airflow's operator starts its child with the checkout as the working
directory, so the same relative defaults land here.

`parse_messages` reads captures through Yggdryl and requires no bundled
protocol registry; the FIX dictionary `parse_fix` types against ships inside
the package.

The larger 111-line fixture the test suite and every documented count use is
`python/tests/data/ulbridge.log`, not this sample.
