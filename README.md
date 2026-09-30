# rekep

`rekep` lands ULBridge FIX bridge captures as Iceberg tables, in three
layers: **bronze** holds what was read -- the captured lines and every FIX
message parsed out of them -- **silver** what a walk settled -- one row per
FIX event, the order books folded from them, and the order, quote and
execution events flattened out of the books -- and **gold** is the consumers'
own. Each table is written by one function of `rekep.pipeline`, a task, over
one window of time; where, when and over which window a task runs is its
caller's.

```text
capture                              -> parse_log_messages         -> bronze.record_keeping.log_messages
bronze.record_keeping.log_messages   -> parse_fix_messages_raw     -> bronze.record_keeping.fix_messages
bronze.record_keeping.fix_messages   -> parse_fix_messages_refined -> silver.record_keeping.fix_messages
silver.record_keeping.fix_messages   -> parse_books                -> silver.record_keeping.books
silver.record_keeping.books          -> parse_orders               -> silver.record_keeping.orders
                                     -> parse_quotes               -> silver.record_keeping.quotes
                                     -> parse_executions           -> silver.record_keeping.executions
```

```bash
pip install "rekep[iceberg] @ git+https://github.com/Platob/yggfin#subdirectory=python"
```

From a checkout, `pip install "./python[iceberg]"`, with `glue` or
`s3tables` added for those catalogs. Land the [shipped capture](data/README.md)
from the repository root into three local catalogs:

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    FLATTENERS,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
window = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
with storages:
    capture = "file:data/capture/ulbridge.log"
    assert parse_log_messages(capture, storages, window) == Landed(read=129, written=129)
    assert parse_fix_messages_raw(storages, window) == Landed(read=129, written=126)
    assert parse_fix_messages_refined(storages, window) == Landed(read=126, written=49)
    books = parse_books(storages, window)
    events = {
        kind: task(storages, window, snapshot_id=books.snapshot_id).written
        for kind, task in FLATTENERS.items()
    }
    assert (books.written, events) == (30, {"orders": 8, "quotes": 0, "executions": 8})
```

A rerun of a task over its window leaves the table it writes holding each row
once: the three keyed tasks merge on `curruuid`, writing only the rows their
table lacks or holds with other values, and the book and event tasks replace
their window. A line's clock states no offset: `parse_log_messages` reads it
in `timezone`, `Europe/Zurich` unless stated -- the Central European clock
the shipped capture's bridge prints -- so each line is dated in the hour of
the message it carries. The FIX dictionary ships inside the package and is
the process default: `rekep.FixRegistry.from_env()` answers it and
`rekep.FixCodec.from_env()` parses under it.

| read | for |
| --- | --- |
| [documentation](https://platob.github.io/yggfin/) | storages, tasks, DAGs, tables, the FIX registry and sample rows |
| [`schemas/`](schemas/README.md) | every table's Iceberg contract and each layer's dbt sources |
| [`data/`](data/README.md) | the capture every documented count reads |
| [`config/`](config/README.md) | where an operator's own FIX dictionary goes |
| [`AGENTS.md`](AGENTS.md) | how the code is written and who owns what |

Development, from `python/`:

```bash
uv run pytest
uv run pytest -m integration
uv run ruff check . ../tools && uv run ruff format --check . ../tools
uv run --group docs mkdocs build --strict --config-file ../mkdocs.yml
```
