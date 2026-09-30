# XMLA endpoint

The three local catalogs a run lands are folders of Iceberg tables, and an XML
for Analysis endpoint serves those folders as they are: Excel, Power Query
and any XMLA client then query the landed tables with no copy and no second
catalog. Land the catalogs first -- the example of [Start here](../index.md),
into `root`:

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
    assert parse_fix_messages_refined(storages, window) == Landed(read=126, written=51)
    books = parse_books(storages, window)
    events = {
        kind: task(storages, window, snapshot_id=books.snapshot_id).written
        for kind, task in FLATTENERS.items()
    }
    assert (books.written, events) == (30, {"orders": 9, "quotes": 0, "executions": 9})

# Each layer's warehouse is a folder of Iceberg tables.
assert sorted(path.name for path in (root / "silver" / "record_keeping").iterdir()) == [
    "books",
    "executions",
    "fix_messages",
    "orders",
    "quotes",
]
```

Then serve the three warehouses, one catalog each, under the names the
layers have:

```bash
# Excel, Power Query and .odc connections: https://platob.github.io/yggdryl/media/#excel
yggdryl xmla serve bronze=<root>/bronze silver=<root>/silver gold=<root>/gold --bind 127.0.0.1:8080
```

The first line the server prints is its endpoint, `http://127.0.0.1:8080/xmla`;
`--bind 127.0.0.1:0` takes a free port and prints the one it took.

## What a client sees

| XMLA | rekep |
| --- | --- |
| catalog, and its one cube | a layer: `bronze`, `silver`, `gold` |
| schema | the namespace, `record_keeping` |
| table | a table folder, `books`, `fix_messages`, ... |
| statement | `select ... from silver.record_keeping.books`, the three-part name a task writes |

`MDSCHEMA_CUBES` answers one row per catalog -- three cubes, the gold one
empty until a consumer lands a table in it -- and `DBSCHEMA_TABLES` the seven
tables under `record_keeping`. A statement is a query over one table, not MDX
or DAX; the endpoint's own documentation, linked in the command above, says
which Excel doors take one -- Power Query with a query, an `.odc` with a
command text -- and which do not.

```bash
curl -s http://127.0.0.1:8080/xmla -H "Content-Type: text/xml; charset=utf-8" --data-binary '
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
<Command><Statement>select currunix, crosscode, state, bidpx from silver.record_keeping.books limit 5</Statement></Command>
<Properties><PropertyList><Format>Tabular</Format></PropertyList></Properties>
</Execute></soap:Body></soap:Envelope>'
```

A rowset has no element for a map, so a statement over a market or FIX table
names its columns: `select *` there is refused at `securityids`, `metadata`
and the other map columns.

## Read-only

Serve without `--writable`. PyIceberg's catalog owns every commit through
the SQLite pointer beside each warehouse; a statement writing through the
endpoint would commit a metadata file that pointer never names, and forks the
table. The tasks remain the one writer.

Two consequences of reading folders rather than the catalog:

- A table's current state is its highest-numbered metadata file, which is the
  one the catalog last committed while the tasks are its only writer.
- A table dropped from the catalog without purging its files still shows,
  because its folder is still there: drop with a purge, or remove the folder.
