# Local data

This directory holds the capture every documented count reads, and it is
where the local catalog the [README](../README.md) quick start names lives, so
a clone lands without being configured first.

| path | tracked | what it is |
| --- | :---: | --- |
| `capture/ulbridge.log` | yes | the [144-line ULBridge capture](#the-capture); `file:data/capture` resolves to the directory holding it |
| `catalog.db` | no | the local SQLite catalog, `sqlite:///data/catalog.db` |
| `warehouse/` | no | its Iceberg warehouse, `data/warehouse` |

Every location here is relative, so it resolves against the working
directory: land from the repository root, or name absolute locations in the
mapping `IcebergCatalog.from_dict` reads. Delete `catalog.db` and `warehouse/` to start over.

`parse_messages` reads captures through the native text reader and requires no
bundled protocol registry; the FIX dictionary the FIX stages type against
ships inside the package.

## The capture

`capture/ulbridge.log` is the byte-exact, anonymized 144-line ULBridge capture
of the native core -- `rust/tests/fix/ulbridge.log` in Yggdryl 0.1.11, the
release `python/pyproject.toml` pins -- so the examples, the test suite and
every documented count read the same bytes the core's own acceptance example
does. The digest below is what pins them. It holds ordinary prose,
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
| `market.books` rows over 12:00 to 13:00 UTC | 5 | the hour's 12 `fix.refined` rows folded from no depth; a window holding the AE report whose side states no `Side(54)`, or the cancel reject that names no symbol, is refused, so the whole day is not a book window |
| `market.orders`, `market.quotes`, `market.executions` rows of that hour | 1, 0, 7 | the book deltas and the execution list the five books carry, read off the one snapshot `parse_books` committed |

`python/tests/test_fix.py` holds every chain's walked counts;
`python/tests/test_workflow.py` runs the capture through `parse_messages`,
`parse_fix_raw` and `parse_fix_refined`. `python/tests/test_docs.py` pins the digest above.
In a core checkout at that release, `cargo run --example fix_capture` prints
the same 79 frame arrivals as `parsed 79 messages`.

The [Parse FIX refined](../docs/pipeline/parse-fix-refined.md) page shows the
chain the walk names `00026877711XOEA0`, including the partial fill and the
fill that closed the order.
