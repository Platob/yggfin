# Local data

This directory holds the capture every documented count reads.

| path | tracked | what it is |
| --- | :---: | --- |
| `capture/ulbridge.log` | yes | the [144-line ULBridge capture](#the-capture); `file:data/capture` resolves to the directory holding it |

Every location here is relative, so it resolves against the working
directory: land from the repository root. The examples land into three
SQLite catalogs under a scratch folder, one per layer, so a run leaves
nothing here.

## The capture

`capture/ulbridge.log` is a byte-exact, anonymized 144-line ULBridge capture:
ordinary prose, numeric FIX, ULLINK, FIXML, bridge configuration rows, printed
separators and control-byte separators, with several messages logged again at
every hop they passed. The examples, the test suite and every documented
count read these bytes, and the digest below is what pins them.

Its content SHA-256, as `sha256sum` prints it, is:

```text
2825a01694662924696abd0ebff116d64971ac3a8a5ce35dbfcddb1abb1c9e4b
```

Every line sits under the bridge's bracket and is dated 2026-08-14 by the
bridge's clock: 129 spell the fraction as three digits after a point, `.769`,
and 15 group the micros after them, `.524_315`. The bridge prints its local
time, two hours ahead of the UTC its FIX frames state: the 112 lines printed
at 14:46 carry the messages of 12:46.

| hour the lines were printed at | lines |
| --- | ---: |
| 03 | 16 |
| 14 | 112 |
| 16 | 1 |
| 23 | 15 |

### What it answers

Over the capture's whole day, `window_of("2026-08-14", "2026-08-14")`:

| reading | count | why |
| --- | ---: | --- |
| lines | 144 | one `bronze.record_keeping.log_messages` row each: the content code digests the line's row number, so the exact repeats answer identities of their own |
| lines the row header does not date | 0 | the shipped header reads every fraction this bridge writes |
| messages the parse answers | 79 | 65 lines carry no frame and each of the other 79 carries one; a message is a row, not a line |
| `bronze.record_keeping.fix_messages` rows | 48 | the key folds the 31 messages that restate another hop's exactly |
| `silver.record_keeping.fix_messages` rows | 19 | the 48 bronze rows walked: the messages of one event merged, and one expiry added |

The book fold refuses two of the day's messages, as it must: a trade report
at 14:52:55 whose side states no `Side(54)`, printed at 16:52, and a cancel
reject at 21:59:46 that names no symbol. So the day is no book window, and
the documented runs use `window_of("2026-08-14T00:00:00Z",
"2026-08-14T16:30:00Z")`, which lands 128 lines, 41 bronze and 14 silver FIX
rows, 6 books, 1 order, 0 quotes and 7 executions:
[Data samples](../docs/samples/index.md) shows every table's rows.

`python/tests/storages/` runs the capture through every task under
`-m integration`, and `python/tests/test_docs.py` pins the digest above.
