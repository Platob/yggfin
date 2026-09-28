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

Every order, execution, trade and venue identifier, account, party, comp id,
conversation id, person, counterparty and host in it is a pseudonym: one per
value, of its length and character classes, so every frame keeps its
`BodyLength(9)` and every chain joins as it did, and every `CheckSum(10)` is
the sum of the bytes published. Instruments, prices, quantities and clocks
are the market's own. Every task lands the rows the
original bytes landed, identities aside: party ids keep the order a message
sorts them in, and the pseudonyms were drawn so that the messages of one
instant, which the walk and the book fold order by identity, still arrive in
the order they did.

Its content SHA-256, as `sha256sum` prints it, is:

```text
a86eebbea2a32427965094f0d7b336becf4c3e3050c72c1ebf98a5819713f7e0
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
| messages the parse answers | 135 | 65 lines carry no frame and each of the other 79 carries one, and each of the 56 reports of a fill answers the execution it reports beside itself; a message is a row, not a line |
| `bronze.record_keeping.fix_messages` rows | 81 | the key folds the 54 messages that restate another hop's exactly |
| `silver.record_keeping.fix_messages` rows | 69 | the 81 bronze rows walked: the messages of one event merged and one expiry added, 27 events, and every chain still alive restated on each whole hour, 42 views |
| `silver.record_keeping.books` rows | 45 | the 69 silver rows folded into three `MIC:CFI` categories: 7 books an event moved, and every book restated on each whole hour from the first after it opens to 21:00, 38 views |
| `silver.record_keeping.orders`, `quotes`, `executions` rows | 11, 0, 7 | the books' order deltas, quote deltas and executions |

At 21:59:46 the capture holds a cancel request and its reject, which states
no `Side(54)`: the walk joins it to the one live side of its order, the
sell, so the fold books both on that side -- the two orders the day adds to
the morning's nine. The documented runs use
`window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")`, the morning's
order flow and the expiry of its open order at 16:25, which lands 128 lines,
74 bronze and 49 silver FIX rows, 29 books, 9 orders, 0 quotes and 7
executions:
[Data samples](../docs/samples/index.md) shows every table's rows.

`python/tests/storages/` runs the capture through every task under
`-m integration`, and `python/tests/test_docs.py` pins the digest above.
