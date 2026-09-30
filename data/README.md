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
instant, which the walk and the book fold order by their place there and then
by identity, still arrive in the order they did.

Its content SHA-256, as `sha256sum` prints it, is:

```text
a86eebbea2a32427965094f0d7b336becf4c3e3050c72c1ebf98a5819713f7e0
```

Every line sits under the bridge's bracket and is dated 2026-08-14 by the
bridge's clock: 129 spell the fraction as three digits after a point, `.769`,
and 15 group the micros after them, `.524_315`. The bridge prints a Central
European summer clock, two hours ahead of the UTC its FIX frames state, and
`parse_log_messages` reads it in `Europe/Zurich` unless told another zone: the
112 lines printed at 14:46 are dated 12:46 UTC, the hour of the messages they
carry.

| hour the lines were printed at | UTC hour of `currunix` | lines |
| --- | --- | ---: |
| 03 | 01 | 16 |
| 14 | 12 | 112 |
| 16 | 14 | 1 |
| 23 | 21 | 15 |

### What it answers

Over the capture's whole day, `window_of("2026-08-14", "2026-08-14")`:

| reading | count | why |
| --- | ---: | --- |
| lines | 144 | one `bronze.record_keeping.log_messages` row each: the row number reaches each line's identity, so the exact repeats answer identities of their own |
| lines the row header does not date | 0 | the shipped header reads both fractions the capture spells, `.769` and `.524_315` |
| messages the parse answers | 136 | 65 lines carry no frame and each of the other 79 carries one, each of the 56 reports of a fill answers the execution it reports beside itself, and the trade report the one execution its side states; a message is a row, not a line |
| `bronze.record_keeping.fix_messages` rows | 136 | every copy a hop logged is placed apart at its instant, so the key folds none |
| `silver.record_keeping.fix_messages` rows | 67 | the 136 bronze rows walked: the copies of one event folded and one expiry added, 25 events, and every chain still alive restated on each whole hour, 42 views |
| `silver.record_keeping.books` rows | 46 | the 67 silver rows folded into three `MIC:CFI` categories: 8 books an event moved, and every book restated on each whole hour from the first after it opens to 21:00, 38 views |
| `silver.record_keeping.orders`, `quotes`, `executions` rows | 10, 0, 8 | the books' order deltas, quote deltas and executions |

At 21:59:46 the capture holds a cancel request and its reject, which states
no `Side(54)`: the walk joins it to the one live side of its order, the
sell, so the fold books both on that side -- the two orders the day adds to
the morning's eight. The documented runs use
`window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")`, the morning's
order flow, its one trade report at 14:52 and the expiry of its open order at
16:25, which lands 129 lines, 126 bronze and 49 silver FIX rows, 30 books, 8
orders, 0 quotes and 8 executions:
[Data samples](../docs/samples/index.md) shows every table's rows.

`python/tests/storages/` runs the capture through every task under
`-m integration`, and `python/tests/test_docs.py` pins the digest above.
