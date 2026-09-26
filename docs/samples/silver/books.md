# silver.record_keeping.books

6 rows: one per book the fold answered over the silver events of
`[2026-08-14 00:00, 2026-08-14 16:30)` UTC, from no depth before its start. Columns:
[silver.record_keeping.books](../../tables/silver/books.md).

## Every book

| currunix | ticker | curruuid | executions | bid live | bid deltas | ask live | ask deltas |
| --- | --- | --- | :---: | :---: | :---: | :---: | :---: |
| `2026-08-14 01:03:17` | `1605` | `…2cc900` | 1 | 0 | 0 | 0 | 0 |
| `2026-08-14 12:46:39.743` | `HOLN` | `…c7ac99` | 1 | 0 | 0 | 0 | 0 |
| `2026-08-14 12:46:39.743016` | `ABBN.S` | `…6f8604` | 3 | 0 | 0 | 0 | 0 |
| `2026-08-14 12:46:39.762` | `ABBN.S` | `…e09d28` | 1 | 0 | 0 | 0 | 0 |
| `2026-08-14 12:46:40.02` | `HOLN` | `…6c3488` | 0 | 1 | 1 | 0 | 0 |
| `2026-08-14 12:46:58.453` | `EXAMPLECO.S` | `…fdc785` | 1 | 0 | 0 | 0 | 0 |

## One book with depth

The first book standing any depth is `HOLN` at
`2026-08-14 12:46:40.02`. `live` is what stands on a side, `deltas` what
changed it since the book before, and `limits` the price levels `live`
aggregates to.

| side | kind | crosscode | state | price | quantity | curruuid |
| --- | --- | --- | --- | :---: | :---: | --- |
| `bidside.live` | `order_event` | `XM8NNITE383` | `PENDING_NEW` (1001) | 72.3 | 50 | `…7a8389` |

| side | price | quantity | orders |
| --- | :---: | :---: | :---: |
| `bidside.limits` | 72.3 | 50 | 1 |
