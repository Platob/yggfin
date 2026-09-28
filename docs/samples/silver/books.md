# silver.record_keeping.books

29 rows: one per book the fold answered over the silver events of
`[2026-08-14 00:00, 2026-08-14 16:30)` UTC, read from the hour before its start,
and every book again on each whole hour between the fold's first and last
operation, `snapunix` set and no event restated: the views silver holds
there are the book's membership, never a delta. A book is one `MIC:CFI`
category per instant, so `crosscode` is the category and no book states a
ticker. Columns: [silver.record_keeping.books](../../tables/silver/books.md).

## Every book

| currunix | crosscode | curruuid | alive | deltas | executions | bidlimits | asklimits | bidpx | askpx |
| --- | --- | --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `2026-08-14 01:03:17` | `PUMA:XXXXXX` | `…2e58af` | 1 | 1 | 1 | 1 | 0 |  |  |
| `2026-08-14 02:00:00` | `PUMA:XXXXXX` | `…4dbff9` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 03:00:00` | `PUMA:XXXXXX` | `…82271a` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 04:00:00` | `PUMA:XXXXXX` | `…7de0ac` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 05:00:00` | `PUMA:XXXXXX` | `…baf7ba` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 06:00:00` | `PUMA:XXXXXX` | `…40badf` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 07:00:00` | `PUMA:XXXXXX` | `…0c8e3d` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 08:00:00` | `PUMA:XXXXXX` | `…fa368b` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 09:00:00` | `PUMA:XXXXXX` | `…784199` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 10:00:00` | `PUMA:XXXXXX` | `…70c33e` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 11:00:00` | `PUMA:XXXXXX` | `…0ea77c` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 12:00:00` | `PUMA:XXXXXX` | `…6486bb` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 12:46:39.743` | `XSWX:ESVTFR` | `…466dd3` | 0 | 2 | 1 | 0 | 0 |  |  |
| `2026-08-14 12:46:39.743016` | `XSWX:ESVTFR` | `…2fbcd3` | 0 | 3 | 3 | 0 | 0 |  |  |
| `2026-08-14 12:46:39.762` | `XSWX:ESVTFR` | `…a9f680` | 0 | 1 | 1 | 0 | 0 |  |  |
| `2026-08-14 12:46:40.02` | `XXXX:XXXXXX` | `…ec0691` | 1 | 1 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 12:46:58.453` | `XSWX:ESVTFR` | `…4a468f` | 0 | 1 | 1 | 0 | 0 |  |  |
| `2026-08-14 13:00:00` | `XSWX:ESVTFR` | `…485c90` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 13:00:00` | `PUMA:XXXXXX` | `…6f5c17` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 13:00:00` | `XXXX:XXXXXX` | `…004a55` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 14:00:00` | `XSWX:ESVTFR` | `…2319c8` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 14:00:00` | `PUMA:XXXXXX` | `…eec671` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 14:00:00` | `XXXX:XXXXXX` | `…36b51c` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 15:00:00` | `XSWX:ESVTFR` | `…3cde99` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 15:00:00` | `PUMA:XXXXXX` | `…df3177` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 15:00:00` | `XXXX:XXXXXX` | `…91d1ac` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 16:00:00` | `XSWX:ESVTFR` | `…b51fa7` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 16:00:00` | `PUMA:XXXXXX` | `…c414a1` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 16:00:00` | `XXXX:XXXXXX` | `…91286c` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |

## One book with depth

The first book standing any depth is `PUMA:XXXXXX` at
`2026-08-14 01:03:17`. `alive` is what stands on either side, `deltas`
what changed since the book before, `executions` what traded, and
`bidlimits` and `asklimits` the price levels `alive` aggregates to, best
first; `bidpx` and `askpx` are the best tradable level of each.

| marketdatakind | side | crosscode | state | price | quantity | curruuid |
| --- | --- | --- | --- | :---: | :---: | --- |
| `ORDR` (10) | `BUY` (1) | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) |  | 3000000 | `…72e147` |

| levels | price | quantity | orders | tradable |
| --- | :---: | :---: | :---: | --- |
| `bidlimits` |  | 3000000 | 1 | `true` |
