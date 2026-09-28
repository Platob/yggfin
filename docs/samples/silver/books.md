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
| `2026-08-14 01:03:17` | `JOVM:XXXXXX` | `…a5a2fb` | 1 | 1 | 1 | 1 | 0 |  |  |
| `2026-08-14 02:00:00` | `JOVM:XXXXXX` | `…520d8d` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 03:00:00` | `JOVM:XXXXXX` | `…baf80d` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 04:00:00` | `JOVM:XXXXXX` | `…dccd9e` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 05:00:00` | `JOVM:XXXXXX` | `…b4795c` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 06:00:00` | `JOVM:XXXXXX` | `…9a2617` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 07:00:00` | `JOVM:XXXXXX` | `…204fe5` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 08:00:00` | `JOVM:XXXXXX` | `…236928` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 09:00:00` | `JOVM:XXXXXX` | `…276791` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 10:00:00` | `JOVM:XXXXXX` | `…6169d1` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 11:00:00` | `JOVM:XXXXXX` | `…66d8a5` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 12:00:00` | `JOVM:XXXXXX` | `…265668` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 12:46:39.743` | `XSWX:ESVTFR` | `…6066f4` | 0 | 1 | 1 | 0 | 0 |  |  |
| `2026-08-14 12:46:39.743016` | `XSWX:ESVTFR` | `…29d236` | 1 | 3 | 3 | 1 | 0 | 83.08 |  |
| `2026-08-14 12:46:39.762` | `XSWX:ESVTFR` | `…5c7e36` | 0 | 1 | 1 | 0 | 0 |  |  |
| `2026-08-14 12:46:40.02` | `XXXX:XXXXXX` | `…ec0691` | 1 | 1 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 12:46:58.453` | `XSWX:ESVTFR` | `…e6a3e3` | 0 | 1 | 1 | 0 | 0 |  |  |
| `2026-08-14 13:00:00` | `XSWX:ESVTFR` | `…485c90` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 13:00:00` | `JOVM:XXXXXX` | `…d10997` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 13:00:00` | `XXXX:XXXXXX` | `…004a55` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 14:00:00` | `XSWX:ESVTFR` | `…2319c8` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 14:00:00` | `JOVM:XXXXXX` | `…9fd5a3` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 14:00:00` | `XXXX:XXXXXX` | `…36b51c` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 15:00:00` | `XSWX:ESVTFR` | `…3cde99` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 15:00:00` | `XXXX:XXXXXX` | `…91d1ac` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |
| `2026-08-14 15:00:00` | `JOVM:XXXXXX` | `…115e54` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 16:00:00` | `XSWX:ESVTFR` | `…b51fa7` | 0 | 0 | 0 | 0 | 0 |  |  |
| `2026-08-14 16:00:00` | `JOVM:XXXXXX` | `…f0e04a` | 1 | 0 | 0 | 1 | 0 |  |  |
| `2026-08-14 16:00:00` | `XXXX:XXXXXX` | `…91286c` | 1 | 0 | 0 | 1 | 0 | 72.3 |  |

## One book with depth

The first book standing any depth is `JOVM:XXXXXX` at
`2026-08-14 01:03:17`. `alive` is what stands on either side, `deltas`
what changed since the book before, `executions` what traded, and
`bidlimits` and `asklimits` the price levels `alive` aggregates to, best
first; `bidpx` and `askpx` are the best tradable level of each.

| marketdatakind | side | crosscode | state | price | quantity | curruuid |
| --- | --- | --- | --- | :---: | :---: | --- |
| `ORDR` (10) | `BUY` (1) | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) |  | 3000000 | `…4b1b7f` |

| levels | price | quantity | orders | tradable |
| --- | :---: | :---: | :---: | --- |
| `bidlimits` |  | 3000000 | 1 | `true` |
