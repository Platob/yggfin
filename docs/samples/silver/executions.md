# silver.record_keeping.executions

7 rows: the execution leaves of the one book snapshot `parse_books` committed
over `[2026-08-14 00:00, 2026-08-14 16:30)` UTC. Columns: [silver.record_keeping.executions](../../tables/silver/executions.md).

## Every row

| currunix | kind | ticker | crosscode | side | state | price | quantity | lastpx | lastqty | lines |
| --- | --- | --- | --- | --- | --- | :---: | :---: | :---: | :---: | --- |
| `2026-08-14 01:03:17` | `execution_event` | `1605` | `20260814_CQ9_LIAPUS_9623` | `BUY` | `TRADE` (4002) |  | 3000000 | 39.9 | 24000 | 123, 124, 126 |
| `2026-08-14 12:46:39.743` | `execution_event` | `HOLN` | `00079132558GLXC0` | `BUY` | `FILLED` (8003) | 72.28 | 300 | 72.28 | 235 | 2 |
| `2026-08-14 12:46:39.743016` | `execution_event` | `ABBN.S` | `00079132557GLXC0` | `BUY` | `PARTIALLY_FILLED` (4001) | 83.08 | 600 | 83.08 | 57 | 56, 57, 58, 60, 64, 71 |
| `2026-08-14 12:46:39.743016` | `execution_event` | `ABBN.S` | `00079132557GLXC0` | `BUY` | `FILLED` (8003) | 83.08 | 600 | 83.08 | 75 | 73, 74, 75, 77, 82, 83, 91 |
| `2026-08-14 12:46:39.743016` | `execution_event` | `ABBN.S` | `00079132557GLXC0` | `BUY` | `PARTIALLY_FILLED` (4001) | 83.08 | 600 | 83.08 | 21 | 6, 7, 8, 9, 11, 15, 22 |
| `2026-08-14 12:46:39.762` | `execution_event` | `ABBN.S` | `00079132557GLXC0` | `BUY` | `FILLED` (8003) | 83.08 | 600 | 83.08 | 57 | 35, 36, 37, 39, 44, 45, 53 |
| `2026-08-14 12:46:58.453` | `execution_event` | `EXAMPLECO.S` | `00079132541GLXC0` | `BUY` | `FILLED` (8003) | 83.04 | 36 | 83.04 | 36 | 105 |
