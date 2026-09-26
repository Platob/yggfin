# silver.record_keeping.fix_messages

14 rows: the bronze messages of `[2026-08-14 00:00, 2026-08-14 16:30)` UTC walked into
the events they are, each placed in its chain and naming every line it was
logged on. Columns: [silver.record_keeping.fix_messages](../../tables/silver/fix_messages.md).

## Two chains

`crosscode` is the business identifier a chain shares, `seqnum` the step an
event stands at and `prevuuid` the event it follows. An expiry is an event
the walk generates at the deadline the chain stated: no line recorded it.

### `00079132557GLXC0`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…9cd112` |  |  | 6, 7, 8, 9, 11, 15, 22 |
| 1 | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…0070ab` | `…9cd112` | `2026-08-14 12:46:39.743016` | 56, 57, 58, 60, 64, 71 |
| 2 | `2026-08-14 12:46:39.743016` | `FILLED` (8003) | `…52194f` | `…0070ab` | `2026-08-14 12:46:39.743016` | 73, 74, 75, 77, 82, 83, 91 |
|  | `2026-08-14 12:46:39.762` | `FILLED` (8003) | `…cae164` |  |  | 35, 36, 37, 39, 44, 45, 53 |

### `00079132559GLXC0`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.757` | `NEW` (2001) | `…7b182c` |  |  | 34 |
| 1 | `2026-08-14 12:46:39.757079` | `NEW` (2001) | `…ed8d14` | `…7b182c` | `2026-08-14 12:46:39.757` | 24, 25, 26 |
| 2 | `2026-08-14 16:25:00` | `EXPIRED` (9500) | `…010180` | `…ed8d14` | `2026-08-14 12:46:39.757079` | 24, 25, 26 |

## One event, every line it was logged on

Execution `00064703468GBYZ0` is one event, `…52194f`, dated
`2026-08-14 12:46:39.743016` by its `TransactTime` and recorded first at
`2026-08-14 14:46:39.771`; `srcuuids` names the 7 lines
whose frames the walk merged into it, each joining to a
`bronze.record_keeping.log_messages.curruuid`:

| line | curruuid | msgpluginid | body |
| :---: | --- | --- | --- |
| 73 | `…dd73f0` | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=78182|34=40221|49=O…` |
| 74 | `…cf8893` | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVENTTIMESTA…` |
| 75 | `…a77e13` | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR…` |
| 77 | `…e2d69f` | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=CH0012221…` |
| 82 | `…9a8185` | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ESVTFR|#IS…` |
| 83 | `…aecceb` | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LASTM…` |
| 91 | `…0a3db8` | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` |

## States

`state` is stored as the `int32` code of a lifecycle-sorted enum;
[States](../../tables/states.md) lists every member.

| state | rows |
| --- | :---: |
| `UNKNOWN` (0) | 1 |
| `PENDING_NEW` (1001) | 1 |
| `NEW` (2001) | 2 |
| `PARTIALLY_FILLED` (4001) | 2 |
| `TRADE` (4002) | 1 |
| `FILLED` (8003) | 6 |
| `EXPIRED` (9500) | 1 |
