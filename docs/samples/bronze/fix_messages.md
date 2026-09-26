# bronze.record_keeping.fix_messages

41 rows: one per FIX message the parse read off the stored lines of
`[2026-08-14 00:00, 2026-08-14 16:30)` UTC, keyed on the message's identity. Nothing has walked, so
`seqnum` and `prevuuid` are empty on every row.
Columns: [bronze.record_keeping.fix_messages](../../tables/bronze/fix_messages.md).

## One execution, logged at every hop

Execution `00064703468GBYZ0` closed order `00079132557GLXC0`. The bridge logged it on
lines 73 to 92, once per plugin it passed, and wrote prose between.
The parse answers a message per frame a line carries; a frame that restates
another under the same identity is folded by the key and counted in
`skipped` -- 6 of them here.

| line | msgpluginid | body | the parse |
| :---: | --- | --- | --- |
| 73 | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=78182|34=40…` | row `…821b63` |
| 74 | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVEN…` | row `…a71bf3` |
| 75 | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | row `…914f96` |
| 76 | `OMS_X1_TradeCapture_Add_Fields` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | restates `…914f96`, folded |
| 77 | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=C…` | row `…bada44` |
| 78 | `MIFID_BuySide_Timestamps_Add_Fields` | `IN : #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` | restates `…bada44`, folded |
| 79 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 80 | `MIFID_BuySide_Timestamps_Add_Fields` | `New Buy side MIFID trade` | no frame |
| 81 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 82 | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ES…` | row `…09b542` |
| 83 | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH001222171…` | row `…d15a8f` |
| 84 | `ULBridge` | `Result of message post-enrichment : #CFICODE=ES…` | restates `…d15a8f`, folded |
| 85 | `EnrichmentManager` | `Enrichment execution[&SetEnv, &TECH_AddFields_O…` | no frame |
| 86 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | restates `…d15a8f`, folded |
| 87 | `AliasManager` | `Resolve @ULFilter->ULFilter (0us)` | no frame |
| 88 | `ULBridge` | `A NOE rule matched, destination resolved to: UL…` | no frame |
| 89 | `ULBridge` | `Execution report with no ClOrderID so no route …` | no frame |
| 90 | `ULBridge` | `Message received: Message type [execution repor…` | restates `…d15a8f`, folded |
| 91 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | row `…84ea0a` |
| 92 | `ULFilter` | `PushMessage : #CFICODE=ESVTFR|#ISINCODE=CH00122…` | restates `…84ea0a`, folded |

## Its rows

A message stating its own `SendingTime` is dated by the transaction clock
standing within `official_time_delay_ms` of it, here its `TransactTime`; one
stating none is dated by the line it was read off, until the walk dates it
by its `TransactTime`. Each row names the one line it was parsed from.

| currunix | curruuid | state | sendingtime | transacttime | line |
| --- | --- | --- | --- | --- | :---: |
| `2026-08-14 12:46:39.743016` | `…821b63` | `FILLED` (8003) | `2026-08-14 12:46:39.762` | `2026-08-14 12:46:39.743016` | 73 |
| `2026-08-14 14:46:39.771` | `…84ea0a` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 91 |
| `2026-08-14 14:46:39.771` | `…09b542` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 82 |
| `2026-08-14 14:46:39.771` | `…a71bf3` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 74 |
| `2026-08-14 14:46:39.771` | `…bada44` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 77 |
| `2026-08-14 14:46:39.771` | `…d15a8f` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 83 |
| `2026-08-14 14:46:39.771` | `…914f96` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 75 |

## Every row, by message type and state

| msgtype | state | rows |
| --- | --- | :---: |
| `8` | `NEW` (2001) | 4 |
| `8` | `PARTIALLY_FILLED` (4001) | 3 |
| `8` | `TRADE` (4002) | 13 |
| `8` | `FILLED` (8003) | 17 |
| `A` | `UNKNOWN` (0) | 1 |
| `D` | `PENDING_NEW` (1001) | 2 |
| `n` | `FILLED` (8003) | 1 |
