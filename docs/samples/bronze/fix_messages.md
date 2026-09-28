# bronze.record_keeping.fix_messages

74 rows: one per FIX message the parse read off the stored lines of
`[2026-08-14 00:00, 2026-08-14 16:30)` UTC, keyed on the message's identity. Nothing has walked, so
`seqnum` and `prevuuid` are empty on every row.
Columns: [bronze.record_keeping.fix_messages](../../tables/bronze/fix_messages.md).

## One execution, logged at every hop

Execution `00030561317VOJO7` closed order `BUY:00084776691VFRM7`. The bridge logged it on
lines 73 to 92, once per plugin it passed, and wrote prose between.
The parse answers a message per frame a line carries, and a report of a fill
answers the execution it splits off beside it; a message that restates
another under the same identity is folded by the key and counted in
`skipped` -- 12 of them here.

| line | msgpluginid | body | the parse |
| :---: | --- | --- | --- |
| 73 | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=89525|34=40…` | row `…1bd79f`; row `…44557d` |
| 74 | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVEN…` | row `…36bdc1`; row `…49748f` |
| 75 | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | row `…8f34a4`; row `…469ab9` |
| 76 | `OMS_X1_TradeCapture_Add_Fields` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | restates `…8f34a4`, folded; restates `…469ab9`, folded |
| 77 | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=C…` | row `…699d7d`; row `…879dd1` |
| 78 | `MIFID_BuySide_Timestamps_Add_Fields` | `IN : #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` | restates `…699d7d`, folded; restates `…879dd1`, folded |
| 79 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 80 | `MIFID_BuySide_Timestamps_Add_Fields` | `New Buy side MIFID trade` | no frame |
| 81 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 82 | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ES…` | row `…1372bf`; row `…963af2` |
| 83 | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH001222171…` | row `…dc1cb8`; row `…4d3852` |
| 84 | `ULBridge` | `Result of message post-enrichment : #CFICODE=ES…` | restates `…dc1cb8`, folded; restates `…4d3852`, folded |
| 85 | `EnrichmentManager` | `Enrichment execution[&SetEnv, &TECH_AddFields_O…` | no frame |
| 86 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | restates `…dc1cb8`, folded; restates `…4d3852`, folded |
| 87 | `AliasManager` | `Resolve @ULFilter->ULFilter (0us)` | no frame |
| 88 | `ULBridge` | `A NOE rule matched, destination resolved to: UL…` | no frame |
| 89 | `ULBridge` | `Execution report with no ClOrderID so no route …` | no frame |
| 90 | `ULBridge` | `Message received: Message type [execution repor…` | restates `…dc1cb8`, folded; restates `…4d3852`, folded |
| 91 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | row `…66a1aa`; row `…0bc0aa` |
| 92 | `ULFilter` | `PushMessage : #CFICODE=ESVTFR|#ISINCODE=CH00122…` | restates `…66a1aa`, folded; restates `…0bc0aa`, folded |

## Its rows

A message stating its own `SendingTime` is dated by the transaction clock
standing within `official_time_delay_ms` of it, here its `TransactTime`; one
stating none is dated by the line it was read off, until the walk dates it
by its `TransactTime`. Each row names the one line it was parsed from, and
the execution a report splits off names that report beside it, `FILLED`:
one fill, complete in itself, whatever the report's own state.

| currunix | msgcat | curruuid | state | sendingtime | transacttime | line |
| --- | --- | --- | --- | --- | --- | :---: |
| `2026-08-14 12:46:39.743016` | `EXEC` (8) | `…44557d` | `FILLED` (8003) | `2026-08-14 12:46:39.762` | `2026-08-14 12:46:39.743016` | 73 |
| `2026-08-14 12:46:39.743016` | `ORDR` (10) | `…1bd79f` | `FILLED` (8003) | `2026-08-14 12:46:39.762` | `2026-08-14 12:46:39.743016` | 73 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…66a1aa` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 91 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…36bdc1` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 74 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…879dd1` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 77 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…8f34a4` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 75 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…0bc0aa` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 91 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…1372bf` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 82 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…469ab9` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 75 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…963af2` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 82 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…49748f` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 74 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…dc1cb8` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 83 |
| `2026-08-14 14:46:39.771` | `ORDR` (10) | `…699d7d` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 77 |
| `2026-08-14 14:46:39.771` | `EXEC` (8) | `…4d3852` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 83 |

## Every row, by message type and state

| msgtype | state | rows |
| --- | --- | :---: |
| `8` | `NEW` (2001) | 4 |
| `8` | `PARTIALLY_FILLED` (4001) | 3 |
| `8` | `TRADE` (4002) | 13 |
| `8` | `FILLED` (8003) | 50 |
| `A` | `UNKNOWN` (0) | 1 |
| `D` | `PENDING_NEW` (1001) | 2 |
| `n` | `FILLED` (8003) | 1 |
