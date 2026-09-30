# bronze.record_keeping.fix_messages

126 rows: one per FIX message the parse read off the stored lines of
`[2026-08-14 00:00, 2026-08-14 16:30)` UTC, keyed on the message's identity. Nothing has walked, so
`prevuuid` is empty on every row; `seqnum` is the message's place in its
run -- the messages the parse handed over at one instant, one after
another -- null at place zero.
Columns: [bronze.record_keeping.fix_messages](../../tables/bronze/fix_messages.md).

## One execution, logged at every hop

Execution `00030561317VOJO7` is the last fill of order `BUYS:00084776691VFRM7`. The bridge
logged it on lines 73 to 92, once per plugin it passed, and wrote
prose between.
The parse answers a message per frame a line carries, and a report of a fill
answers the execution it splits off beside it. Every copy is a row of its
own: its place in its run reaches its identity, so bronze keeps each hop's
copy and the walk folds them into one event.

| line | msgpluginid | body | the parse |
| :---: | --- | --- | --- |
| 73 | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=89525|34=40…` | row `…08dc84`; row `…aef2d8` |
| 74 | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVEN…` | row `…9b60c2`; row `…eecfb0` |
| 75 | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | row `…678c57`; row `…b33eb2` |
| 76 | `OMS_X1_TradeCapture_Add_Fields` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORI…` | row `…bd6656`; row `…dc09be` |
| 77 | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=C…` | row `…06bc3e`; row `…7a3d58` |
| 78 | `MIFID_BuySide_Timestamps_Add_Fields` | `IN : #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` | row `…691b6e`; row `…147518` |
| 79 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 80 | `MIFID_BuySide_Timestamps_Add_Fields` | `New Buy side MIFID trade` | no frame |
| 81 | `MIFID_BuySide_Timestamps_Add_Fields` | ` -> No DestKey. Unable to lookup the routes to …` | no frame |
| 82 | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ES…` | row `…4278c8`; row `…3e118d` |
| 83 | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH001222171…` | row `…3811a0`; row `…b318d9` |
| 84 | `ULBridge` | `Result of message post-enrichment : #CFICODE=ES…` | row `…9ad15e`; row `…0b65b1` |
| 85 | `EnrichmentManager` | `Enrichment execution[&SetEnv, &TECH_AddFields_O…` | no frame |
| 86 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | row `…7a32ee`; row `…6787a4` |
| 87 | `AliasManager` | `Resolve @ULFilter->ULFilter (0us)` | no frame |
| 88 | `ULBridge` | `A NOE rule matched, destination resolved to: UL…` | no frame |
| 89 | `ULBridge` | `Execution report with no ClOrderID so no route …` | no frame |
| 90 | `ULBridge` | `Message received: Message type [execution repor…` | row `…4214ba`; row `…242453` |
| 91 | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH001222…` | row `…0d8cad`; row `…9f6fa0` |
| 92 | `ULFilter` | `PushMessage : #CFICODE=ESVTFR|#ISINCODE=CH00122…` | row `…aeef81`; row `…c3c178` |

## Its rows

A message stating its own `SendingTime` is dated by the transaction clock
standing within `official_time_delay_ms` of it, here its `TransactTime`; one
stating none is measured against the line it was read off, which, read in
the bridge's zone, stands within that delay of its `TransactTime`, so it is
dated by that clock too, at the precision its frame spells. Each row names
the one line it was parsed from, and the execution a report splits off
names that report beside it, `FILLED`: one fill, complete in itself,
whatever the report's own state. The report itself is filed under its
order's category, `ORDR`.

| currunix | seqnum | msgcat | curruuid | state | sendingtime | transacttime | line |
| --- | :---: | --- | --- | --- | --- | --- | :---: |
| `2026-08-14 12:46:39.743` |  | `ORDR` (10) | `…9b60c2` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 74 |
| `2026-08-14 12:46:39.743` | 1 | `EXEC` (8) | `…eecfb0` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 74 |
| `2026-08-14 12:46:39.743` | 2 | `ORDR` (10) | `…678c57` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 75 |
| `2026-08-14 12:46:39.743` | 3 | `EXEC` (8) | `…b33eb2` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 75 |
| `2026-08-14 12:46:39.743` | 4 | `ORDR` (10) | `…bd6656` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 76 |
| `2026-08-14 12:46:39.743` | 5 | `EXEC` (8) | `…dc09be` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 76 |
| `2026-08-14 12:46:39.743` | 6 | `ORDR` (10) | `…06bc3e` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 77 |
| `2026-08-14 12:46:39.743` | 7 | `EXEC` (8) | `…7a3d58` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 77 |
| `2026-08-14 12:46:39.743` | 8 | `ORDR` (10) | `…691b6e` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 78 |
| `2026-08-14 12:46:39.743` | 9 | `EXEC` (8) | `…147518` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 78 |
| `2026-08-14 12:46:39.743` | 10 | `ORDR` (10) | `…4278c8` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 82 |
| `2026-08-14 12:46:39.743` | 11 | `EXEC` (8) | `…3e118d` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 82 |
| `2026-08-14 12:46:39.743` | 12 | `ORDR` (10) | `…3811a0` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 83 |
| `2026-08-14 12:46:39.743` | 13 | `EXEC` (8) | `…b318d9` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 83 |
| `2026-08-14 12:46:39.743` | 14 | `ORDR` (10) | `…9ad15e` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 84 |
| `2026-08-14 12:46:39.743` | 15 | `EXEC` (8) | `…0b65b1` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 84 |
| `2026-08-14 12:46:39.743` | 16 | `ORDR` (10) | `…7a32ee` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 86 |
| `2026-08-14 12:46:39.743` | 17 | `EXEC` (8) | `…6787a4` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 86 |
| `2026-08-14 12:46:39.743` | 18 | `ORDR` (10) | `…4214ba` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 90 |
| `2026-08-14 12:46:39.743` | 19 | `EXEC` (8) | `…242453` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 90 |
| `2026-08-14 12:46:39.743` | 20 | `ORDR` (10) | `…0d8cad` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 91 |
| `2026-08-14 12:46:39.743` | 21 | `EXEC` (8) | `…9f6fa0` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 91 |
| `2026-08-14 12:46:39.743` | 22 | `ORDR` (10) | `…aeef81` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 92 |
| `2026-08-14 12:46:39.743` | 23 | `EXEC` (8) | `…c3c178` | `FILLED` (8003) |  | `2026-08-14 12:46:39.743` | 92 |
| `2026-08-14 12:46:39.743016` |  | `ORDR` (10) | `…08dc84` | `FILLED` (8003) | `2026-08-14 12:46:39.762` | `2026-08-14 12:46:39.743016` | 73 |
| `2026-08-14 12:46:39.743016` | 1 | `EXEC` (8) | `…aef2d8` | `FILLED` (8003) | `2026-08-14 12:46:39.762` | `2026-08-14 12:46:39.743016` | 73 |

## Every row, by message type and state

| msgtype | state | rows |
| --- | --- | :---: |
| `8` | `NEW` (2001) | 8 |
| `8` | `PARTIALLY_FILLED` (4001) | 3 |
| `8` | `TRADE` (4002) | 23 |
| `8` | `FILLED` (8003) | 86 |
| `A` | `UNKNOWN` (0) | 1 |
| `AE` | `FILLED` (8003) | 2 |
| `D` | `PENDING_NEW` (1001) | 2 |
| `n` | `FILLED` (8003) | 1 |
