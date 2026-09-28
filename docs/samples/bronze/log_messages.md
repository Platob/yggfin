# bronze.record_keeping.log_messages

128 lines of the capture fall in `[2026-08-14 00:00, 2026-08-14 16:30)` UTC; each is one row.
Columns: [bronze.record_keeping.log_messages](../../tables/bronze/log_messages.md).

## Lines per hour

A line is dated by its row header, as the bridge printed it, so the table is
laid out by the hour of the bridge's own clock.

| hour of `currunix` | lines |
| --- | :---: |
| `03:00` | 16 |
| `14:00` | 112 |

## The first 8 lines

`seqnum` is the row number in the object the line was read from, and `body`
is the line past its row header; the other columns are the header's captures.

| seqnum | currunix | curruuid | msgpluginid | loglevel | msgseqnum | body |
| :---: | --- | --- | --- | --- | :---: | --- |
| 1 | `2026-08-14 14:46:39.769` | `…5f1d22` | `ULBridge` | `INFO` | 3088 | `Execution report from OMS_X1_OrderOut type trade for KL3RCZUA797. Forwa…` |
| 2 | `2026-08-14 14:46:39.769` | `…64d589` | `ULBridge` | `INFO` | 3088 | `Message received: Message type [execution report <trade>] from (OMS_X1_…` |
| 3 | `2026-08-14 14:46:39.769` | `…e078b0` | `EnrichmentManager` | `INFO` | 3088 | `Enrichment execution[&SetEnv, &ULMSG_Add_Fields]` |
| 4 | `2026-08-14 14:46:39.769` | `…0a64e0` | `ULMSG_BROKER_BDG_DMZ_CLI` | `DEBUG` | 3088 | `PushMessage : #AVEXDESTINATION=XSWX|#BLOOMBERGCODE=HOLN SW|#CFICODE=ESX…` |
| 5 | `2026-08-14 14:46:39.769` | `…b27698` | `ULMSG_BROKER_BDG_DMZ_CLI` | `INFO` | 3088 | `Sending : 8=FIX.4.2|9=3513|35=UL|49=ULB_DMZ_BROKER_BDG|56=ULB_CLI_BROKE…` |
| 6 | `2026-08-14 14:46:39.769` | `…ea2708` | `OMS_X1_TradeCapture` | `INFO` | 40218 | `Receiving : 8=FIX.4.4|9=938|35=8|34=40218|49=OMSAUDITX1|56=FIRMAUDITX1|…` |
| 7 | `2026-08-14 14:46:39.769` | `…4b2d4d` | `OMS_X1_TradeCapture` | `DEBUG` | 40218 | `RouteMessage : ACTION=EXECUTION|AGGRESSORINDICATOR=Y|ALTEVENTTEXT=Order…` |
| 8 | `2026-08-14 14:46:39.769` | `…5bc1cf` | `TECH_AddFields_OMS_X1` | `DEBUG` | 40218 | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR=Y|ALTEVENTTEXT=…` |

## What every line states

A line is an event the read settled, so it carries the event columns every
table opens with; a line is not a lifecycle, so most of them are empty.

| column | on every line |
| --- | --- |
| `crosscode` | `local://bound/data/capture/ulbridge.log` |
| `state` | `UNKNOWN` (0) |
| `creaunix`, `execunix`, `recdunix`, `exprunix`, `prevunix`, `snapunix` | empty |
| `prevuuid`, `srcuuids` | empty |
