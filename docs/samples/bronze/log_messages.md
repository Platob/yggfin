# bronze.record_keeping.log_messages

129 lines of the capture fall in `[2026-08-14 00:00, 2026-08-14 16:30)` UTC; each is one row.
Columns: [bronze.record_keeping.log_messages](../../tables/bronze/log_messages.md).

## Lines per hour

A line is dated by its row header, whose clock states no offset and is read
in the zone the bridge prints in, `Europe/Zurich`, so the table is laid out by
the UTC hour each line was printed at: the hour of the messages the lines
carry, two hours before the one the bridge's own clock spells.

| hour of `currunix` | on the bridge's clock | lines |
| --- | --- | :---: |
| `01:00` | `03:00` | 16 |
| `12:00` | `14:00` | 112 |
| `14:00` | `16:00` | 1 |

## The first 8 lines

`seqnum` is the row number in the object the line was read from, and `body`
is the line past its row header; the other columns are the header's captures.

| seqnum | currunix | curruuid | msgpluginid | loglevel | msgseqnum | body |
| :---: | --- | --- | --- | --- | :---: | --- |
| 1 | `2026-08-14 12:46:39.769` | `…b4e5e9` | `ULBridge` | `INFO` | 3088 | `Execution report from OMS_X1_OrderOut type trade for KL3RCZUA797. Forwa…` |
| 2 | `2026-08-14 12:46:39.769` | `…d2a7ac` | `ULBridge` | `INFO` | 3088 | `Message received: Message type [execution report <trade>] from (OMS_X1_…` |
| 3 | `2026-08-14 12:46:39.769` | `…453ee3` | `EnrichmentManager` | `INFO` | 3088 | `Enrichment execution[&SetEnv, &ULMSG_Add_Fields]` |
| 4 | `2026-08-14 12:46:39.769` | `…0a9572` | `ULMSG_BROKER_BDG_DMZ_CLI` | `DEBUG` | 3088 | `PushMessage : #AVEXDESTINATION=XSWX|#BLOOMBERGCODE=HOLN SW|#CFICODE=ESX…` |
| 5 | `2026-08-14 12:46:39.769` | `…03e18e` | `ULMSG_BROKER_BDG_DMZ_CLI` | `INFO` | 3088 | `Sending : 8=FIX.4.2|9=3513|35=UL|49=ULB_DMZ_BROKER_BDG|56=ULB_CLI_BROKE…` |
| 6 | `2026-08-14 12:46:39.769` | `…778909` | `OMS_X1_TradeCapture` | `INFO` | 40218 | `Receiving : 8=FIX.4.4|9=938|35=8|34=40218|49=OMSAUDITX1|56=FIRMAUDITX1|…` |
| 7 | `2026-08-14 12:46:39.769` | `…3b32da` | `OMS_X1_TradeCapture` | `DEBUG` | 40218 | `RouteMessage : ACTION=EXECUTION|AGGRESSORINDICATOR=Y|ALTEVENTTEXT=Order…` |
| 8 | `2026-08-14 12:46:39.769` | `…f6ee81` | `TECH_AddFields_OMS_X1` | `DEBUG` | 40218 | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR=Y|ALTEVENTTEXT=…` |

## What every line states

A line is an event the read settled, so it carries the event columns every
table opens with; a line is not a lifecycle, so most of them are empty.

| column | on every line |
| --- | --- |
| `crosscode` | `local://bound/data/capture/ulbridge.log` |
| `state` | `UNKNOWN` (0) |
| `creaunix` | the earliest instant the read has dated a line of the capture by |
| `prevunix` | the instant the read dated the line before by |
| `recdunix`, `exprunix`, `snapunix` | empty |
| `prevuuid`, `srcuuids` | empty |
