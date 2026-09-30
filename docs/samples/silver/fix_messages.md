# silver.record_keeping.fix_messages

49 rows: the bronze messages of `[2026-08-14 00:00, 2026-08-14 16:30)` UTC walked into
the 22 events they are, the copies of each folded into one row
placed in its chain and naming the lines its session event was logged on,
and 27 hourly views of the chains alive.
Columns: [silver.record_keeping.fix_messages](../../tables/silver/fix_messages.md).

## Two chains

`crosscode` is the business identifier a chain shares, `seqnum` an event's
place among the events of its instant and `prevuuid` the event it follows.
An expiry is an event the walk generates at the deadline the chain stated:
no line recorded it.

The fills of `BUYS:00084776691VFRM7` are dated at one instant, and the walk takes
the rows of one instant in the order the table holds them -- by the place
each copy took in its run of the parse, then by `curruuid` -- rather than
in the order they happened: here the last fill, execution `00030561317VOJO7`, is
taken first and stands as a head of its own, while the first two chain on
to the bridge's later `FILLED` restatement, so [the books](books.md) hold
the order resting between the two.

### `BUYS:00084776691VFRM7`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.743016` | `FILLED` (8003) | `…d3a6d4` |  |  | 73, 74, 75, 76, 77, 78, 82, 83, 84, 86, 90, 91, 92 |
| 1 | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…7b9c09` |  |  | 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 21, 22 |
| 2 | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…ec2710` | `…7b9c09` | `2026-08-14 12:46:39.743016` | 56, 57, 58, 59, 60, 61, 63, 64, 65, 70, 71 |
|  | `2026-08-14 12:46:39.762` | `FILLED` (8003) | `…a11727` | `…ec2710` | `2026-08-14 12:46:39.743016` | 35, 36, 37, 38, 39, 40, 44, 45, 46, 48, 52, 53, 54 |

### `BUYS:00037497066VFRM7`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.757` | `NEW` (2001) | `…6b1160` |  |  | 34 |
|  | `2026-08-14 12:46:39.757079` | `UPDATED` (3004) | `…4d4b85` | `…6b1160` | `2026-08-14 12:46:39.757` | 24, 25, 26, 27, 28, 31, 33 |
|  | `2026-08-14 16:25:00` | `EXPIRED` (9500) | `…43f6ff` | `…4d4b85` | `2026-08-14 12:46:39.757079` | 24, 25, 26, 27, 28, 31, 33 |

## Every chain alive on the hour

27 rows are views: at every whole hour the walk crosses, each chain
still alive is restated as it stands, dated at the tick -- `currunix` and
`snapunix` both -- under the identity that instant derives, which opens
with the tick's millisecond. It repeats the event's place, state and
lines and moves no chain on.

| snapunix | crosscode | state | curruuid | restates |
| --- | --- | --- | --- | --- |
| `2026-08-14 02:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffdff-6100-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 03:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffe36-4f80-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 04:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffe6d-3e00-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 05:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffea4-2c80-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 06:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffedb-1b00-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 07:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff12-0980-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 08:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff48-f800-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 09:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff7f-e680-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 10:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fffb6-d500-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 11:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fffed-c380-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 12:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00024-b200-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 13:00:00` | `BUYS:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a0005b-a080-7000-89ae-f29882a6fdd8` | `01a0004f-6b94-7000-89ae-f29882a6fdd8` |
| `2026-08-14 13:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a0005b-a080-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 13:00:00` | `BUYS:00037497066VFRM7` | `UPDATED` (3004) | `01a0005b-a080-7000-a528-77db054d4b85` | `01a0004f-6a8d-7000-a528-77db054d4b85` |
| `2026-08-14 13:00:00` |  | `UNKNOWN` (0) | `01a0005b-a080-7000-baef-ce79760e62d4` | `01a0004f-6b8c-7000-baef-ce79760e62d4` |
| `2026-08-14 14:00:00` | `BUYS:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a00092-8f00-7000-89ae-f29882a6fdd8` | `01a0004f-6b94-7000-89ae-f29882a6fdd8` |
| `2026-08-14 14:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00092-8f00-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 14:00:00` | `BUYS:00037497066VFRM7` | `UPDATED` (3004) | `01a00092-8f00-7000-a528-77db054d4b85` | `01a0004f-6a8d-7000-a528-77db054d4b85` |
| `2026-08-14 14:00:00` |  | `UNKNOWN` (0) | `01a00092-8f00-7000-baef-ce79760e62d4` | `01a0004f-6b8c-7000-baef-ce79760e62d4` |
| `2026-08-14 15:00:00` | `BUYS:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a000c9-7d80-7000-89ae-f29882a6fdd8` | `01a0004f-6b94-7000-89ae-f29882a6fdd8` |
| `2026-08-14 15:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a000c9-7d80-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 15:00:00` | `BUYS:00037497066VFRM7` | `UPDATED` (3004) | `01a000c9-7d80-7000-a528-77db054d4b85` | `01a0004f-6a8d-7000-a528-77db054d4b85` |
| `2026-08-14 15:00:00` |  | `UNKNOWN` (0) | `01a000c9-7d80-7000-baef-ce79760e62d4` | `01a0004f-6b8c-7000-baef-ce79760e62d4` |
| `2026-08-14 16:00:00` | `BUYS:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a00100-6c00-7000-89ae-f29882a6fdd8` | `01a0004f-6b94-7000-89ae-f29882a6fdd8` |
| `2026-08-14 16:00:00` | `BUYS:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00100-6c00-7000-928e-129231eb73f1` | `019ffdcb-7408-7000-928e-129231eb73f1` |
| `2026-08-14 16:00:00` | `BUYS:00037497066VFRM7` | `UPDATED` (3004) | `01a00100-6c00-7000-a528-77db054d4b85` | `01a0004f-6a8d-7000-a528-77db054d4b85` |
| `2026-08-14 16:00:00` |  | `UNKNOWN` (0) | `01a00100-6c00-7000-baef-ce79760e62d4` | `01a0004f-6b8c-7000-baef-ce79760e62d4` |

## One event, every line it was logged on

Execution `00030561317VOJO7` is one event, `…257236`, dated
`2026-08-14 12:46:39.743016` by its `TransactTime` and recorded first at
`2026-08-14 12:46:39.771`; `srcuuids` names the 13 lines
whose frames the walk merged into it, each joining to a
`bronze.record_keeping.log_messages.curruuid`, beside the reports it was
split out of:

| line | curruuid | msgpluginid | body |
| :---: | --- | --- | --- |
| 73 | `…a3b1c1` | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=89525|34=40221|49=O…` |
| 74 | `…13233d` | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVENTTIMESTA…` |
| 75 | `…3760da` | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR…` |
| 76 | `…ab9274` | `OMS_X1_TradeCapture_Add_Fields` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR…` |
| 77 | `…45d6bf` | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=CH0012221…` |
| 78 | `…1b0329` | `MIFID_BuySide_Timestamps_Add_Fields` | `IN : #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LASTMKT=XS…` |
| 82 | `…376741` | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ESVTFR|#IS…` |
| 83 | `…32badd` | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LASTM…` |
| 84 | `…c04717` | `ULBridge` | `Result of message post-enrichment : #CFICODE=ESVTFR|#IS…` |
| 86 | `…55538d` | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` |
| 90 | `…2affde` | `ULBridge` | `Message received: Message type [execution report <trade…` |
| 91 | `…c47110` | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` |
| 92 | `…75f949` | `ULFilter` | `PushMessage : #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#L…` |

## States

`state` is stored as the `int32` code of a lifecycle-sorted enum;
[States](../../tables/states.md) lists every member.

| state | rows |
| --- | :---: |
| `UNKNOWN` (0) | 5 |
| `PENDING_NEW` (1001) | 5 |
| `NEW` (2001) | 1 |
| `UPDATED` (3004) | 5 |
| `PARTIALLY_FILLED` (4001) | 2 |
| `TRADE` (4002) | 16 |
| `FILLED` (8003) | 14 |
| `EXPIRED` (9500) | 1 |
