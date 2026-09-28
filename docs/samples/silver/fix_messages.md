# silver.record_keeping.fix_messages

49 rows: the bronze messages of `[2026-08-14 00:00, 2026-08-14 16:30)` UTC walked into
the 22 events they are, each placed in its chain and naming
every line it was logged on, and 27 hourly views of the chains
alive. Columns: [silver.record_keeping.fix_messages](../../tables/silver/fix_messages.md).

## Two chains

`crosscode` is the business identifier a chain shares, `seqnum` the step an
event stands at and `prevuuid` the event it follows. An expiry is an event
the walk generates at the deadline the chain stated: no line recorded it.

### `BUY:00084776691VFRM7`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…2edb43` |  |  | 56, 57, 58, 60, 64, 71 |
| 1 | `2026-08-14 12:46:39.743016` | `PARTIALLY_FILLED` (4001) | `…fb98cf` | `…2edb43` | `2026-08-14 12:46:39.743016` | 6, 7, 8, 9, 11, 15, 22 |
| 2 | `2026-08-14 12:46:39.743016` | `FILLED` (8003) | `…cbe26b` | `…fb98cf` | `2026-08-14 12:46:39.743016` | 73, 74, 75, 77, 82, 83, 91 |
|  | `2026-08-14 12:46:39.762` | `FILLED` (8003) | `…77a2eb` |  |  | 35, 36, 37, 39, 44, 45, 53 |

### `BUY:00037497066VFRM7`

| seqnum | currunix | state | curruuid | prevuuid | prevunix | lines |
| :---: | --- | --- | --- | --- | --- | --- |
|  | `2026-08-14 12:46:39.757` | `NEW` (2001) | `…475745` |  |  | 34 |
| 1 | `2026-08-14 12:46:39.757079` | `UPDATED` (3004) | `…43c2a2` | `…475745` | `2026-08-14 12:46:39.757` | 24, 25, 26 |
| 2 | `2026-08-14 16:25:00` | `EXPIRED` (9500) | `…5f0ece` | `…43c2a2` | `2026-08-14 12:46:39.757079` | 24, 25, 26 |

## Every chain alive on the hour

27 rows are views: at every whole hour the walk crosses, each chain
still alive is restated as it stands, dated at the tick -- `currunix` and
`snapunix` both -- under the identity that instant derives, which opens
with the tick's millisecond. It repeats the event's place, state and
lines and moves no chain on.

| snapunix | crosscode | state | curruuid | restates |
| --- | --- | --- | --- | --- |
| `2026-08-14 02:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffdff-6100-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 03:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffe36-4f80-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 04:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffe6d-3e00-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 05:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffea4-2c80-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 06:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019ffedb-1b00-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 07:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff12-0980-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 08:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff48-f800-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 09:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fff7f-e680-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 10:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fffb6-d500-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 11:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `019fffed-c380-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 12:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00024-b200-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 13:00:00` | `BUY:00037497066VFRM7` | `UPDATED` (3004) | `01a0005b-a080-7001-9d6c-ba976843c2a2` | `01a0004f-6a8d-7001-9d6c-ba976843c2a2` |
| `2026-08-14 13:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a0005b-a080-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 13:00:00` |  | `UNKNOWN` (0) | `01a0005b-a080-7000-98ba-20ee319b978b` | `01a0004f-6b8c-7000-b93c-b9c562ad26cf` |
| `2026-08-14 13:00:00` | `BUY:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a0005b-a080-7000-b5fd-3d0df9f16c28` | `01a0004f-6b94-7000-b5fd-3d0df9f16c28` |
| `2026-08-14 14:00:00` | `BUY:00037497066VFRM7` | `UPDATED` (3004) | `01a00092-8f00-7001-9d6c-ba976843c2a2` | `01a0004f-6a8d-7001-9d6c-ba976843c2a2` |
| `2026-08-14 14:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00092-8f00-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 14:00:00` |  | `UNKNOWN` (0) | `01a00092-8f00-7000-98ba-20ee319b978b` | `01a0004f-6b8c-7000-b93c-b9c562ad26cf` |
| `2026-08-14 14:00:00` | `BUY:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a00092-8f00-7000-b5fd-3d0df9f16c28` | `01a0004f-6b94-7000-b5fd-3d0df9f16c28` |
| `2026-08-14 15:00:00` | `BUY:00037497066VFRM7` | `UPDATED` (3004) | `01a000c9-7d80-7001-9d6c-ba976843c2a2` | `01a0004f-6a8d-7001-9d6c-ba976843c2a2` |
| `2026-08-14 15:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a000c9-7d80-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 15:00:00` |  | `UNKNOWN` (0) | `01a000c9-7d80-7000-98ba-20ee319b978b` | `01a0004f-6b8c-7000-b93c-b9c562ad26cf` |
| `2026-08-14 15:00:00` | `BUY:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a000c9-7d80-7000-b5fd-3d0df9f16c28` | `01a0004f-6b94-7000-b5fd-3d0df9f16c28` |
| `2026-08-14 16:00:00` | `BUY:00037497066VFRM7` | `UPDATED` (3004) | `01a00100-6c00-7001-9d6c-ba976843c2a2` | `01a0004f-6a8d-7001-9d6c-ba976843c2a2` |
| `2026-08-14 16:00:00` | `BUY:20260814_DT6_PGYVLK_8840` | `TRADE` (4002) | `01a00100-6c00-7000-8341-41bcf39bae73` | `019ffdcb-7408-7000-8341-41bcf39bae73` |
| `2026-08-14 16:00:00` |  | `UNKNOWN` (0) | `01a00100-6c00-7000-98ba-20ee319b978b` | `01a0004f-6b8c-7000-b93c-b9c562ad26cf` |
| `2026-08-14 16:00:00` | `BUY:KL3RCZUA564` | `PENDING_NEW` (1001) | `01a00100-6c00-7000-b5fd-3d0df9f16c28` | `01a0004f-6b94-7000-b5fd-3d0df9f16c28` |

## One event, every line it was logged on

Execution `00079791199VOJO7` is one event, `…5e050f`, dated
`2026-08-14 12:46:39.743016` by its `TransactTime` and recorded first at
`2026-08-14 14:46:39.771`; `srcuuids` names the 7 lines
whose frames the walk merged into it, each joining to a
`bronze.record_keeping.log_messages.curruuid`, beside the reports it was
split out of:

| line | curruuid | msgpluginid | body |
| :---: | --- | --- | --- |
| 73 | `…7bbfeb` | `OMS_X1_TradeCapture` | `Receiving : 8=FIX.4.4|9=886|35=8|50=89525|34=40221|49=O…` |
| 74 | `…eaaa72` | `OMS_X1_TradeCapture` | `RouteMessage : CFICODE=ESVTFR|CURRENCY=CHF|EVENTTIMESTA…` |
| 75 | `…809977` | `TECH_AddFields_OMS_X1` | `After Enrichment -> ACTION=EXECUTION|AGGRESSORINDICATOR…` |
| 77 | `…7cbd6c` | `MIFID_BuySideROE_Add_Fields` | `After Enrichment -> #CFICODE=ESVTFR|#ISINCODE=CH0012221…` |
| 82 | `…28ec08` | `MIFID_BuySide_Timestamps_Add_Fields` | `Result of message post-enrichment : #CFICODE=ESVTFR|#IS…` |
| 83 | `…d734f7` | `Force_IRIS_ByPass` | `After --> #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LASTM…` |
| 91 | `…439fe5` | `EnrichmentManagerModule` | `Revert Fail: #CFICODE=ESVTFR|#ISINCODE=CH0012221716|#LA…` |

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
