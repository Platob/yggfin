# fix.messages

`fix.messages` is the typed protocol product. A row is one message and not one
line: a line carrying two frames answers two rows, a bulk configuration answer
one row per configuration it names, and a line carrying no message none at all.
Ordinary prose, unknown keys, and values that cannot be typed remain observable
instead of disappearing.

## Product contract

| property | value |
| --- | --- |
| row grain | one FIX message, from a raw source line that carried one or more |
| primary key | `(url, rownum, uuid)` |
| partition | `timepartition`, Iceberg `hour` transform |
| columns | 118 with the bundled dictionary |
| exact source identity | `bodyhash`, the line's own bytes |
| message identity | `uuid`, and `puuid` for the chain it belongs to; both stored as the sixteen bytes they are |
| lossless protocol record | `nofixentries` |
| reviewed contract | `schemas/rekep/fix-message.json` |

## Complete schema

The table below lists every top-level column in storage order. `tag` is FIX
metadata; a dash marks capture or structural columns rather than protocol
fields.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `url` | `string` | no | — | source URI; primary-key member |
| 2 | `rownum` | `int64` | no | — | physical line number; primary-key member |
| 3 | `timestamp` | `timestamp[us, tz=UTC]` | yes | — | capture clock; dates a message that states no `SendingTime` |
| 4 | `timepartition` | `timestamp[us, tz=UTC]` | yes | — | capture hour partition |
| 5 | `threadId` | `int64` | yes | — | bridge thread |
| 6 | `seqNum` | `int64` | yes | — | bridge sequence; can fill `msgseqnum` |
| 7 | `level` | `string` | yes | — | log severity |
| 8 | `bodyhash` | `fixed_size_binary[16]` | yes | — | digest of exact body bytes |
| 9 | `body` | `binary` | no | — | exact raw body |
| 10 | `beginstring` | `string` | no | 8 | wire or inferred FIX version marker |
| 11 | `bodylength` | `int32` | yes | 9 | stated body byte length |
| 12 | `msgtype` | `string` | yes | 35 | canonical message type |
| 13 | `sendercompid` | `string` | yes | 49 | sending firm |
| 14 | `targetcompid` | `string` | yes | 56 | receiving firm |
| 15 | `onbehalfofcompid` | `string` | yes | 115 | represented origin firm |
| 16 | `delivertocompid` | `string` | yes | 128 | ultimate destination firm |
| 17 | `securedatalen` | `int32` | yes | 90 | encrypted payload length |
| 18 | `securedata` | `binary` | yes | 91 | encrypted payload bytes |
| 19 | `msgseqnum` | `int64` | yes | 34 | FIX session sequence |
| 20 | `sendersubid` | `string` | yes | 50 | sender sub-identity |
| 21 | `senderlocationid` | `string` | yes | 142 | sender location |
| 22 | `targetsubid` | `string` | yes | 57 | target sub-identity |
| 23 | `targetlocationid` | `string` | yes | 143 | target location |
| 24 | `onbehalfofsubid` | `string` | yes | 116 | represented sender sub-identity |
| 25 | `onbehalfoflocationid` | `string` | yes | 144 | represented sender location |
| 26 | `delivertosubid` | `string` | yes | 129 | ultimate recipient sub-identity |
| 27 | `delivertolocationid` | `string` | yes | 145 | ultimate recipient location |
| 28 | `possdupflag` | `bool` | yes | 43 | possible retransmission |
| 29 | `possresend` | `bool` | yes | 97 | content may have been resent |
| 30 | `sendingtime` | `timestamp[us, tz=UTC]` | no | 52 | transmission time |
| 31 | `origsendingtime` | `timestamp[us, tz=UTC]` | yes | 122 | original transmission time |
| 32 | `xmldatalen` | `int32` | yes | 212 | XML/data byte length |
| 33 | `xmldata` | `binary` | yes | 213 | XML or embedded bridge-row bytes |
| 34 | `account` | `string` | yes | 1 | trading account |
| 35 | `clordid` | `string` | yes | 11 | client order identity |
| 36 | `origclordid` | `string` | yes | 41 | prior client order identity |
| 37 | `secondaryclordid` | `string` | yes | 526 | secondary client order identity |
| 38 | `orderid` | `string` | yes | 37 | venue order identity |
| 39 | `secondaryorderid` | `string` | yes | 198 | secondary venue order identity |
| 40 | `execid` | `string` | yes | 17 | execution identity |
| 41 | `tradeid` | `string` | yes | 1003 | trade identity |
| 42 | `quotereqid` | `string` | yes | 131 | quote-request identity |
| 43 | `quoteid` | `string` | yes | 117 | quote identity |
| 44 | `quoterespid` | `string` | yes | 693 | quote-response identity |
| 45 | `symbol` | `string` | yes | 55 | human-readable instrument symbol |
| 46 | `securityid` | `string` | yes | 48 | instrument identifier |
| 47 | `securityidsource` | `string` | yes | 22 | identifier scheme |
| 48 | `securitytype` | `string` | yes | 167 | instrument type |
| 49 | `securitysubtype` | `string` | yes | 762 | instrument subtype |
| 50 | `securityexchange` | `fixed_size_binary[4]` | yes | 207 | instrument market MIC |
| 51 | `cficode` | `string` | yes | 461 | ISO 10962 classification |
| 52 | `maturitydate` | `timestamp[us]` | yes | 541 | maturity date |
| 53 | `product` | `int32` | yes | 460 | FIX product class |
| 54 | `side` | `fixed_size_binary[4]` | yes | 54 | buy/sell side code |
| 55 | `ordtype` | `string` | yes | 40 | order type code |
| 56 | `price` | `double` | yes | 44 | order price |
| 57 | `orderqty` | `double` | yes | 38 | ordered quantity |
| 58 | `quantity` | `double` | yes | 53 | generic total quantity |
| 59 | `qtytype` | `int32` | yes | 854 | quantity unit kind |
| 60 | `currency` | `fixed_size_binary[3]` | yes | 15 | trading currency |
| 61 | `settlcurrency` | `fixed_size_binary[3]` | yes | 120 | settlement currency |
| 62 | `bidpx` | `double` | yes | 132 | bid price |
| 63 | `offerpx` | `double` | yes | 133 | offer price |
| 64 | `bidsize` | `double` | yes | 134 | bid quantity |
| 65 | `offersize` | `double` | yes | 135 | offer quantity |
| 66 | `lastpx` | `double` | yes | 31 | last-fill price |
| 67 | `lastqty` | `double` | yes | 32 | last-fill quantity |
| 68 | `avgpx` | `double` | yes | 6 | cumulative average fill price |
| 69 | `cumqty` | `double` | yes | 14 | cumulative filled quantity |
| 70 | `leavesqty` | `double` | yes | 151 | remaining quantity |
| 71 | `transacttime` | `timestamp[us, tz=UTC]` | yes | 60 | business transaction time |
| 72 | `settldate` | `timestamp[us]` | yes | 64 | settlement date |
| 73 | `tradedate` | `timestamp[us]` | yes | 75 | trading date |
| 74 | `expiretime` | `timestamp[us, tz=UTC]` | yes | 126 | order expiry |
| 75 | `ordstatus` | `fixed_size_binary[10]` | yes | 39 | current order status |
| 76 | `exectype` | `fixed_size_binary[10]` | yes | 150 | execution-report event type |
| 77 | `quotestatus` | `int32` | yes | 297 | quote status |
| 78 | `quoteresponselevel` | `int32` | yes | 301 | requested quote response level |
| 79 | `quoteentryrejectreason` | `int32` | yes | 368 | quote-entry rejection reason |
| 80 | `ordrejreason` | `int32` | yes | 103 | order rejection reason |
| 81 | `cxlrejreason` | `int32` | yes | 102 | cancel/replace rejection reason |
| 82 | `text` | `string` | yes | 58 | free-form protocol text |
| 83 | `nopartyids` | `int32` | yes | 453 | party identifiers and roles |
| 84 | `parties` | `list<...>` | yes | 209321 | Repeating group below should contain unique combinations of PartyID, PartyI... |
| 85 | `nosecurityaltid` | `int32` | yes | 454 | alternate instrument identifiers |
| 86 | `secaltidgrp` | `list<...>` | yes | 589472 | — |
| 87 | `notrdregtimestamps` | `int32` | yes | 768 | regulatory timestamps |
| 88 | `trdregtimestamps` | `list<...>` | yes | 763375 | Required if NoTrdRegTimestamps(768) > 0. |
| 89 | `signaturelength` | `int32` | yes | 93 | signature byte length |
| 90 | `signature` | `binary` | yes | 89 | electronic signature bytes |
| 91 | `checksum` | `string` | yes | 10 | wire checksum spelling |
| 92 | `version` | `string` | yes | 65001 | version used for translation |
| 93 | `symbolticker` | `string` | yes | 65002 | normalized cross-venue instrument key |
| 94 | `updatedat` | `timestamp[us, tz=UTC]` | no | 65003 | The settled message instant, truncated to the snapshot grid by the lifecycle. |
| 95 | `unixpartition` | `int64` | no | 65004 | hour-floor Unix seconds |
| 96 | `parentclordid` | `string` | yes | 65005 | parent client order id |
| 97 | `parentorderid` | `string` | yes | 65006 | parent venue order id |
| 98 | `sendersessionid` | `string` | yes | 65007 | The session a message came from: the message's own statement, else the sess... |
| 99 | `msgctxid` | `string` | yes | 65008 | bridge message context |
| 100 | `pluginid` | `string` | yes | 65009 | The plugin that logged the line inside a bridge, as the bridge names it: th... |
| 101 | `prevpluginid` | `string` | yes | 65010 | The plugin the message came through before the one that logged it, as the b... |
| 102 | `sendersessionname` | `string` | yes | 65011 | The name of the session a message came from, as the bridge row states it. |
| 103 | `targetsessionname` | `string` | yes | 65012 | The name of the session a message went to, as the bridge row states it. |
| 104 | `isincode` | `fixed_size_binary[12]` | yes | 65013 | resolved ISIN |
| 105 | `miccode` | `fixed_size_binary[4]` | yes | 65014 | resolved ISO 10383 MIC |
| 106 | `state` | `fixed_size_binary[10]` | yes | 65015 | normalized order lifecycle state |
| 107 | `instuuid` | `fixed_size_binary[16]` | yes | 65016 | The instrument's version-8 UUID over the xxh128 digest of its market, its c... |
| 108 | `uuid` | `fixed_size_binary[16]` | no | 65017 | The version-8 UUID of signed updatedat nanoseconds and 58 bits of the canon... |
| 109 | `puuid` | `fixed_size_binary[16]` | no | 65018 | The event chain's version-8 UUID over XXH3-128 of code alone. |
| 110 | `targetsessionid` | `string` | yes | 65019 | The session a message went to, as the message states it. |
| 111 | `altids` | `map<...>` | yes | 65020 | The identifiers this message states at its own level, keyed by canonical fi... |
| 112 | `prevtimestamp` | `timestamp[us, tz=UTC]` | yes | 65021 | The preceding message's timestamp in the selected event chain. |
| 113 | `prevuuid` | `fixed_size_binary[16]` | yes | 65022 | The preceding message's UUID in the selected event chain. |
| 114 | `createdat` | `timestamp[us, tz=UTC]` | no | 65023 | The creation instant, preserved after initial materialization. |
| 115 | `code` | `string` | no | 65024 | The exact event-chain name; empty means unknown. |
| 116 | `snapshotat` | `timestamp[us, tz=UTC]` | no | 65025 | The real event's instant, independent of the snapshot grid. |
| 117 | `msgdirection` | `string` | yes | 385 | sent/received direction |
| 118 | `nofixentries` | `list<FixEntry>` | yes | — | every parsed pair in arrival order |

### Nested columns

| column | element schema |
| --- | --- |
| `parties` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier`, and a nested `ptyssubgrp` |
| `secaltidgrp` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `trdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, NBBO price/quantity/source |
| `altids` | a sorted map of canonical field name to the identifier the message states |
| `FixEntry` | `tag:int32`, `key:string`, `value:string`, recursively nested `nofixentries` |

Each named group column is preceded by its own counter column, which is the
scalar field the dictionary names for it. The exact nested types, column ids
and nullability are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
contract. The `tag` column above is the registry's, not the contract's: an
Iceberg schema carries no field metadata.

## Source-column folding

Nine raw columns lead the row. `senderSessionId`, `msgCtxId` and `pluginid` do
not appear a second time: a capture named after a field fills that field, so
they land in `sendersessionid`, `msgctxid` and `pluginid` instead of leading
the row. `seqNum` remains visible and also fills `msgseqnum` when tag 34 is
absent. A fill never overrides what the frame itself stated.

`timestamp` leads the row as capture context, and is offered to the codec a
second time as the message's `SendingTime`: a message that stated none of its
own is dated by the instant its line was captured, so the identity computed
from that clock is the same on every replay.

## The settled bundle

Every message carries these columns, each non-null:

| column | what settles it |
| --- | --- |
| `beginstring` | the wire's own, else the version the message was read at |
| `sendingtime` | the message's own, else the capture clock the row carried, else the epoch |
| `snapshotat` | `TransactTime`, else `SendingTime` |
| `updatedat`, `createdat` | both start at `snapshotat` |
| `code` | the event-chain name; empty means unknown |
| `uuid` | of `updatedat`'s nanoseconds and the named message content |
| `puuid` | of `code` alone |
| `unixpartition` | hour floor of the message's own clock |

Combined with required `url`, `rownum` and `body`, this lets prose and damaged
messages share one strict table without losing source position.

## Why an identity is stored as bytes

A UUID reaches Arrow as the canonical `arrow.uuid` extension type, and a row
filter is lowered to Arrow's own compute kernels — which that type carries none
of. A column of it can therefore appear in no predicate at all: not a read
filter, not an ordering, and not the predicate a merge deletes by. Stored as
`fixed_size_binary[16]` the same sixteen bytes name themselves in every one of
them, which is what lets this table be filtered, overwritten, compacted and
deleted from on its own key. The Iceberg type is `fixed[16]`; read a value back
as a UUID with `uuid.UUID(bytes=...)`.

The expression API names these columns with either the raw sixteen bytes or a
`uuid.UUID`. PyIceberg's SQL-string grammar cannot name a `fixed[16]` literal,
so filter them through `EqualTo` and `In` rather than through a filter string.

## The arrival record

`nofixentries` is the lossless protocol transcription. It preserves duplicate
tags, original key spelling, original text, ordering and nested entries, and a
pair no dictionary explains is an entry of tag 0 under its raw key -- so one
record holds everything that arrived and there is no second column of misses.
A value that fails typed conversion becomes null in its projected column while
remaining present here. A row's line is rebuilt from this record and never
from the columns, which are a reading of the message rather than the message.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 118
assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
assert schema.names[-2:] == ["msgdirection", "nofixentries"]
```
