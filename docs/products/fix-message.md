# fix.messages

`fix.messages` is the typed protocol product. One input row from
`logs.messages` produces one output row unless adjacent-message deduplication
is explicitly enabled. Ordinary prose, unknown keys, and values that cannot be
typed remain observable instead of disappearing.

## Product contract

| property | value |
| --- | --- |
| row grain | one FIX-codec result per raw source line |
| primary key | `(url, rownum)` |
| partition | `timepartition`, Iceberg `hour` transform |
| columns | 111 with the bundled registry |
| exact source identity | `bodyhash` |
| parsed message identity | `msghash` |
| lossless protocol record | `nofixentries` |
| registry misses | `nounmappedfixentries` |
| reviewed field | `schemas/rekep/fix-message.json` |

## Complete schema

The table below lists every top-level column in storage order. `tag` is FIX
metadata; a dash marks capture or structural columns rather than protocol
fields.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `url` | `string` | no | — | source URI; primary-key member |
| 2 | `rownum` | `int64` | no | — | physical line number; primary-key member |
| 3 | `timepartition` | `timestamp[us, UTC]` | yes | — | capture hour partition |
| 4 | `threadId` | `int64` | yes | — | bridge thread |
| 5 | `sessionUid` | `string` | yes | — | bridge session identity |
| 6 | `seqNum` | `int64` | yes | — | bridge sequence; can fill `msgseqnum` |
| 7 | `plugin` | `string` | yes | — | plugin that logged the line |
| 8 | `level` | `string` | yes | — | log severity |
| 9 | `bodyhash` | `fixed_size_binary[16]` | yes | — | digest of exact body bytes |
| 10 | `body` | `binary` | no | — | exact raw body |
| 11 | `beginstring` | `string` | no | 8 | wire or inferred FIX version marker |
| 12 | `bodylength` | `int32` | yes | 9 | stated body byte length |
| 13 | `msgtype` | `fixed_size_binary[8]` | yes | 35 | canonical message type |
| 14 | `sendercompid` | `string` | yes | 49 | sending firm |
| 15 | `targetcompid` | `string` | yes | 56 | receiving firm |
| 16 | `onbehalfofcompid` | `string` | yes | 115 | represented origin firm |
| 17 | `delivertocompid` | `string` | yes | 128 | ultimate destination firm |
| 18 | `securedatalen` | `int32` | yes | 90 | encrypted payload length |
| 19 | `securedata` | `binary` | yes | 91 | encrypted payload bytes |
| 20 | `msgseqnum` | `int64` | yes | 34 | FIX session sequence |
| 21 | `sendersubid` | `string` | yes | 50 | sender sub-identity |
| 22 | `senderlocationid` | `string` | yes | 142 | sender location |
| 23 | `targetsubid` | `string` | yes | 57 | target sub-identity |
| 24 | `targetlocationid` | `string` | yes | 143 | target location |
| 25 | `onbehalfofsubid` | `string` | yes | 116 | represented sender sub-identity |
| 26 | `onbehalfoflocationid` | `string` | yes | 144 | represented sender location |
| 27 | `delivertosubid` | `string` | yes | 129 | ultimate recipient sub-identity |
| 28 | `delivertolocationid` | `string` | yes | 145 | ultimate recipient location |
| 29 | `possdupflag` | `bool` | yes | 43 | possible retransmission |
| 30 | `possresend` | `bool` | yes | 97 | content may have been resent |
| 31 | `sendingtime` | `timestamp[us, UTC]` | yes | 52 | transmission time |
| 32 | `origsendingtime` | `timestamp[us, UTC]` | yes | 122 | original transmission time |
| 33 | `xmldatalen` | `int32` | yes | 212 | XML/data byte length |
| 34 | `xmldata` | `binary` | yes | 213 | XML or embedded bridge-row bytes |
| 35 | `account` | `string` | yes | 1 | trading account |
| 36 | `clordid` | `string` | yes | 11 | client order identity |
| 37 | `origclordid` | `string` | yes | 41 | prior client order identity |
| 38 | `secondaryclordid` | `string` | yes | 526 | secondary client order identity |
| 39 | `orderid` | `string` | yes | 37 | venue order identity |
| 40 | `secondaryorderid` | `string` | yes | 198 | secondary venue order identity |
| 41 | `execid` | `string` | yes | 17 | execution identity |
| 42 | `tradeid` | `string` | yes | 1003 | trade identity |
| 43 | `quotereqid` | `string` | yes | 131 | quote-request identity |
| 44 | `quoteid` | `string` | yes | 117 | quote identity |
| 45 | `quoterespid` | `string` | yes | 693 | quote-response identity |
| 46 | `symbol` | `string` | yes | 55 | human-readable instrument symbol |
| 47 | `securityid` | `string` | yes | 48 | instrument identifier |
| 48 | `securityidsource` | `string` | yes | 22 | identifier scheme |
| 49 | `securitytype` | `string` | yes | 167 | instrument type |
| 50 | `securitysubtype` | `string` | yes | 762 | instrument subtype |
| 51 | `securityexchange` | `fixed_size_binary[4]` | yes | 207 | instrument market MIC |
| 52 | `cficode` | `string` | yes | 461 | ISO 10962 classification |
| 53 | `maturitydate` | `timestamp[us]` | yes | 541 | maturity date |
| 54 | `product` | `int32` | yes | 460 | FIX product class |
| 55 | `side` | `fixed_size_binary[4]` | yes | 54 | buy/sell side code |
| 56 | `ordtype` | `string` | yes | 40 | order type code |
| 57 | `price` | `double` | yes | 44 | order price |
| 58 | `orderqty` | `double` | yes | 38 | ordered quantity |
| 59 | `quantity` | `double` | yes | 53 | generic total quantity |
| 60 | `qtytype` | `int32` | yes | 854 | quantity unit kind |
| 61 | `currency` | `fixed_size_binary[3]` | yes | 15 | trading currency |
| 62 | `settlcurrency` | `fixed_size_binary[3]` | yes | 120 | settlement currency |
| 63 | `bidpx` | `double` | yes | 132 | bid price |
| 64 | `offerpx` | `double` | yes | 133 | offer price |
| 65 | `bidsize` | `double` | yes | 134 | bid quantity |
| 66 | `offersize` | `double` | yes | 135 | offer quantity |
| 67 | `lastpx` | `double` | yes | 31 | last-fill price |
| 68 | `lastqty` | `double` | yes | 32 | last-fill quantity |
| 69 | `avgpx` | `double` | yes | 6 | cumulative average fill price |
| 70 | `cumqty` | `double` | yes | 14 | cumulative filled quantity |
| 71 | `leavesqty` | `double` | yes | 151 | remaining quantity |
| 72 | `transacttime` | `timestamp[us, UTC]` | yes | 60 | business transaction time |
| 73 | `settldate` | `timestamp[us]` | yes | 64 | settlement date |
| 74 | `tradedate` | `timestamp[us]` | yes | 75 | trading date |
| 75 | `expiretime` | `timestamp[us, UTC]` | yes | 126 | order expiry |
| 76 | `ordstatus` | `fixed_size_binary[10]` | yes | 39 | current order status |
| 77 | `exectype` | `fixed_size_binary[10]` | yes | 150 | execution-report event type |
| 78 | `quotestatus` | `int32` | yes | 297 | quote status |
| 79 | `quoteresponselevel` | `int32` | yes | 301 | requested quote response level |
| 80 | `quoteentryrejectreason` | `int32` | yes | 368 | quote-entry rejection reason |
| 81 | `ordrejreason` | `int32` | yes | 103 | order rejection reason |
| 82 | `cxlrejreason` | `int32` | yes | 102 | cancel/replace rejection reason |
| 83 | `text` | `string` | yes | 58 | free-form protocol text |
| 84 | `nopartyids` | `list<struct>` | yes | 453 | party identifiers and roles |
| 85 | `nosecurityaltid` | `list<struct>` | yes | 454 | alternate instrument identifiers |
| 86 | `notrdregtimestamps` | `list<struct>` | yes | 768 | regulatory timestamps |
| 87 | `signaturelength` | `int32` | yes | 93 | signature byte length |
| 88 | `signature` | `binary` | yes | 89 | electronic signature bytes |
| 89 | `checksum` | `string` | yes | 10 | wire checksum spelling |
| 90 | `msghash` | `fixed_size_binary[16]` | no | 65000 | digest of parsed arrivals, excluding envelope tags |
| 91 | `version` | `string` | yes | 65001 | version used for translation |
| 92 | `symbolticker` | `string` | yes | 65002 | normalized cross-venue instrument key |
| 93 | `timestamp` | `timestamp[us, UTC]` | no | 65003 | capture clock, message clock, then epoch |
| 94 | `unixpartition` | `int64` | no | 65004 | hour-floor Unix seconds |
| 95 | `parentclordid` | `string` | yes | 65005 | parent client order id |
| 96 | `parentorderid` | `string` | yes | 65006 | parent venue order id |
| 97 | `sessionid` | `string` | yes | 65007 | protocol session identity |
| 98 | `msgctxid` | `string` | yes | 65008 | bridge message context |
| 99 | `senderpluginid` | `string` | yes | 65009 | source plugin id |
| 100 | `targetpluginid` | `string` | yes | 65010 | destination plugin id |
| 101 | `senderpluginsession` | `string` | yes | 65011 | source plugin session |
| 102 | `targetpluginsession` | `string` | yes | 65012 | destination plugin session |
| 103 | `isincode` | `string` | yes | 65013 | resolved ISIN |
| 104 | `miccode` | `fixed_size_binary[4]` | yes | 65014 | resolved ISO 10383 MIC |
| 105 | `state` | `fixed_size_binary[10]` | yes | 65015 | normalized order lifecycle state |
| 106 | `instid` | `fixed_size_binary[16]` | yes | 65016 | instrument identity: xxh128 of market, classification, ISIN else symbol, currency |
| 107 | `id` | `fixed_size_binary[16]` | yes | 65017 | message identity: the instant closest to market impact, then an xxh3 digest of what it said |
| 108 | `persistentid` | `fixed_size_binary[16]` | yes | 65018 | order-chain identity, carried by every later message sharing one of its identifiers |
| 109 | `msgdirection` | `fixed_size_binary[4]` | yes | 385 | sent/received direction |
| 110 | `nofixentries` | `list<FixEntry>` | yes | — | every parsed pair in arrival order |
| 111 | `nounmappedfixentries` | `list<FixEntry>` | yes | — | arrival pairs no registry field explained |

### Nested columns

| column | element schema |
| --- | --- |
| `nopartyids` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier` |
| `nosecurityaltid` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `notrdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, NBBO price/quantity/source |
| `FixEntry` | `tag:int32`, `branch:int32`, `key:string`, `value:string`, recursively nested `nofixentries` |

The exact nested Arrow types and all metadata are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
snapshot.

## Source-column folding

Ten raw columns lead the row. Raw `timestamp` and `msgCtxId` do not appear a
second time: folded-name matching lets them fill fixed `timestamp` and
`msgctxid`. `seqNum` remains visible and also fills `msgseqnum` when tag 34 is
absent. `plugin` fills the sender plugin session for a sent row or the target
plugin session for a received row unless the message states that value itself.

## Required stamps

The parser guarantees these columns for every Arrow output row:

| column | fallback order |
| --- | --- |
| `beginstring` | wire value, selected version, branch/dictionary version, parser default |
| `msghash` | digest of the arrival record, including an empty record |
| `timestamp` | source timestamp, protocol clocks, Unix epoch |
| `unixpartition` | hour floor of `timestamp` |

Combined with required `url`, `rownum`, and `body`, this lets prose and damaged
messages share one strict table without losing source position.

## Arrival and unmapped records

`nofixentries` is the lossless protocol transcription. It preserves duplicate
tags, original key spelling, original text, ordering, branch digest, and nested
entries. A value that fails typed conversion becomes null in its projected
column while remaining present here.

`nounmappedfixentries` is a view of that same record restricted to unknown
fields. It is the registry-maintenance queue: no second parse is needed to see
which venue fields remain untyped.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 111
assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
assert schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
```
