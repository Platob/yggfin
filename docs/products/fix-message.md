# fix.messages

`fix.messages` is the typed protocol product. One input row from
`logs.messages` produces one output row, except a bridge configuration
document, which states one message per MBean and therefore several rows.
Ordinary prose, unknown keys, and values that cannot be typed remain
observable instead of disappearing.

## Product contract

| property | value |
| --- | --- |
| row grain | one FIX-codec result; a source line states one, or several |
| primary key | `(url, rownum, msghash)` |
| partition | `timepartition`, Iceberg `hour` transform |
| columns | 114 with the bundled registry |
| exact source identity | `bodyhash` |
| parsed message identity | `msghash` |
| lossless protocol record | `nofixentries` |
| registry misses | `nounmappedfixentries` |
| reviewed contract | `schemas/rekep/fix-message.json` |

## Complete schema

The table below lists every top-level column in storage order. `tag` is FIX
metadata; a dash marks a capture or structural column rather than a protocol
field, and `counter N` marks a repeating-group column whose occurrences are
counted by tag `N` in the column before it.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `url` | `string` | no | — | source URI; primary-key member |
| 2 | `rownum` | `int64` | no | — | physical line number; primary-key member |
| 3 | `timepartition` | `timestamp[us, UTC]` | yes | — | capture hour partition |
| 4 | `threadId` | `int64` | yes | — | bridge thread identifier captured from the line header |
| 5 | `sessionUid` | `string` | yes | — | bridge session identifier captured from a message-context header |
| 6 | `seqNum` | `int64` | yes | — | bridge sequence number captured from a message-context header |
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
| 30 | `sendingtime` | `timestamp[us, UTC]` | yes | 52 | transmission time |
| 31 | `origsendingtime` | `timestamp[us, UTC]` | yes | 122 | original transmission time |
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
| 71 | `transacttime` | `timestamp[us, UTC]` | yes | 60 | business transaction time |
| 72 | `settldate` | `timestamp[us]` | yes | 64 | settlement date |
| 73 | `tradedate` | `timestamp[us]` | yes | 75 | trading date |
| 74 | `expiretime` | `timestamp[us, UTC]` | yes | 126 | order expiry |
| 75 | `ordstatus` | `fixed_size_binary[10]` | yes | 39 | current order status |
| 76 | `exectype` | `fixed_size_binary[10]` | yes | 150 | execution-report event type |
| 77 | `quotestatus` | `int32` | yes | 297 | quote status |
| 78 | `quoteresponselevel` | `int32` | yes | 301 | requested quote response level |
| 79 | `quoteentryrejectreason` | `int32` | yes | 368 | quote-entry rejection reason |
| 80 | `ordrejreason` | `int32` | yes | 103 | order rejection reason |
| 81 | `cxlrejreason` | `int32` | yes | 102 | cancel/replace rejection reason |
| 82 | `text` | `string` | yes | 58 | free-form protocol text |
| 83 | `nopartyids` | `int32` | yes | 453 | party occurrence count |
| 84 | `parties` | `list&lt;struct&gt;` | yes | counter 453 | party identifiers and roles, one occurrence per `nopartyids` |
| 85 | `nosecurityaltid` | `int32` | yes | 454 | alternate-identifier occurrence count |
| 86 | `secaltidgrp` | `list&lt;struct&gt;` | yes | counter 454 | alternate instrument identifiers, one occurrence per `nosecurityaltid` |
| 87 | `notrdregtimestamps` | `int32` | yes | 768 | regulatory-timestamp occurrence count |
| 88 | `trdregtimestamps` | `list&lt;struct&gt;` | yes | counter 768 | regulatory timestamps, one occurrence per `notrdregtimestamps` |
| 89 | `signaturelength` | `int32` | yes | 93 | signature byte length |
| 90 | `signature` | `binary` | yes | 89 | electronic signature bytes |
| 91 | `checksum` | `string` | yes | 10 | wire checksum spelling |
| 92 | `msghash` | `fixed_size_binary[16]` | no | 65000 | digest of parsed arrivals, excluding envelope tags |
| 93 | `version` | `string` | yes | 65001 | version used for translation |
| 94 | `symbolticker` | `string` | yes | 65002 | normalized cross-venue instrument key |
| 95 | `timestamp` | `timestamp[us, UTC]` | no | 65003 | capture clock, message clock, then epoch |
| 96 | `unixpartition` | `int64` | no | 65004 | hour-floor Unix seconds |
| 97 | `parentclordid` | `string` | yes | 65005 | parent client order id |
| 98 | `parentorderid` | `string` | yes | 65006 | parent venue order id |
| 99 | `sendersessionid` | `string` | yes | 65007 | session the message came from, as it states it |
| 100 | `msgctxid` | `string` | yes | 65008 | bridge message context |
| 101 | `pluginid` | `string` | yes | 65009 | plugin that logged the line, and the dialect the row is read under |
| 102 | `prevpluginid` | `string` | yes | 65010 | plugin the message came through before the one that logged it |
| 103 | `sendersessionname` | `string` | yes | 65011 | name of the session the message came from |
| 104 | `targetsessionname` | `string` | yes | 65012 | name of the session the message went to |
| 105 | `isincode` | `fixed_size_binary[12]` | yes | 65013 | resolved ISIN |
| 106 | `miccode` | `fixed_size_binary[4]` | yes | 65014 | resolved ISO 10383 MIC |
| 107 | `state` | `fixed_size_binary[10]` | yes | 65015 | normalized order lifecycle state |
| 108 | `instid` | `fixed_size_binary[16]` | yes | 65016 | instrument identity: xxh128 of market, classification, ISIN else symbol, currency |
| 109 | `id` | `fixed_size_binary[16]` | yes | 65017 | message identity: the instant closest to market impact, then an xxh3 digest of what it said |
| 110 | `persistentid` | `fixed_size_binary[16]` | yes | 65018 | order-chain identity, carried by every later message sharing one of its identifiers |
| 111 | `targetsessionid` | `string` | yes | 65019 | session the message went to, as it states it |
| 112 | `msgdirection` | `fixed_size_binary[4]` | yes | 385 | sent/received direction |
| 113 | `nofixentries` | `list&lt;struct&gt;` | yes | — | every parsed pair in arrival order |
| 114 | `nounmappedfixentries` | `list&lt;struct&gt;` | yes | — | arrival pairs no registry field explained |

### Nested columns

A repeating group is two columns: the counter the wire carried, typed `int32`,
and the group column beside it holding the occurrences as a list of structs.

| column | element schema |
| --- | --- |
| `parties` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier` |
| `secaltidgrp` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `trdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, NBBO price/quantity/source |
| `FixEntry` | `tag:int32`, `key:string`, `value:string`, recursively nested `nofixentries` |

The exact nested types, column ids and nullability are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
contract. The `tag` column above is the registry's, not the contract's: an
Iceberg schema carries no field metadata.

An entry states the tag its key resolved to, the key and value exactly as the
line spelled them, and the entries nested under it. It states no branch: the
branch a pair resolved in is one decision about the whole message, not a fact
per pair, so it is read off the message and never repeated a hundred times
down a column. See [Decode](../fix/decode.md#nothing-is-lost-at-the-end).

## Source-column folding

Nine raw columns lead the row. Raw `timestamp`, `msgCtxId` and `pluginid` do
not appear a second time: a capture column whose folded name a fixed column
already takes fills that column instead of being carried in front of it.
`seqNum` keeps its own column and also fills `msgseqnum` when tag 34 is absent.

`pluginid` is the column that changed shape: it names the plugin that logged
the line, and where that text is the name or an alias of a branch the registry
declares, it also names the dialect the row is read under — outranking the
codec's own `branch` pin. The full rule, and the capture names beside it, are
on the [Capture columns](../fix/capture.md) page.

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
tags, original key spelling, original text, ordering, and nested entries. A
value that fails typed conversion becomes null in its projected column while
remaining present here.

`nounmappedfixentries` is a view of that same record restricted to unknown
fields. It is the registry-maintenance queue: no second parse is needed to see
which venue fields remain untyped.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 114
assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
assert schema.field("parties").metadata[b"fix:counter"] == b"453"
assert schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
```
