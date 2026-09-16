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
| primary key | `(sourceurl, rownum, msghash)` |
| partition | `timepartition`, Iceberg `hour` transform |
| columns | 128 with the bundled dictionary |
| exact source identity | `bodyhash`, the line's own bytes |
| message identity | `msghash`, and `msgphash` for the chain it belongs to; both stored as the sixteen bytes they are |
| lossless protocol record | `fixentries`, under the `nofixentries` that counts it |
| reviewed contract | `schemas/rekep/fix-message.json` |

## Complete schema

The table below lists every top-level column in storage order. `tag` is FIX
metadata; a dash marks capture or structural columns rather than protocol
fields.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `rownum` | `int64` | no | — | physical line number; primary-key member |
| 2 | `timestamp` | `timestamp[us, tz=UTC]` | yes | — | capture clock; the batch door offers it as `SendingTime` |
| 3 | `timepartition` | `timestamp[us, tz=UTC]` | yes | — | capture hour partition |
| 4 | `threadId` | `int64` | yes | — | bridge thread |
| 5 | `level` | `string` | yes | — | log severity |
| 6 | `bodyhash` | `fixed_size_binary[16]` | yes | — | digest of exact body bytes |
| 7 | `body` | `binary` | no | — | exact raw body |
| 8 | `updatedat` | `timestamp[us, tz=UTC]` | no | 65003 | The settled message instant, truncated to the snapshot grid by the lifecycle. |
| 9 | `prevupdatedat` | `timestamp[us, tz=UTC]` | yes | 65021 | The preceding message's updatedat in the selected event chain. |
| 10 | `createdat` | `timestamp[us, tz=UTC]` | no | 65023 | The creation instant, preserved after initial materialization. |
| 11 | `snapshotat` | `timestamp[us, tz=UTC]` | yes | 65025 | The instant a reading of this chain was taken at, ungridded; only a snapshot stamps it. |
| 12 | `recordedat` | `timestamp[us, tz=UTC]` | yes | 65028 | The instant the capture recorded this line: its own text timestamp, else SendingTime. |
| 13 | `expiredat` | `timestamp[us, tz=UTC]` | yes | 65029 | The instant the message stops being good: ExpireTime, ValidUntilTime, ExpireDate, else MaturityDate. |
| 14 | `instuuid` | `fixed_size_binary[16]` | yes | 65016 | The instrument's sixteen bytes over its market, its classification, its ISIN or symbol, and its currency. |
| 15 | `msghash` | `fixed_size_binary[16]` | no | 65017 | The message's sixteen bytes: signed `updatedat` nanoseconds, then the XXH64 of its named content. |
| 16 | `msgphash` | `fixed_size_binary[16]` | no | 65018 | The event chain's sixteen bytes: the big-endian XXH3-128 of `code` alone. |
| 17 | `prevmsghash` | `fixed_size_binary[16]` | yes | 65022 | The preceding message's sixteen identity bytes in the selected event chain. |
| 18 | `code` | `string` | no | 65024 | The exact event-chain name; empty means unknown. |
| 19 | `version` | `string` | yes | 65001 | version used for translation |
| 20 | `symbolticker` | `string` | yes | 65002 | normalized cross-venue instrument key |
| 21 | `parentclordid` | `string` | yes | 65005 | parent client order id |
| 22 | `parentorderid` | `string` | yes | 65006 | parent venue order id |
| 23 | `sendersessionid` | `string` | yes | 65007 | The session a message came from, as the message states it. |
| 24 | `msgctxid` | `string` | yes | 65008 | The message context a bridge handled the message in, as its own log names it. |
| 25 | `pluginid` | `string` | yes | 65009 | The plugin that logged the line inside a bridge, as the bridge names it. |
| 26 | `prevpluginid` | `string` | yes | 65010 | The plugin the message came through before the one that logged it. |
| 27 | `sendersessionname` | `string` | yes | 65011 | The name of the session a message came from, as the bridge row states it. |
| 28 | `targetsessionname` | `string` | yes | 65012 | The name of the session a message went to, as the bridge row states it. |
| 29 | `isincode` | `string` | yes | 65013 | resolved ISIN |
| 30 | `miccode` | `string` | yes | 65014 | resolved ISO 10383 MIC |
| 31 | `state` | `string` | yes | 65015 | normalized order lifecycle state |
| 32 | `targetsessionid` | `string` | yes | 65019 | The session a message went to, as the message states it. |
| 33 | `altids` | `map<...>` | yes | 65020 | The identifiers this message states at its own level, keyed by canonical field name. |
| 34 | `sourceurl` | `string` | no | 65026 | The object this message's line was read from; primary-key member |
| 35 | `bidcurrency` | `string` | yes | 65030 | The currency the bid lane is denominated in: Currency, else SettlCurrency. |
| 36 | `offercurrency` | `string` | yes | 65031 | The currency the offer lane is denominated in: Currency, else SettlCurrency. |
| 37 | `bridgesessionid` | `string` | yes | 65032 | The session instance a bridge handled the line on, as its row header brackets it. |
| 38 | `bloombergcode` | `string` | yes | 65033 | The instrument's Bloomberg identifier, from the message or a SecurityAltID. |
| 39 | `cusipcode` | `string` | yes | 65034 | The instrument's CUSIP, from the message or a SecurityAltID. |
| 40 | `sedolcode` | `string` | yes | 65035 | The instrument's SEDOL, from the message or a SecurityAltID. |
| 41 | `instids` | `struct<...>` | yes | 65036 | Every identifier this instrument is known by, filled from the columns beside it. |
| 42 | `sessionmsgid` | `string` | yes | 65037 | The session and the message context that name one message within it. |
| 43 | `sessionmsgseqid` | `string` | yes | 65038 | The session, the context and the sequence number that name one occurrence. |
| 44 | `beginstring` | `string` | no | 8 | wire or inferred FIX version marker |
| 45 | `bodylength` | `int32` | yes | 9 | stated body byte length |
| 46 | `msgtype` | `string` | yes | 35 | canonical message type |
| 47 | `sendercompid` | `string` | yes | 49 | sending firm |
| 48 | `targetcompid` | `string` | yes | 56 | receiving firm |
| 49 | `onbehalfofcompid` | `string` | yes | 115 | represented origin firm |
| 50 | `delivertocompid` | `string` | yes | 128 | ultimate destination firm |
| 51 | `securedatalen` | `int32` | yes | 90 | encrypted payload length |
| 52 | `securedata` | `binary` | yes | 91 | encrypted payload bytes |
| 53 | `msgseqnum` | `int64` | yes | 34 | FIX session sequence; filled from the capture where the frame stated none |
| 54 | `sendersubid` | `string` | yes | 50 | sender sub-identity |
| 55 | `senderlocationid` | `string` | yes | 142 | sender location |
| 56 | `targetsubid` | `string` | yes | 57 | target sub-identity |
| 57 | `targetlocationid` | `string` | yes | 143 | target location |
| 58 | `onbehalfofsubid` | `string` | yes | 116 | represented sender sub-identity |
| 59 | `onbehalfoflocationid` | `string` | yes | 144 | represented sender location |
| 60 | `delivertosubid` | `string` | yes | 129 | ultimate recipient sub-identity |
| 61 | `delivertolocationid` | `string` | yes | 145 | ultimate recipient location |
| 62 | `possdupflag` | `bool` | yes | 43 | possible retransmission |
| 63 | `possresend` | `bool` | yes | 97 | content may have been resent |
| 64 | `sendingtime` | `timestamp[us, tz=UTC]` | no | 52 | transmission time |
| 65 | `origsendingtime` | `timestamp[us, tz=UTC]` | yes | 122 | original transmission time |
| 66 | `xmldatalen` | `int32` | yes | 212 | XML/data byte length |
| 67 | `xmldata` | `binary` | yes | 213 | XML or embedded bridge-row bytes |
| 68 | `account` | `string` | yes | 1 | trading account |
| 69 | `clordid` | `string` | yes | 11 | client order identity |
| 70 | `origclordid` | `string` | yes | 41 | prior client order identity |
| 71 | `secondaryclordid` | `string` | yes | 526 | secondary client order identity |
| 72 | `orderid` | `string` | yes | 37 | venue order identity |
| 73 | `secondaryorderid` | `string` | yes | 198 | secondary venue order identity |
| 74 | `execid` | `string` | yes | 17 | execution identity |
| 75 | `tradeid` | `string` | yes | 1003 | trade identity |
| 76 | `quotereqid` | `string` | yes | 131 | quote-request identity |
| 77 | `quoteid` | `string` | yes | 117 | quote identity |
| 78 | `quoterespid` | `string` | yes | 693 | quote-response identity |
| 79 | `symbol` | `string` | yes | 55 | human-readable instrument symbol |
| 80 | `securityid` | `string` | yes | 48 | instrument identifier |
| 81 | `securityidsource` | `string` | yes | 22 | identifier scheme |
| 82 | `securitytype` | `string` | yes | 167 | instrument type |
| 83 | `securitysubtype` | `string` | yes | 762 | instrument subtype |
| 84 | `securityexchange` | `string` | yes | 207 | instrument market MIC |
| 85 | `cficode` | `string` | yes | 461 | ISO 10962 classification |
| 86 | `maturitydate` | `timestamp[us]` | yes | 541 | maturity date |
| 87 | `product` | `int32` | yes | 460 | FIX product class |
| 88 | `side` | `string` | yes | 54 | buy/sell side code |
| 89 | `ordtype` | `string` | yes | 40 | order type code |
| 90 | `price` | `double` | yes | 44 | order price |
| 91 | `orderqty` | `double` | yes | 38 | ordered quantity |
| 92 | `quantity` | `double` | yes | 53 | generic total quantity |
| 93 | `qtytype` | `int32` | yes | 854 | quantity unit kind |
| 94 | `currency` | `string` | yes | 15 | trading currency |
| 95 | `settlcurrency` | `string` | yes | 120 | settlement currency |
| 96 | `bidpx` | `double` | yes | 132 | bid price |
| 97 | `offerpx` | `double` | yes | 133 | offer price |
| 98 | `bidsize` | `double` | yes | 134 | bid quantity |
| 99 | `offersize` | `double` | yes | 135 | offer quantity |
| 100 | `lastpx` | `double` | yes | 31 | last-fill price |
| 101 | `lastqty` | `double` | yes | 32 | last-fill quantity |
| 102 | `avgpx` | `double` | yes | 6 | cumulative average fill price |
| 103 | `cumqty` | `double` | yes | 14 | cumulative filled quantity |
| 104 | `leavesqty` | `double` | yes | 151 | remaining quantity |
| 105 | `transacttime` | `timestamp[us, tz=UTC]` | yes | 60 | business transaction time |
| 106 | `settldate` | `timestamp[us]` | yes | 64 | settlement date |
| 107 | `tradedate` | `timestamp[us]` | yes | 75 | trading date |
| 108 | `expiretime` | `timestamp[us, tz=UTC]` | yes | 126 | order expiry |
| 109 | `ordstatus` | `string` | yes | 39 | current order status |
| 110 | `exectype` | `string` | yes | 150 | execution-report event type |
| 111 | `quotestatus` | `int32` | yes | 297 | quote status |
| 112 | `quoteresponselevel` | `int32` | yes | 301 | requested quote response level |
| 113 | `quoteentryrejectreason` | `int32` | yes | 368 | quote-entry rejection reason |
| 114 | `ordrejreason` | `int32` | yes | 103 | order rejection reason |
| 115 | `cxlrejreason` | `int32` | yes | 102 | cancel/replace rejection reason |
| 116 | `text` | `string` | yes | 58 | free-form protocol text |
| 117 | `nopartyids` | `int32` | yes | 453 | party identifiers and roles |
| 118 | `parties` | `list<...>` | yes | 209321 | Repeating group below should contain unique combinations of PartyID,... |
| 119 | `nosecurityaltid` | `int32` | yes | 454 | alternate instrument identifiers |
| 120 | `secaltidgrp` | `list<...>` | yes | 589472 | — |
| 121 | `notrdregtimestamps` | `int32` | yes | 768 | regulatory timestamps |
| 122 | `trdregtimestamps` | `list<...>` | yes | 763375 | Required if NoTrdRegTimestamps(768) > 0. |
| 123 | `signaturelength` | `int32` | yes | 93 | signature byte length |
| 124 | `signature` | `binary` | yes | 89 | electronic signature bytes |
| 125 | `checksum` | `string` | yes | 10 | wire checksum spelling |
| 126 | `msgdirection` | `string` | yes | 385 | sent/received direction |
| 127 | `nofixentries` | `int32` | yes | 65027 | how many pairs the message carried |
| 128 | `fixentries` | `list<...>` | yes | — | every parsed pair in arrival order |

### Nested columns

| column | element schema |
| --- | --- |
| `parties` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier`, and a nested `ptyssubgrp` |
| `secaltidgrp` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `trdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, NBBO price/quantity/source |
| `altids` | a sorted map of canonical field name to the identifier the message states |
| `instids` | `cficode`, `isincode`, `bloombergcode`, `cusipcode`, `sedolcode` |
| `fixentries` | `tagnum:int32`, `tagname:string`, `tagvalue:string`, `tagkey:string`, recursively nested `fixentries` |

Each named group column is preceded by its own counter column, which is the
scalar field the dictionary names for it. The exact nested types, column ids
and nullability are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
contract. The `tag` column above is the registry's, not the contract's: an
Iceberg schema carries no field metadata.

## Source-column folding

Seven raw columns lead the row. The other five captures do not appear a second
time: a capture named after a field fills that field, so `sourceurl`,
`msgctxid`, `pluginid`, `bridgesessionid` and `msgseqnum` land in the columns
of those names -- tags 65026, 65008, 65009, 65032 and 34 -- instead of leading
the row. `msgseqnum` fills `MsgSeqNum` only where the frame stated none, and a
fill never overrides what the frame itself stated.

`bridgesessionid` is the session *instance* the bridge handled the line on, as
its own row header brackets it. It is not `sendersessionid` (65007), which is
the counterparty session the message names: two connections to one counterparty
are two instances, so they are two columns.

`timestamp` leads the row as capture context and dates nothing by itself -- a
bridge spells that clock the way a log spells one, not the way `SendingTime` is
spelled. The batch door offers it to the codec a second time as the message's
`SendingTime`, so a message that stated none of its own is dated by the instant
its line was captured; the line door has no typed column to offer and hands
that message the codec's `UNDATED` floor instead. Neither door reads the
instant the parse ran, so the identity computed from that clock is the same on
every replay.

## The settled bundle

Every message carries these columns, each non-null:

| column | what settles it |
| --- | --- |
| `beginstring` | the wire's own, else the version the message was read at |
| `sendingtime` | the message's own, else the capture clock the batch door offered, else `UNDATED` |
| `updatedat` | the settled instant, floored to the one-second snapshot grid |
| `createdat` | the chain's first creation instant, at the resolution it happened at |
| `code` | the event-chain name; empty means unknown |
| `msghash` | over `updatedat`'s nanoseconds and the named message content |
| `msgphash` | over `code` alone |

`snapshotat` is not among them: only a snapshot stamps it, so it is null on
every other row. It and `createdat` keep the real instant that `updatedat`'s
grid rounded away.

Combined with required `sourceurl`, `rownum` and `body`, this lets prose and
damaged messages share one strict table without losing source position.

Those ten columns are also the replay bundle. A projection that drops any of
`updatedat`, `createdat`, `msghash`, `msgphash`, `code`, `sendingtime` or
`beginstring` is not a replayable FIX row: what is left cannot be dated, named
or identified again from its own bytes, and there is no key left to replace it
on.

## Why an identity is stored as bytes

An identity here is sixteen ordered bytes and nothing else: `msghash` is signed
`updatedat` nanoseconds followed by the XXH64 of the canonical named message
content, and `msgphash` is the XXH3-128 of `code`. They reach Arrow as
`fixed_size_binary[16]` and Iceberg as `fixed[16]`, so the same bytes name
themselves in every predicate a row filter is lowered to — a read filter, an
ordering, and the keys a replace takes stored rows out by — which is what lets this table
be filtered, overwritten, compacted and deleted from on its own key.

What the storage boundary strips is elsewhere. A semantic datatype — a URL, an
ISIN, a MIC, a currency — crosses Arrow as its storage type under an extension
name in the column's metadata, and a table that stored the storage type reads
back a column that no longer merges with the declaration. `iceberg_fix_field`
therefore drops that name from `sourceurl`, `isincode`, `miccode` and every
column like them, and narrows a nanosecond instant to the microsecond an
Iceberg v2 timestamp holds.

The expression API names an identity column with the raw sixteen bytes.
PyIceberg's SQL-string grammar cannot name a `fixed[16]` literal, so filter
them through `EqualTo` and `In` rather than through a filter string.

## The arrival record

`fixentries` is the lossless protocol transcription, and `nofixentries` is the
counter that counts it: the arrival record is a group named after itself, under
the counter the dictionary names for it. It preserves duplicate tags, original
key spelling, original text, ordering and nested entries, and a pair no
dictionary explains is an entry of tag 0 under its raw key -- so one record
holds everything that arrived and there is no second column of misses.
A value that fails typed conversion becomes null in its projected column while
remaining present here. A row's line is rebuilt from this record and never
from the columns, which are a reading of the message rather than the message.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 128
assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
assert schema.names[-3:] == ["msgdirection", "nofixentries", "fixentries"]
```
