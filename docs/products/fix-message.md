# fix.messages

`fix.messages` is the typed protocol product. A row is one *event*: a line
carrying two frames answers two messages, a bulk configuration answer one per
configuration it names, a line carrying no message none at all -- and the same
message logged again at every hop it passes restates the event it already
named rather than opening a second one. Ordinary prose, unknown keys, and
values that cannot be typed remain observable instead of disappearing.

## Product contract

| property | value |
| --- | --- |
| row grain | one settled market event, however many captured lines stated it |
| primary key | `curruuid` |
| partition | `unix`, Iceberg `hour` transform |
| sort order | `unix`, then `seqnum`, then `curruuid` |
| columns | 129 with the bundled dictionary |
| exact source bytes | not here: `logs.messages` holds them, under the `bodyhash` this row carries |
| event identity | `curruuid`, and `crossuuid` for the chain it stands in; both stored as the sixteen bytes they are |
| content code | `hashcode`, over the event's facts, text, metadata and entry tree |
| lossless protocol record | `fixentries`, under the `nofixentries` that counts it |
| reviewed contract | `schemas/rekep/fix-message.json` |

## Complete schema

The table below lists every top-level column in storage order. `tag` is FIX
metadata; a dash marks capture or structural columns rather than protocol
fields.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `rownum` | `int64` | no | — | physical line number of one line that stated the event |
| 2 | `timestamp` | `timestamp[us, tz=UTC]` | yes | — | capture clock of that line; context, and it dates nothing |
| 3 | `timepartition` | `timestamp[us, tz=UTC]` | yes | — | the capture hour of that line, carried |
| 4 | `threadId` | `int64` | yes | — | bridge thread |
| 5 | `level` | `string` | yes | — | log severity |
| 6 | `bodyhash` | `fixed_size_binary[16]` | no | — | digest of that line's exact bytes, and the handle into `logs.messages` |
| 7 | `unix` | `timestamp[us, tz=UTC]` | no | 65003 | When the message happened: the settled instant, UTC. |
| 8 | `creatunix` | `timestamp[us, tz=UTC]` | no | 65023 | When the message was created: what it states, else when the original was sent, else when it happened; the earliest its chain knows once… |
| 9 | `prevunix` | `timestamp[us, tz=UTC]` | yes | 65021 | When the message this one follows happened, where it follows one. |
| 10 | `expirunix` | `timestamp[us, tz=UTC]` | yes | 65029 | When the message stops being good: ExpireTime, else ValidUntilTime, else ExpireDate, else the instrument's MaturityDate. |
| 11 | `snapunix` | `timestamp[us, tz=UTC]` | yes | 65025 | The grid instant a walk read this message as the snapshot of; empty on every row no snapshot was taken of. |
| 12 | `recordedat` | `timestamp[us, tz=UTC]` | yes | 65028 | The instant the capture recorded this line: the line's own text timestamp, else SendingTime. |
| 13 | `sendingtime` | `timestamp[us, tz=UTC]` | yes | 52 | Time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| 14 | `origsendingtime` | `timestamp[us, tz=UTC]` | yes | 122 | Original time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") when transmitting… |
| 15 | `transacttime` | `timestamp[us, tz=UTC]` | yes | 60 | Timestamp when the business transaction represented by the message occurred. |
| 16 | `settldate` | `timestamp[us]` | yes | 64 | Specific date of trade settlement (SettlementDate) in YYYYMMDD format. |
| 17 | `tradedate` | `timestamp[us]` | yes | 75 | Indicates date of trading day. Absence of this field indicates current day (expressed in local time at place of trade). |
| 18 | `expiretime` | `timestamp[us, tz=UTC]` | yes | 126 | Time/Date of order expiration (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| 19 | `curruuid` | `fixed_size_binary[16]` | no | 65039 | The message's identity: the UUIDv7 its instant and its code derive. |
| 20 | `crossuuid` | `fixed_size_binary[16]` | no | 65040 | The identity every message of one lifecycle shares, derived from the identifier they share; the message's own where it names none. |
| 21 | `crosscode` | `string` | yes | 65048 | The identifier every message of one lifecycle shares: OrderID, else ClOrdID, OrigClOrdID, QuoteID, QuoteReqID or MDReqID, the first stated. |
| 22 | `hashcode` | `int64` | no | 65017 | The XXH3-64 of what the event states and the named FIX content behind it. |
| 23 | `crosshashcode` | `int64` | no | 65018 | The XXH3-64 of the cross code; zero where the message names none. |
| 24 | `prevuuid` | `fixed_size_binary[16]` | yes | 65022 | The identity of the message this one follows, where it follows one. |
| 25 | `seqnum` | `int64` | yes | 65042 | The message's place in its chain: how many came before it. |
| 26 | `parentuuids` | `list<...>` | yes | 65041 | The identities of the messages this one descends from, in the order it states them. |
| 27 | `identifiers` | `map<...>` | yes | 65020 | The names this message goes by, each under the canonical name of the field that stated it, in sorted order; repeating-group members are… |
| 28 | `beginstring` | `string` | no | 8 | Identifies beginning of new message and session protocol version by means of a session profile identifier (see FIX Session Layer for… |
| 29 | `msgtype` | `string` | yes | 35 | Defines message type ALWAYS THIRD FIELD IN MESSAGE. (Always unencrypted) |
| 30 | `msgseqnum` | `int64` | yes | 34 | Integer message sequence number. |
| 31 | `sendercompid` | `string` | yes | 49 | Assigned value used to identify firm sending message. |
| 32 | `targetcompid` | `string` | yes | 56 | Assigned value used to identify receiving firm. |
| 33 | `possdupflag` | `bool` | yes | 43 | Indicates possible retransmission of message with this sequence number |
| 34 | `msgdirection` | `string` | yes | 385 | Specifies the direction of the message. |
| 35 | `sourceurl` | `string` | yes | 65026 | The object this message's line was read from. |
| 36 | `pluginid` | `string` | yes | 65009 | The plugin that logged the line inside a bridge, as the bridge names it: the row's own pluginid column, never derived. |
| 37 | `msgctxid` | `string` | yes | 65008 | The message context a bridge handled the message in, as its own log names it. |
| 38 | `msgsessionid` | `string` | yes | 65032 | The session instance a bridge handled a line on, as its own row header brackets it - never what the message states about itself. |
| 39 | `symbol` | `string` | yes | 55 | Ticker symbol. Common, "human understood" representation of the security. SecurityID (48) value can be specified if no symbol exists… |
| 40 | `symbolticker` | `string` | yes | 65054 | The ticker the instrument is known by: what Symbol settles on, null where the message names none. |
| 41 | `securityid` | `string` | yes | 48 | Security identifier value of SecurityIDSource (22) type (e.g. CUSIP, SEDOL, ISIN, etc). Requires SecurityIDSource. |
| 42 | `securityidsource` | `string` | yes | 22 | Identifies class or source of the SecurityID(48) value. |
| 43 | `securitytype` | `string` | yes | 167 | Indicates type of security. Security type enumerations are grouped by Product(460) field value. NOTE: Additional values may be used by… |
| 44 | `securitysubtype` | `string` | yes | 762 | Sub-type qualification/identification of the SecurityType. As an example for SecurityType(167)="REPO", the SecuritySubType="General… |
| 45 | `securityexchange` | `string` | yes | 207 | Market used to help identify a security. |
| 46 | `cficode` | `string` | yes | 461 | Indicates the type of security using ISO 10962 standard, Classification of Financial Instruments (CFI code) values. ISO 10962 is… |
| 47 | `maturitydate` | `timestamp[us]` | yes | 541 | Date of maturity. |
| 48 | `product` | `int32` | yes | 460 | Indicates the type of product the security is associated with. See also the CFICode (461) and SecurityType (167) fields. |
| 49 | `isincode` | `string` | yes | 65013 | The instrument's ISIN: the message's own, else SecurityID or a SecurityAltID whose source is ISIN. |
| 50 | `cusipcode` | `string` | yes | 65034 | The instrument's CUSIP: the message's own, else a SecurityID or SecurityAltID whose source is CUSIP. |
| 51 | `sedolcode` | `string` | yes | 65035 | The instrument's SEDOL: the message's own, else a SecurityID or SecurityAltID whose source is SEDOL. |
| 52 | `bloombergcode` | `string` | yes | 65033 | The instrument's Bloomberg identifier: the message's own, else a SecurityID or SecurityAltID whose source is Bloomberg. |
| 53 | `miccode` | `string` | yes | 65014 | The market the message names, as an ISO 10383 MIC: the message's own, else SecurityExchange, ExDestination or LastMkt. |
| 54 | `securitytradingstatus` | `int32` | yes | 326 | Identifies the trading status applicable to the security. |
| 55 | `tradsesstatus` | `int32` | yes | 340 | State of the trading session. |
| 56 | `securitystatus` | `string` | yes | 965 | Indicates the current state of the instrument. |
| 57 | `tradable` | `bool` | yes | 65053 | Whether the instrument could be traded when the message was sent: SecurityTradingStatus, else TradSesStatus, else SecurityStatus; null… |
| 58 | `account` | `string` | yes | 1 | Account mnemonic as agreed between buy and sell sides, e.g. broker and institution or investor/intermediary and fund manager. |
| 59 | `clordid` | `string` | yes | 11 | Unique identifier for Order as assigned by the buy-side (institution, broker, intermediary etc.) (identified by SenderCompID(49) or… |
| 60 | `origclordid` | `string` | yes | 41 | ClOrdID (11) of the previous order (NOT the initial order of the day) as assigned by the institution, used to identify the previous… |
| 61 | `secondaryclordid` | `string` | yes | 526 | Assigned by the party which originates the order. Can be used to provide the ClOrdID (11) used by an exchange or executing system. |
| 62 | `orderid` | `string` | yes | 37 | Unique identifier for Order as assigned by sell-side (broker, exchange, ECN). Uniqueness must be guaranteed within a single trading day.… |
| 63 | `secondaryorderid` | `string` | yes | 198 | Assigned by the party which accepts the order. Can be used to provide the OrderID (37) used by an exchange or executing system. |
| 64 | `execid` | `string` | yes | 17 | Unique identifier of execution message as assigned by sell-side (broker, exchange, ECN) (will be 0 (zero) for ExecType (150)=I (Order… |
| 65 | `tradeid` | `string` | yes | 1003 | The unique ID assigned to the trade entity once it is received or matched by the exchange or central counterparty. |
| 66 | `quotereqid` | `string` | yes | 131 | Unique identifier for a QuoteRequest(35=R). |
| 67 | `quoteid` | `string` | yes | 117 | Unique identifier for quote |
| 68 | `quoterespid` | `string` | yes | 693 | Message reference for Quote Response |
| 69 | `side` | `string` | yes | 54 | Side of order (see Volume : "Glossary" for value definitions) |
| 70 | `px` | `decimal128(38, 18)` | yes | 65043 | The price the message is about: Price, else LastPx, else AvgPx, else the price its own side's lane quotes. |
| 71 | `prevpx` | `decimal128(38, 18)` | yes | 65051 | The price stated before this message: PrevClosePx where the message states one, else the price its predecessor in the chain stated. |
| 72 | `lastpx` | `double` | yes | 31 | Price of this (last) fill. |
| 73 | `avgpx` | `double` | yes | 6 | Calculated average price of all fills on this order. |
| 74 | `qty` | `decimal128(38, 18)` | yes | 65044 | The quantity the message is about: OrderQty, else LastQty, else CumQty, else LeavesQty, else the size its own side's lane quotes. |
| 75 | `prevqty` | `decimal128(38, 18)` | yes | 65052 | The quantity the message's predecessor in the chain stated. |
| 76 | `lastqty` | `double` | yes | 32 | Quantity (e.g. shares) bought/sold on this (last) fill. |
| 77 | `cumqty` | `double` | yes | 14 | Total quantity (e.g. number of shares) filled. |
| 78 | `leavesqty` | `double` | yes | 151 | Quantity open for further execution. If the OrdStatus (39) is Canceled, DoneForTheDay, Expired, Calculated, or Rejected (in which case… |
| 79 | `unit` | `string` | yes | 65045 | The unit the quantity is counted in: UnitOfMeasure. |
| 80 | `currency` | `string` | yes | 15 | Identifies currency used for price or quantity fields, depending on the asset class being traded. CurrencyCodeSource(2897) may be used… |
| 81 | `settlcurrency` | `string` | yes | 120 | Currency code of settlement denomination. |
| 82 | `qtytype` | `int32` | yes | 854 | Type of quantity specified in quantity field. ContractMultiplier (tag 231) is required when QtyType = 1 (Contracts). UnitOfMeasure (tag… |
| 83 | `ordtype` | `string` | yes | 40 | Order type. |
| 84 | `timeinforce` | `string` | yes | 59 | Specifies how long the order remains in effect. Absence of this field is interpreted as DAY. NOTE not applicable to CIV Orders. |
| 85 | `bidpx` | `double` | yes | 132 | Bid price/rate |
| 86 | `bidcurrency` | `string` | yes | 65030 | The currency the bid lane is quoted in: the message's own Currency, else SettlCurrency. |
| 87 | `bidunit` | `string` | yes | 65046 | The unit the bid lane's size is counted in, where the lane states it. |
| 88 | `bidsize` | `double` | yes | 134 | Quantity of bid |
| 89 | `offerpx` | `double` | yes | 133 | Offer price/rate |
| 90 | `askcurrency` | `string` | yes | 65031 | The currency the ask lane is quoted in: the message's own Currency, else SettlCurrency. |
| 91 | `askunit` | `string` | yes | 65047 | The unit the ask lane's size is counted in, where the lane states it. |
| 92 | `offersize` | `double` | yes | 135 | Quantity of offer |
| 93 | `state` | `string` | yes | 65015 | The state the order is in: OrdStatus, else ExecType, read as one lifecycle vocabulary. |
| 94 | `ordstatus` | `string` | yes | 39 | Identifies current status of order. *** SOME VALUES HAVE BEEN REPLACED - See "Replaced Features and Supported Approach" *** (see Volume… |
| 95 | `exectype` | `string` | yes | 150 | Describes the specific ExecutionRpt (e.g. Pending Cancel) while OrdStatus(39) will always identify the current order status (e.g.… |
| 96 | `quotestatus` | `int32` | yes | 297 | Identifies the status of the quote acknowledgement. |
| 97 | `quoteresponselevel` | `int32` | yes | 301 | Level of Response requested from receiver of quote messages. A default value should be bilaterally agreed. |
| 98 | `quoteentryrejectreason` | `int32` | yes | 368 | Reason Quote Entry was rejected: |
| 99 | `ordrejreason` | `int32` | yes | 103 | Code to identify reason for order rejection. Note: Values 3, 4, and 5 will be used when rejecting an order due to pre-allocation… |
| 100 | `cxlrejreason` | `int32` | yes | 102 | Code to identify reason for rejection of cancellation or modification. |
| 101 | `text` | `string` | yes | 58 | Free format text string |
| 102 | `nopartyids` | `int32` | yes | 453 | Number of PartyID (448), PartyIDSource (447), and PartyRole (452) entries |
| 103 | `parties` | `list<...>` | yes | 209321 | Repeating group below should contain unique combinations of PartyID, PartyIDSource, and PartyRole |
| 104 | `nosecurityaltid` | `int32` | yes | 454 | Number of SecurityAltID (455) entries. |
| 105 | `secaltidgrp` | `list<...>` | yes | 589472 | SecAltIDGrp |
| 106 | `notrdregtimestamps` | `int32` | yes | 768 | Number of timestamp entries. |
| 107 | `trdregtimestamps` | `list<...>` | yes | 763375 | Required if NoTrdRegTimestamps(768) > 0. |
| 108 | `bodylength` | `int32` | yes | 9 | Message length, in bytes, forward to the CheckSum field. ALWAYS SECOND FIELD IN MESSAGE. (Always unencrypted) |
| 109 | `onbehalfofcompid` | `string` | yes | 115 | Assigned value used to identify firm originating message if the message was delivered by a third party i.e. the third party firm… |
| 110 | `delivertocompid` | `string` | yes | 128 | Assigned value used to identify the firm targeted to receive the message if the message is delivered by a third party i.e. the third… |
| 111 | `securedatalen` | `int32` | yes | 90 | Length of encrypted message |
| 112 | `securedata` | `binary` | yes | 91 | Actual encrypted data stream |
| 113 | `sendersubid` | `string` | yes | 50 | Assigned value used to identify specific message originator (desk, trader, etc.) |
| 114 | `senderlocationid` | `string` | yes | 142 | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) |
| 115 | `targetsubid` | `string` | yes | 57 | Assigned value used to identify specific individual or unit intended to receive message. "ADMIN" reserved for administrative messages… |
| 116 | `targetlocationid` | `string` | yes | 143 | Assigned value used to identify specific message destination's location (i.e. geographic location and/or desk, trader) |
| 117 | `onbehalfofsubid` | `string` | yes | 116 | Assigned value used to identify specific message originator (i.e. trader) if the message was delivered by a third party |
| 118 | `onbehalfoflocationid` | `string` | yes | 144 | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) if the message was… |
| 119 | `delivertosubid` | `string` | yes | 129 | Assigned value used to identify specific message recipient (i.e. trader) if the message is delivered by a third party |
| 120 | `delivertolocationid` | `string` | yes | 145 | Assigned value used to identify specific message recipient's location (i.e. geographic location and/or desk, trader) if the message was… |
| 121 | `possresend` | `bool` | yes | 97 | Indicates that message may contain information that has been sent under another sequence number. |
| 122 | `xmldatalen` | `int32` | yes | 212 | Length of the XmlData data block. |
| 123 | `xmldata` | `binary` | yes | 213 | Actual XML data stream (e.g. FIXML). See appropriate XML reference (e.g. FIXML). Note: may contain embedded SOH characters. |
| 124 | `signaturelength` | `int32` | yes | 93 | Number of bytes in signature field |
| 125 | `signature` | `binary` | yes | 89 | Electronic signature |
| 126 | `checksum` | `string` | yes | 10 | Three byte, simple checksum (see Volume 2: "Checksum Calculation" for description). ALWAYS LAST FIELD IN MESSAGE; i.e. serves, with the… |
| 127 | `metadata` | `map<...>` | yes | 65049 | What a bridge stated under its own namespaces - a `TECH.` or an `AMON.` key - each under the key as the bridge spelled it, folded, in… |
| 128 | `nofixentries` | `int32` | yes | 65027 | How many pairs the message carried, in arrival order. |
| 129 | `fixentries` | `list<...>` | yes | — | Every pair the message carried, in arrival order, beside what the dictionary made of it. |

### Nested columns

| column | element schema |
| --- | --- |
| `parties` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier`, and a nested `ptyssubgrp` |
| `secaltidgrp` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `trdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, NBBO price/quantity/source |
| `identifiers` | a sorted map of canonical field name to the identifier the message states |
| `metadata` | a sorted map of the bridge's own `TECH.` and `firm.` keys, under the spelling it gave them |
| `parentuuids` | the identities this event descends from, the predecessor among them |
| `fixentries` | `tag:int32`, `name:string`, `value:string`, recursively nested `fixentries` |

Each named group column is preceded by its own counter column, which is the
scalar field the dictionary names for it. The exact nested types, column ids
and nullability are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
contract. The `tag` column above is the registry's, not the contract's: an
Iceberg schema carries no field metadata.

## Source-column folding

Seven raw columns lead the row. The other five captures do not appear a second
time: a capture named after a field fills that field, so `sourceurl`,
`msgctxid`, `pluginid`, `msgsessionid` and `msgseqnum` land in the columns of
those names -- tags 65026, 65008, 65009, 65032 and 34 -- instead of leading
the row. `msgseqnum` fills `MsgSeqNum` only where the frame stated none, and a
fill never overrides what the frame itself stated.

`msgsessionid` is the session *instance* the bridge handled the line on, as its
own row header brackets it, and not what the message says about the
counterparty session it names: two connections to one counterparty are two
instances, so they are two facts.

`timestamp` leads the row as capture context and dates nothing at all. A bridge
spells that clock the way a log spells one, not the way `SendingTime` is
spelled, and the same message logged at three hops carries three of them -- so
a line clock that dated a message would make every hop a different event.
Neither door offers it to the codec and neither reads the instant the parse
ran, so the identity is the same on every replay.

## The row references the text; it does not repeat it

`body` is not a column here. `logs.messages` holds the exact bytes, keyed on
the digest of them, and this row carries that digest and nothing else of the
text.

The reason is the grain. A row here is an *event*, and bytes are a *line's*: a
message logged at four hops is four lines with four different bodies and one
row, so a copy of any one of them would be one arrival's bytes presented as the
event's. In the bundled capture, 17 of the 53 events were stated by more than
one digest — twelve by two, four by three, one by four — so a stored copy would
have been wrong about 32% of the table.

What `bodyhash` buys and what it does not:

| question | answer |
| --- | --- |
| what did *this* row's line say, byte for byte? | join `bodyhash` to `logs.messages`; it resolves to exactly one row |
| which line is that? | the first arrival — the one the key folded the others onto |
| what did the *other* arrivals say? | `logs.messages` only. 23 of the capture's digests are reachable there and not from here |
| what message did the event state? | the row itself, and `fixentries` beside it |

## What a row is re-emitted from

`fixentries` and never a stored payload. `FixCodec.write_arrow_reader` and
`FixMsg.into_bytes` rebuild from the arrival record, so dropping `body` takes
nothing the wire is rebuilt from: over the bundled capture the two re-emit the
same 111,908 bytes with the column and without it, and a row that lost
`fixentries` is refused outright.

What they rebuild is the **message**, not the **line**. Not one of the
capture's 79 re-emissions equals the original body: the bridge writes
`Receiving : 8=FIX.4.4|9=938|35=8|...` and the re-emission is canonical
SOH-separated FIX in the dictionary's order. The prefix, the `|` spelling, the
arrival order of the tags and the stated `9=`/`10=` framing are the line's, and
the line is in `logs.messages`.

## One fact, one column

`Price(44)`, `OrderQty(38)` and `Quantity(53)` have no columns of their own.
They are the same facts as `px` and `qty`, which is what a row would otherwise
carry twice, so they are read and written *through* them: `px` is what the
message is about -- its own price, else what it last traded, else what it
averaged, else its own side's lane -- and `qty` is what it orders, else what it
last traded, else the lane's size.

How much is done and how much is left are deliberately not on that ladder:
`cumqty` and `leavesqty` together are the quantity ordered, and the dictionary
already says so as `orderqty`'s derivation. `prevpx` and `prevqty` are what the
previous step in the chain settled on, filled by the walk.

The price and quantity ladders are exact decimals inside the event and
`decimal128(38, 18)` in the column; FIX's own `lastpx`, `avgpx`, `lastqty`,
`cumqty` and `leavesqty` are the `float64` the dictionary types them as.

## The settled bundle

Every message carries these columns, each non-null:

| column | what settles it |
| --- | --- |
| `beginstring` | the wire's own, else the version the message was read at |
| `unix` | the stated instant, else `TransactTime`, else `SendingTime`, else the codec's pin |
| `creatunix` | what the message states, else when the original was sent, else when it happened |
| `hashcode` | XXH3-64 over the event's facts, its text, its metadata, the stated header cells and the entry tree -- never the row's storage, so a message read back out of a row is the same message |
| `crosshashcode` | the digest of `crosscode`, the first chain identifier the message states |
| `curruuid` | a UUIDv7 over `unix` and `hashcode` |
| `crossuuid` | the UUID over `crosshashcode` |

`snapunix` is not among them: only a walk that took a reading stamps it, so it
is null on every other row. `seqnum`, `prevuuid`, `prevunix`, `prevpx` and
`prevqty` are the walk's too, and a chain's first step has none of them.

Combined with `bodyhash` and `fixentries`, this lets prose and damaged messages
share one strict table without losing what arrived.

A projection that drops any of `unix`, `creatunix`, `hashcode`,
`crosshashcode`, `curruuid`, `crossuuid` or `beginstring` is not a replayable
FIX row: what is left cannot be dated, named or identified again from its own
bytes, and there is no key left to replace it on.

## Why an identity is stored as bytes

An identity here is sixteen ordered bytes and nothing else: `curruuid` is a
UUIDv7 over the instant the event settled on and the code of its content, and
`crossuuid` is the UUID over the chain's digest. They reach Arrow as
`fixed_size_binary[16]` and Iceberg as `fixed[16]`.

Iceberg would store a `uuid` and hand it back as `extension<arrow.uuid>`, which
would be the better type but for one thing: Arrow sorts, compares and hashes
the storage type and refuses the extension over it. This table is keyed on an
identity, ordered by one and merged on one, so it needs the bytes a predicate
can be lowered onto -- a read filter, an ordering, and the keys a replace takes
stored rows out by. The value is the same value either way.

What the storage boundary strips is elsewhere. A semantic datatype -- a URL, an
ISIN, a MIC, a currency, a side -- crosses Arrow as its storage type under an
extension name in the column's metadata, and a table that stored the storage
type reads back a column that no longer merges with the declaration.
`iceberg_fix_field` therefore drops that name from `sourceurl`, `isincode`,
`miccode` and every column like them, narrows a nanosecond instant to the
microsecond an Iceberg v2 timestamp holds, and reads a `uint64` content code as
the only sixty-four-bit integer Iceberg has, which is signed: the eight bytes
are the same eight bytes and half of them read back negative.

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

assert len(schema) == 129
assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
assert schema.field("curruuid").metadata[b"iceberg:primary_key"] == b"true"
assert schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
assert "body" not in schema.names
```
