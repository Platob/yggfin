# fix.bronze and fix.silver

`fix.bronze` and `fix.silver` are the typed protocol products, and they are
one shape: the same 123 columns, the same key and the same layout, declared
once on `fix_message_field`. A row in either is one *event*: a line carrying
two frames answers two messages, a bulk configuration answer one per
configuration it names, a line carrying no message none at all -- and the same
message logged again at every hop it passes restates the event it already
named rather than opening a second one. Ordinary prose, unknown keys, and
values that cannot be typed remain observable instead of disappearing.

What tells the two apart is what has been done to the row. A bronze row is the
parse's answer: what the message stated about itself, and nothing about the
message before it. A silver row is the walk's restatement of the same event:
its place in its chain, its lineage, and the clocks and state its chain folded
forward. A product reads `fix.silver` and never `fix.bronze`.

## Product contract

| property | `fix.bronze` and `fix.silver` |
| --- | --- |
| row grain | one event, however many captured lines stated it: parsed in bronze, walked in silver |
| primary key | `curruuid` |
| partition | `currunix`, Iceberg `hour` transform |
| sort order | `currunix`, then `seqnum`, then `curruuid` |
| columns | 123 with the bundled dictionary |
| the line it was read from | `sourceurl`, `rownum` and `srcuuids`; the bytes and the line's own content code are `logs.messages`' |
| event identity | `curruuid`, and `crossuuid` for the chain it stands in; both stored as the sixteen bytes they are |
| content code | `currhashcode`, over the event's facts, text, metadata and entry tree |
| lossless protocol record | `fixentries`, under the `nofixentries` that counts it |
| reviewed contract | `schemas/rekep/fix-message.json`, one document for both |

The key, the partition and the sort order are declared once, on the field
both tables are created with, and nothing else carries either mark. One
message logged at every hop is one event, so an arrival under an identity
already held replaces it; a key is scoped to its partition, so the partition
must be the event's own instant, and the row already carries it.

## What each table holds, and why bronze has no chain

A bronze row is the parse's answer.
[`parse_fix_bronze`](../pipeline/tasks/parse-fix-bronze.md) reads one window
of `logs.messages` off the capture clock, parses every frame a line carried,
and lands the rows. Nothing has walked, so `seqnum`, `prevuuid` and `prevunix`
are null and `parentuuids` is empty on every row; a message read back off a
bronze row stands at step zero. `state` and `creaunix` are what the message
stated, before any fold. `currunix` is the `SendingTime` the message stated,
else the codec's pin, `1970-01-01T00:00:00Z`: one hour, one partition, where
an undated message waits.

A silver row is the walk's restatement.
[`parse_fix_silver`](../pipeline/tasks/parse-fix-silver.md) reads `fix.bronze`
for the window off the event clock -- the rows at the pin by their
`TransactTime` instead -- reads each row back as the message that wrote it,
walks the chains, and lands the walked rows: the chain filled, `creaunix`,
`expirunix` and `state` folded forward, and a message the parse could not date
re-dated by its `TransactTime` and re-settled under a new `curruuid` in the
hour it happened in.

| column | `fix.bronze` | `fix.silver` |
| --- | --- | --- |
| `seqnum`, `prevuuid`, `prevunix` | null on every row | filled where the message follows one |
| `parentuuids` | empty on every row | the identities this event descends from, the predecessor among them |
| `creaunix`, `expirunix`, `state` | as the message stated them | folded forward along the chain |
| `currunix` | the stated `SendingTime`, else the pin | the same where the parse dated the message; its `TransactTime` where it could not |
| `curruuid` | settled over `currunix` and `currhashcode` | re-settled wherever either moved: on every row the walk re-dated, and on a dated row whose content it restated |
| `srcuuids`, `sourceurl`, `rownum` and the carried columns | the line the message was parsed out of | the same line: no walk moves provenance |

So the count of identities is the same in both tables and the set is not.
Over the bundled capture each holds 53 rows and 10 identities are the same in
both; 37 bronze rows sit at the pin and no silver row does, and of the 16
dated rows 6 take a new identity because the walk restated their content; 22
silver rows follow a step. A product needs the chain, which is why
[`build_dbt`](../pipeline/tasks/build-dbt.md) reads `fix.silver` alone.

## Complete schema

The table below lists every top-level column of both tables in storage order.
`tag` is FIX metadata; a dash marks capture or structural columns rather than
protocol fields.

| # | column | Arrow type | null | tag | purpose |
| -: | --- | --- | :---: | ---: | --- |
| 1 | `rownum` | `int64` | yes | — | line number of one line that stated the event, inside that object |
| 2 | `timestamp` | `timestamp[us, tz=UTC]` | yes | — | capture clock of that line; context, and it dates nothing |
| 3 | `timepartition` | `timestamp[us, tz=UTC]` | yes | — | the capture hour of that line, carried |
| 4 | `threadId` | `int64` | yes | — | bridge thread |
| 5 | `pluginid` | `string` | yes | — | the plugin that wrote the line, as the bridge's header brackets it |
| 6 | `level` | `string` | yes | — | log severity |
| 7 | `currunix` | `timestamp[us, tz=UTC]` | no | 65003 | When the message happened: the settled instant, UTC. |
| 8 | `creaunix` | `timestamp[us, tz=UTC]` | no | 65023 | When the message was created: what it states, else when the original was sent, else when it happened; the earliest its chain kn… |
| 9 | `prevunix` | `timestamp[us, tz=UTC]` | yes | 65021 | When the message this one follows happened, where it follows one. |
| 10 | `snapunix` | `timestamp[us, tz=UTC]` | yes | 65025 | The grid instant a walk read this message as the snapshot of; empty on every row no snapshot was taken of. |
| 11 | `expirunix` | `timestamp[us, tz=UTC]` | yes | 65053 | When the message stops being good: ExpireTime, else ValidUntilTime, ExpireDate or MaturityDate; the latest its chain knows once… |
| 12 | `sendingtime` | `timestamp[us, tz=UTC]` | yes | 52 | Time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| 13 | `origsendingtime` | `timestamp[us, tz=UTC]` | yes | 122 | Original time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") when transmitt… |
| 14 | `transacttime` | `timestamp[us, tz=UTC]` | yes | 60 | Timestamp when the business transaction represented by the message occurred. |
| 15 | `settldate` | `timestamp[us]` | yes | 64 | Specific date of trade settlement (SettlementDate) in YYYYMMDD format. |
| 16 | `tradedate` | `timestamp[us]` | yes | 75 | Indicates date of trading day. Absence of this field indicates current day (expressed in local time at place of trade). |
| 17 | `expiretime` | `timestamp[us, tz=UTC]` | yes | 126 | Time/Date of order expiration (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| 18 | `validuntiltime` | `timestamp[us, tz=UTC]` | yes | 62 | Indicates expiration time of indication message (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| 19 | `expiredate` | `timestamp[us]` | yes | 432 | Date of order expiration (last day the order can trade), always expressed in terms of the local market date. The time at which… |
| 20 | `curruuid` | `fixed_size_binary[16]` | no | 65039 | The message's identity: the UUIDv7 its instant and its code derive. |
| 21 | `crossuuid` | `fixed_size_binary[16]` | no | 65040 | The identity every message of one lifecycle shares, derived from the identifier they share; the message's own where it names no… |
| 22 | `crosscode` | `string` | yes | 65048 | The identifier every message of one lifecycle shares: OrderID, else ClOrdID, OrigClOrdID, QuoteID, QuoteReqID or MDReqID, the f… |
| 23 | `currhashcode` | `int64` | no | 65017 | The XXH3-64 of what the event states and the named FIX content behind it. |
| 24 | `crosshashcode` | `int64` | no | 65018 | The XXH3-64 of the cross code; zero where the message names none. |
| 25 | `prevuuid` | `fixed_size_binary[16]` | yes | 65022 | The identity of the message this one follows, where it follows one. |
| 26 | `seqnum` | `int64` | yes | 65042 | The message's place in its chain: how many came before it. |
| 27 | `parentuuids` | `list<...>` | yes | 65041 | The identities of the messages this one descends from, in the order it states them. |
| 28 | `srcuuids` | `list<...>` | yes | 65051 | The identities of the elements this message was read from: the text line it was parsed out of, and none for one parsed from raw… |
| 29 | `identifiers` | `map<...>` | yes | 65020 | The names this message goes by, each under the canonical name of the field that stated it, in sorted order; repeating-group mem… |
| 30 | `beginstring` | `string` | no | 8 | Identifies beginning of new message and session protocol version by means of a session profile identifier (see FIX Session Laye… |
| 31 | `msgtype` | `string` | yes | 35 | Defines message type ALWAYS THIRD FIELD IN MESSAGE. (Always unencrypted) |
| 32 | `msgseqnum` | `int64` | yes | 34 | Integer message sequence number. |
| 33 | `sendercompid` | `string` | yes | 49 | Assigned value used to identify firm sending message. |
| 34 | `targetcompid` | `string` | yes | 56 | Assigned value used to identify receiving firm. |
| 35 | `possdupflag` | `bool` | yes | 43 | Indicates possible retransmission of message with this sequence number |
| 36 | `msgdirection` | `string` | yes | 385 | Specifies the direction of the message. |
| 37 | `sourceurl` | `string` | yes | 65026 | The object this message's line was read from. |
| 38 | `msgpluginid` | `string` | yes | 65009 | The plugin that logged the line inside a bridge, as the bridge names it: the row's own msgpluginid column, never derived. |
| 39 | `msgctxid` | `string` | yes | 65008 | The message context a bridge handled the message in, as its own log names it. |
| 40 | `msgsessionid` | `string` | yes | 65032 | The session instance a bridge handled a line on, as its own row header brackets it - never what the message states about itself. |
| 41 | `symbol` | `string` | yes | 55 | Ticker symbol. Common, "human understood" representation of the security. SecurityID (48) value can be specified if no symbol e… |
| 42 | `securityid` | `string` | yes | 48 | Security identifier value of SecurityIDSource (22) type (e.g. CUSIP, SEDOL, ISIN, etc). Requires SecurityIDSource. |
| 43 | `securityidsource` | `string` | yes | 22 | Identifies class or source of the SecurityID(48) value. |
| 44 | `securitytype` | `string` | yes | 167 | Indicates type of security. Security type enumerations are grouped by Product(460) field value. NOTE: Additional values may be… |
| 45 | `securitysubtype` | `string` | yes | 762 | Sub-type qualification/identification of the SecurityType. As an example for SecurityType(167)="REPO", the SecuritySubType="Gen… |
| 46 | `securityexchange` | `string` | yes | 207 | Market used to help identify a security. |
| 47 | `exdestination` | `string` | yes | 100 | Execution destination as defined by institution when order is entered. |
| 48 | `lastmkt` | `string` | yes | 30 | Market of execution for last fill, or an indication of the market where an order was routed |
| 49 | `cficode` | `string` | yes | 461 | Indicates the type of security using ISO 10962 standard, Classification of Financial Instruments (CFI code) values. ISO 10962 i… |
| 50 | `maturitydate` | `timestamp[us]` | yes | 541 | Date of maturity. |
| 51 | `product` | `int32` | yes | 460 | Indicates the type of product the security is associated with. See also the CFICode (461) and SecurityType (167) fields. |
| 52 | `securitytradingstatus` | `int32` | yes | 326 | Identifies the trading status applicable to the security. |
| 53 | `tradsesstatus` | `int32` | yes | 340 | State of the trading session. |
| 54 | `securitystatus` | `string` | yes | 965 | Indicates the current state of the instrument. |
| 55 | `account` | `string` | yes | 1 | Account mnemonic as agreed between buy and sell sides, e.g. broker and institution or investor/intermediary and fund manager. |
| 56 | `clordid` | `string` | yes | 11 | Unique identifier for Order as assigned by the buy-side (institution, broker, intermediary etc.) (identified by SenderCompID(49… |
| 57 | `origclordid` | `string` | yes | 41 | ClOrdID (11) of the previous order (NOT the initial order of the day) as assigned by the institution, used to identify the prev… |
| 58 | `secondaryclordid` | `string` | yes | 526 | Assigned by the party which originates the order. Can be used to provide the ClOrdID (11) used by an exchange or executing syst… |
| 59 | `orderid` | `string` | yes | 37 | Unique identifier for Order as assigned by sell-side (broker, exchange, ECN). Uniqueness must be guaranteed within a single tra… |
| 60 | `secondaryorderid` | `string` | yes | 198 | Assigned by the party which accepts the order. Can be used to provide the OrderID (37) used by an exchange or executing system. |
| 61 | `execid` | `string` | yes | 17 | Unique identifier of execution message as assigned by sell-side (broker, exchange, ECN) (will be 0 (zero) for ExecType (150)=I… |
| 62 | `tradeid` | `string` | yes | 1003 | The unique ID assigned to the trade entity once it is received or matched by the exchange or central counterparty. |
| 63 | `quotereqid` | `string` | yes | 131 | Unique identifier for a QuoteRequest(35=R). |
| 64 | `quoteid` | `string` | yes | 117 | Unique identifier for quote |
| 65 | `mdreqid` | `string` | yes | 262 | Unique identifier for Market Data Request |
| 66 | `quoterespid` | `string` | yes | 693 | Message reference for Quote Response |
| 67 | `side` | `string` | yes | 54 | Side of order (see Volume : "Glossary" for value definitions) |
| 68 | `price` | `decimal128(38, 18)` | yes | 44 | Price per unit of quantity (e.g. per share) |
| 69 | `prevclosepx` | `decimal128(38, 18)` | yes | 140 | Previous closing price of security. |
| 70 | `lastpx` | `decimal128(38, 18)` | yes | 31 | Price of this (last) fill. |
| 71 | `avgpx` | `decimal128(38, 18)` | yes | 6 | Calculated average price of all fills on this order. |
| 72 | `orderqty` | `decimal128(38, 18)` | yes | 38 | Quantity ordered. This represents the number of shares for equities or par, face or nominal value for FI instruments. |
| 73 | `quantity` | `decimal128(38, 18)` | yes | 53 | Overall/total quantity (e.g. number of shares) |
| 74 | `lastqty` | `decimal128(38, 18)` | yes | 32 | Quantity (e.g. shares) bought/sold on this (last) fill. |
| 75 | `cumqty` | `decimal128(38, 18)` | yes | 14 | Total quantity (e.g. number of shares) filled. |
| 76 | `leavesqty` | `decimal128(38, 18)` | yes | 151 | Quantity open for further execution. If the OrdStatus (39) is Canceled, DoneForTheDay, Expired, Calculated, or Rejected (in whi… |
| 77 | `unitofmeasure` | `string` | yes | 996 | The unit of measure of the underlying commodity upon which the contract is based. Two groups of units of measure enumerations a… |
| 78 | `currency` | `string` | yes | 15 | Identifies currency used for price or quantity fields, depending on the asset class being traded. CurrencyCodeSource(2897) may… |
| 79 | `settlcurrency` | `string` | yes | 120 | Currency code of settlement denomination. |
| 80 | `qtytype` | `int32` | yes | 854 | Type of quantity specified in quantity field. ContractMultiplier (tag 231) is required when QtyType = 1 (Contracts). UnitOfMeas… |
| 81 | `ordtype` | `string` | yes | 40 | Order type. |
| 82 | `timeinforce` | `string` | yes | 59 | Specifies how long the order remains in effect. Absence of this field is interpreted as DAY. NOTE not applicable to CIV Orders. |
| 83 | `bidpx` | `decimal128(38, 18)` | yes | 132 | Bid price/rate |
| 84 | `bidsize` | `decimal128(38, 18)` | yes | 134 | Quantity of bid |
| 85 | `offerpx` | `decimal128(38, 18)` | yes | 133 | Offer price/rate |
| 86 | `offersize` | `decimal128(38, 18)` | yes | 135 | Quantity of offer |
| 87 | `state` | `string` | yes | 65052 | The state the message reached, ranked so the column sorts by lifecycle: OrdStatus, else ExecType, 00UNKNOWN where neither state… |
| 88 | `ordstatus` | `string` | yes | 39 | Identifies current status of order. *** SOME VALUES HAVE BEEN REPLACED - See "Replaced Features and Supported Approach" *** (se… |
| 89 | `exectype` | `string` | yes | 150 | Describes the specific ExecutionRpt (e.g. Pending Cancel) while OrdStatus(39) will always identify the current order status (e.… |
| 90 | `quotestatus` | `int32` | yes | 297 | Identifies the status of the quote acknowledgement. |
| 91 | `quoteresponselevel` | `int32` | yes | 301 | Level of Response requested from receiver of quote messages. A default value should be bilaterally agreed. |
| 92 | `quoteentryrejectreason` | `int32` | yes | 368 | Reason Quote Entry was rejected: |
| 93 | `ordrejreason` | `int32` | yes | 103 | Code to identify reason for order rejection. Note: Values 3, 4, and 5 will be used when rejecting an order due to pre-allocatio… |
| 94 | `cxlrejreason` | `int32` | yes | 102 | Code to identify reason for rejection of cancellation or modification. |
| 95 | `text` | `string` | yes | 58 | Free format text string |
| 96 | `nopartyids` | `int32` | yes | 453 | Number of PartyID (448), PartyIDSource (447), and PartyRole (452) entries |
| 97 | `parties` | `list<...>` | yes | 209321 | Repeating group below should contain unique combinations of PartyID, PartyIDSource, and PartyRole |
| 98 | `nosecurityaltid` | `int32` | yes | 454 | Number of SecurityAltID (455) entries. |
| 99 | `secaltidgrp` | `list<...>` | yes | 589472 |  |
| 100 | `notrdregtimestamps` | `int32` | yes | 768 | Number of timestamp entries. |
| 101 | `trdregtimestamps` | `list<...>` | yes | 763375 | Required if NoTrdRegTimestamps(768) > 0. |
| 102 | `bodylength` | `int32` | yes | 9 | Message length, in bytes, forward to the CheckSum field. ALWAYS SECOND FIELD IN MESSAGE. (Always unencrypted) |
| 103 | `onbehalfofcompid` | `string` | yes | 115 | Assigned value used to identify firm originating message if the message was delivered by a third party i.e. the third party fir… |
| 104 | `delivertocompid` | `string` | yes | 128 | Assigned value used to identify the firm targeted to receive the message if the message is delivered by a third party i.e. the… |
| 105 | `securedatalen` | `int32` | yes | 90 | Length of encrypted message |
| 106 | `securedata` | `binary` | yes | 91 | Actual encrypted data stream |
| 107 | `sendersubid` | `string` | yes | 50 | Assigned value used to identify specific message originator (desk, trader, etc.) |
| 108 | `senderlocationid` | `string` | yes | 142 | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) |
| 109 | `targetsubid` | `string` | yes | 57 | Assigned value used to identify specific individual or unit intended to receive message. "ADMIN" reserved for administrative me… |
| 110 | `targetlocationid` | `string` | yes | 143 | Assigned value used to identify specific message destination's location (i.e. geographic location and/or desk, trader) |
| 111 | `onbehalfofsubid` | `string` | yes | 116 | Assigned value used to identify specific message originator (i.e. trader) if the message was delivered by a third party |
| 112 | `onbehalfoflocationid` | `string` | yes | 144 | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) if the me… |
| 113 | `delivertosubid` | `string` | yes | 129 | Assigned value used to identify specific message recipient (i.e. trader) if the message is delivered by a third party |
| 114 | `delivertolocationid` | `string` | yes | 145 | Assigned value used to identify specific message recipient's location (i.e. geographic location and/or desk, trader) if the mes… |
| 115 | `possresend` | `bool` | yes | 97 | Indicates that message may contain information that has been sent under another sequence number. |
| 116 | `xmldatalen` | `int32` | yes | 212 | Length of the XmlData data block. |
| 117 | `xmldata` | `binary` | yes | 213 | Actual XML data stream (e.g. FIXML). See appropriate XML reference (e.g. FIXML). Note: may contain embedded SOH characters. |
| 118 | `signaturelength` | `int32` | yes | 93 | Number of bytes in signature field |
| 119 | `signature` | `binary` | yes | 89 | Electronic signature |
| 120 | `checksum` | `string` | yes | 10 | Three byte, simple checksum (see Volume 2: "Checksum Calculation" for description). ALWAYS LAST FIELD IN MESSAGE; i.e. serves,… |
| 121 | `metadata` | `map<...>` | yes | 65049 | What a bridge stated under its own namespaces - a `TECH.` or an `AMON.` key - each under the key as the bridge spelled it, fold… |
| 122 | `nofixentries` | `int32` | yes | 65027 | How many pairs the message carried, in arrival order. |
| 123 | `fixentries` | `list<...>` | yes | — | Every pair the message carried, in arrival order, beside what the dictionary made of it. |

### Nested columns

| column | element schema |
| --- | --- |
| `parties` | `partyid`, `partyidsource`, `partyrole`, `partyrolequalifier`, and a nested `ptyssubgrp` under its own `nopartysubids` |
| `secaltidgrp` | `securityaltid`, `securityaltidsource`, `symbolpositionnumber` |
| `trdregtimestamps` | timestamp, type, origin, manual indicator, desk attributes, information barrier, NBBO entry type, price, quantity and source |
| `identifiers` | a sorted map of canonical field name to the identifier the message states |
| `metadata` | a sorted map of the bridge's own `TECH.` and `AMON.` keys, under the spelling it gave them |
| `parentuuids` | the identities this event descends from, the predecessor among them; empty in bronze |
| `srcuuids` | the one identity this event was read from: the stored line's `curruuid` |
| `fixentries` | `tag:int32`, `name:string`, `value:string`, recursively nested `fixentries` |

Each named group column is preceded by its own counter column, which is the
scalar field the dictionary names for it. The exact nested types, column ids
and nullability are in the
[`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
contract: `curruuid` is field 20 and the identifier field, `currunix` is field
7 and the partition source, `msgtype` is field 31. The `tag` column above is
the registry's, not the contract's: an Iceberg schema carries no field
metadata.

## Source-column folding

Six raw columns lead the row: `rownum`, `timestamp`, `timepartition`,
`threadId`, `pluginid` and `level`. Four captures do not appear a second time:
a capture named after a field fills that field, so `sourceurl`, `msgctxid`,
`msgsessionid` and `msgseqnum` land in the columns of those names -- tags
65026, 65008, 65032 and 34 -- instead of leading the row. `msgseqnum` fills
`MsgSeqNum` only where the frame stated none, and a fill never overrides what
the frame itself stated. The raw contract's `curruuid` is dropped as the row's
own identity takes its name: it reaches the row as `srcuuids`, the one line
the message was parsed out of, and nowhere else.

`pluginid` rides in front because the row's own column for the plugin is
`msgpluginid` (65009), and the raw contract spells the capture as the bridge's
header brackets it. `msgpluginid` is null on every row a capture read answers;
the plugin a line names is the carried column.

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

## The row names the text; it does not hold it

`body` is not a column here, and the `currhashcode` this row carries is the
event's own, never the line's. `logs.messages` holds the exact line and the
line's own content code; this row holds `sourceurl` and `rownum`, which name
the line it was read from, and `srcuuids`, which is that line's own
`curruuid`.

The reason is the grain. A row here is an *event*, and both the bytes and the
line's code are a *line's*: a message logged at four hops is four lines -- four
different bodies, four different line codes -- and one row, so either column
would be one arrival's answer standing in for the event's. In the bundled
capture, 19 of the 53 events were stated by more than one line -- fourteen by
two, four by three, one by five -- so a carried copy would have been one
arrival's in 36% of the table.

| question | answer |
| --- | --- |
| what did the line *this row was read from* say, byte for byte? | join the one entry of `srcuuids` to `logs.messages`' `curruuid`, or `(sourceurl, rownum)` to the same two columns there |
| which line is that? | the one arrival the `curruuid` key kept; the others are restatements it folded |
| what did the *other* arrivals say? | `logs.messages` only, and it holds every one of them |
| what message did the event state? | the row itself, and `fixentries` beside it |

`sourceurl` is the dictionary's own column, so the declaration lets a message
that named no source say so; a capture read fills it on every row, along with
`rownum` and `srcuuids`.

## What a row is re-emitted from

`fixentries` and never a stored payload. `FixCodec.write_arrow_reader` and
`FixMsg.into_bytes` rebuild from the arrival record, so dropping the text
column takes nothing the wire is rebuilt from: over the bundled capture the 79
messages re-emit the same 111,593 bytes read back off the parse's row with
`body` beside it and off the stored row without it.

What they rebuild is the **message**, not the **line**. Not one of the
capture's 79 re-emissions equals a stored line: the bridge writes the header,
then `Receiving : 8=FIX.4.4|9=938|35=8|...`, and the re-emission is canonical
SOH-separated FIX in the dictionary's order. The header, the prefix, the `|`
spelling, the arrival order of the tags and the stated `9=`/`10=` framing are
the line's, and the line is in `logs.messages`.

## A market fact is FIX's own field

`Price(44)`, `OrderQty(38)` and `Quantity(53)` are columns of the row --
`price`, `orderqty` and `quantity` -- beside `lastpx`, `avgpx`, `lastqty`,
`cumqty` and `leavesqty`, and every price and quantity is
`decimal128(38, 18)`. There is no ladder beside them: no `px`, `qty`, `prevpx`
or `prevqty`, because a fact answered twice is a fact two readers disagree
about.

What a message is *about* is a trait, not a column. `FixMsg.px` and
`FixMsg.qty` are what a reader holding the message asks, and they answer off
those fields: its own price, else what it last traded, else what it averaged.
The products restate that reading in SQL, once, in `stg_fix_messages`:
`px = coalesce(price, lastpx, avgpx)`,
`qty = coalesce(orderqty, lastqty, cumqty, leavesqty)`, and the instrument
and market the same way off `symbol`, `securityid` with its source, and
`securityexchange`, else `exdestination`, else `lastmkt`.

## The settled bundle

Every message carries these columns, each non-null:

| column | what settles it |
| --- | --- |
| `beginstring` | the wire's own, which is also the version the message was read at |
| `currunix` | the `SendingTime` the message stated, else the codec's pin; the walk re-dates a pinned message by its `TransactTime` |
| `creaunix` | what the message states, else when the original was sent, else when it happened; the walk folds it forward along the chain |
| `currhashcode` | XXH3-64 over the event's facts, its text, its metadata, the stated header cells and the entry tree -- never the row's storage, so a message read back out of a row is the same message |
| `crosshashcode` | the digest of `crosscode`, the first chain identifier the message states; zero where it names none |
| `curruuid` | a UUIDv7 over `currunix` and `currhashcode` |
| `crossuuid` | the UUID over `crosshashcode` |

`snapunix` is not among them: only a walk that took a reading stamps it, so it
is null on every other row. `seqnum`, `prevuuid`, `prevunix` and `parentuuids`
are the walk's: empty on every bronze row, and on a chain's first step in
silver.

Combined with `fixentries`, this lets prose and damaged messages share one
strict table without losing what arrived.

A projection that drops any of `currunix`, `creaunix`, `currhashcode`,
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
the storage type and refuses the extension over it. Both tables are keyed on
an identity, ordered by one and merged on one, so they need the bytes a
predicate can be lowered onto -- a read filter, an ordering, and the keys a
replace takes stored rows out by. The value is the same value either way.

What the storage boundary strips is elsewhere. A semantic datatype -- a URL, a
MIC, a currency, a side, a state -- crosses Arrow as its storage type under an
extension name in the column's metadata, and a table that stored the storage
type reads back a column that no longer merges with the declaration.
`iceberg_fix_field` therefore drops that name from `sourceurl`, `side`,
`currency`, `state` and every column like them, narrows a nanosecond instant
to the microsecond an Iceberg v2 timestamp holds, and reads a `uint64` content
code as the only sixty-four-bit integer Iceberg has, which is signed: the
eight bytes are the same eight bytes and half of them read back negative.

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

That is the bronze record. `fix.bronze` holds the arrival record as the parse
transcribed it; `fix.silver` holds it as the walk read it back off the bronze
row and restated it, and the restatement is the dictionary's spelling rather
than the capture's: an entry the read cannot carry, a tag-0 pair with no
value, is dropped; a value the dictionary knows by another spelling is
respelled; a nested group is written in the order the dictionary declares its
members. Over the bundled capture that is two null tag-0 entries on one row,
`59=day` written back as `59=0` on three, and the party sub-group of three
execution reports moved to the end of its party -- where the projected
`parties` column of those three silver rows then loses it, because the
read-back refuses the nested counter and keeps what reads (the core prints
`FIX column parties: kept what reads and nulled the rest`). A line rebuilt
from a silver record is the message; one rebuilt from the bronze record is the
bytes.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 123
assert schema.field("msgtype").metadata[b"FIX:tag"] == b"35"
assert schema.field("curruuid").metadata[b"ICEBERG:primary_key"] == b"true"
assert schema.field("currunix").metadata[b"ICEBERG:partition_key"] == b"hour"
assert schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
assert "body" not in schema.names
assert schema.field("currhashcode").metadata[b"FIX:tag"] == b"65017"
```
