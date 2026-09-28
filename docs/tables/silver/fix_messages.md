# silver.record_keeping.fix_messages

One row per FIX event, walked: the chain it belongs to, the step it stands at, the lines it was logged on, and the state, creation and expiry its chain folded forward.

| | |
| --- | --- |
| written by | [`parse_fix_messages_refined`](../../tasks/parse-fix-messages-refined.md) |
| key | `curruuid` |
| partitioned by | `hour(currunix)` |
| sorted by | `currunix`, `seqnum`, `curruuid` |
| columns | 133 |
| Iceberg contract | `schemas/silver/record_keeping/fix_messages.json` |
| dbt source | `{{ source('silver', 'fix_messages') }}`, `schemas/silver/schema.yml` |
| sample rows | [silver.record_keeping.fix_messages](../../samples/silver/fix_messages.md) |

## Columns

| column | type | required | description |
| --- | --- | :---: | --- |
| `currunix` | `timestamptz` | yes | When the event happened: the settled instant, UTC. |
| `execunix` | `timestamptz` |  | When the message's execution happened: ExecutionTimestamp, an execution TrdRegTimestamp, a proprietary EventTimestamp, or a trade's TransactTime, the first one stated; an execution report stating none executed at its currunix; the latest its chain reached once followed. |
| `recdunix` | `timestamptz` |  | When the message was recorded by its carrier, where the carrier states one; the earliest its statements know. |
| `creaunix` | `timestamptz` | yes | When the message was created: what it states, else when the original was sent, else when it happened; the earliest its chain knows once followed. |
| `prevunix` | `timestamptz` |  | When the event this one follows happened, where it follows one. |
| `snapunix` | `timestamptz` |  | The grid instant a walk read this event as the snapshot of; empty on every row no snapshot was taken of. |
| `exprunix` | `timestamptz` |  | When the message stops being good: ExpireTime, else ValidUntilTime, ExpireDate or MaturityDate; a newer explicit deadline replaces the one its chain carried. |
| `sendingtime` | `timestamptz` |  | Time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| `origsendingtime` | `timestamptz` |  | Original time of message transmission (always expressed in UTC (Universal Time Coordinated, also known as "GMT") when transmitting orders as the result of a resend request. |
| `transacttime` | `timestamptz` |  | Timestamp when the business transaction represented by the message occurred. |
| `settldate` | `timestamp` |  | Specific date of trade settlement (SettlementDate) in YYYYMMDD format. |
| `tradedate` | `timestamp` |  | Indicates date of trading day. Absence of this field indicates current day (expressed in local time at place of trade). |
| `expiretime` | `timestamptz` |  | Time/Date of order expiration (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| `validuntiltime` | `timestamptz` |  | Indicates expiration time of indication message (always expressed in UTC (Universal Time Coordinated, also known as "GMT") |
| `expiredate` | `timestamp` |  | Date of order expiration (last day the order can trade), always expressed in terms of the local market date. The time at which the order expires is determined by the local market's business practices |
| `curruuid` | `fixed[16]` | yes | The event's identity: UUIDv7 ordered by millisecond and sequence, with a content payload seeded by its cross hash. |
| `crossuuid` | `fixed[16]` | yes | The identity every event of one chain shares, derived from the code they share; the event's own where it names none. |
| `crosscode` | `string` |  | The identifier every message of one lifecycle shares: OrderID, else ClOrdID, OrigClOrdID, QuoteID, QuoteReqID or MDReqID, the first stated. |
| `currhashcode` | `long` | yes | The XXH3-64 of what the event states and the named FIX content behind it. |
| `crosshashcode` | `long` | yes | The XXH3-64 of the cross code; zero where the event names none. |
| `prevuuid` | `fixed[16]` |  | The identity of the event this one follows, where it follows one. |
| `seqnum` | `long` |  | The event's place in its chain: how many came before it. |
| `srcuuids` | `list<fixed[16]>` |  | The identities of the elements this message was read from: the text line it was parsed out of, and none for one parsed from raw bytes. Provenance, never lineage: no walk moves it. |
| `beginstring` | `string` | yes | Identifies beginning of new message and session protocol version by means of a session profile identifier (see FIX Session Layer for details). ALWAYS FIRST FIELD IN MESSAGE. (Always unencrypted). |
| `msgtype` | `string` |  | Defines message type ALWAYS THIRD FIELD IN MESSAGE. (Always unencrypted) |
| `msgcat` | `int` |  | The business category of the message type, as the member of the marketdatakind enum: the dictionary's FIX:msgcat for the type, UNKN where it files none; a row stating one is the row's word. |
| `msgseqnum` | `long` |  | Integer message sequence number. |
| `sendercompid` | `string` |  | Assigned value used to identify firm sending message. |
| `targetcompid` | `string` |  | Assigned value used to identify receiving firm. |
| `possdupflag` | `boolean` |  | Indicates possible retransmission of message with this sequence number |
| `msgdirection` | `string` |  | Specifies the direction of the message. |
| `msgpluginid` | `string` |  | The plugin that logged the line inside a bridge, as the bridge names it: the row's own msgpluginid column, never derived. |
| `msgoriginator` | `string` |  | The plugin a message came into a bridge through, as the bridge's own log line names it: a message received from (X as ...), an execution report from X, or the plugin that logged a Receiving line. Provenance, never content. |
| `msgctxid` | `string` |  | The message context a bridge handled the message in, as its own log names it. |
| `msgsessionid` | `string` |  | The session instance a bridge handled a line on, as its own row header brackets it - never what the message states about itself. |
| `msgsesseventid` | `string` |  | The session event a bridge delivered the message as: MsgType, the session instance, the message context and MsgSeqNum joined by `:`, where all four are stated. Derived and never content: the key two observations of one delivery merge on. |
| `conversationid` | `string` |  | The conversation a bridge filed the message under, as stated: a CONVERSATIONID field, else the {conversationId: ...} of the log line. Provenance, never content. |
| `symbol` | `string` |  | Ticker symbol. Common, "human understood" representation of the security. SecurityID (48) value can be specified if no symbol exists (e.g. non-exchange traded Collective Investment Vehicles) |
| `securityid` | `string` |  | Security identifier value of SecurityIDSource (22) type (e.g. CUSIP, SEDOL, ISIN, etc). Requires SecurityIDSource. |
| `securityidsource` | `string` |  | Identifies class or source of the SecurityID(48) value. |
| `isincode` | `string` |  | The normalized ISIN the message identifies. |
| `forexcode` | `string` |  | The currency pair the message is about, canonical CCY1/CCY2: the FOREX entry of the message's security identifiers, a view of get_securityids(); detected off Symbol(55) where the message states no other class; row-stated when written. |
| `bloombergcode` | `string` |  | The normalized Bloomberg identifier the message identifies. |
| `figicode` | `string` |  | The normalized FIGI the message identifies. |
| `miccode` | `string` |  | The normalized market MIC the message identifies: LastMkt, else ExDestination, the market a bridge's instrument key names or SecurityExchange, the first an ISO 10383 MIC or a Reuters mnemonic resolving to one; a bridge's INSTRUMENT[EXCHANGE] states it. |
| `strikepx` | `decimal(38, 18)` |  | The option strike price the message identifies: StrikePrice, as the decimal leaf; a row stating one is the row's word. |
| `securitytype` | `string` |  | Indicates type of security. Security type enumerations are grouped by Product(460) field value. NOTE: Additional values may be used by mutual agreement of the counterparties. |
| `securitysubtype` | `string` |  | Sub-type qualification/identification of the SecurityType. As an example for SecurityType(167)="REPO", the SecuritySubType="General Collateral" can be used to further specify the type of REPO. |
| `securityexchange` | `string` |  | Market used to help identify a security. |
| `exdestination` | `string` |  | Execution destination as defined by institution when order is entered. |
| `lastmkt` | `string` |  | Market of execution for last fill, or an indication of the market where an order was routed |
| `cficode` | `string` |  | Indicates the type of security using ISO 10962 standard, Classification of Financial Instruments (CFI code) values. ISO 10962 is maintained by ANNA (Association of National Numbering Agencies) acting as Registration Authority. See "Appendix 6-B FIX Fields Based Upon Other Standards". See also the Product (460) and SecurityType (167) fields. It is recommended that CFICode be used instead of SecurityType (167) for non-Fixed Income instruments. |
| `maturitydate` | `timestamp` |  | Date of maturity. |
| `product` | `int` |  | Indicates the type of product the security is associated with. See also the CFICode (461) and SecurityType (167) fields. |
| `securitytradingstatus` | `int` |  | Identifies the trading status applicable to the security. |
| `tradsesstatus` | `int` |  | State of the trading session. |
| `securitystatus` | `string` |  | Indicates the current state of the instrument. |
| `account` | `string` |  | Account mnemonic as agreed between buy and sell sides, e.g. broker and institution or investor/intermediary and fund manager. |
| `clordid` | `string` |  | Unique identifier for Order as assigned by the buy-side (institution, broker, intermediary etc.) (identified by SenderCompID(49) or OnBehalfOfCompID(115) as appropriate). Uniqueness must be guaranteed within a single trading day. Firms, particularly those which electronically submit multi-day orders, trade globally or throughout market close periods, should ensure uniqueness across days, for example by embedding a date within the ClOrdID(11) field. |
| `origclordid` | `string` |  | ClOrdID (11) of the previous order (NOT the initial order of the day) as assigned by the institution, used to identify the previous order in cancel and cancel/replace requests. |
| `secondaryclordid` | `string` |  | Assigned by the party which originates the order. Can be used to provide the ClOrdID (11) used by an exchange or executing system. |
| `orderid` | `string` |  | Unique identifier for Order as assigned by sell-side (broker, exchange, ECN). Uniqueness must be guaranteed within a single trading day. Firms which accept multi-day orders should consider embedding a date within the OrderID field to assure uniqueness across days. |
| `secondaryorderid` | `string` |  | Assigned by the party which accepts the order. Can be used to provide the OrderID (37) used by an exchange or executing system. |
| `execid` | `string` |  | Unique identifier of execution message as assigned by sell-side (broker, exchange, ECN) (will be 0 (zero) for ExecType (150)=I (Order Status)). |
| `tradeid` | `string` |  | The unique ID assigned to the trade entity once it is received or matched by the exchange or central counterparty. |
| `quotereqid` | `string` |  | Unique identifier for a QuoteRequest(35=R). |
| `quoteid` | `string` |  | Unique identifier for quote |
| `mdreqid` | `string` |  | Unique identifier for Market Data Request |
| `quoterespid` | `string` |  | Message reference for Quote Response |
| `side` | `int` |  | Side of order (see Volume : "Glossary" for value definitions) |
| `price` | `decimal(38, 18)` |  | Price per unit of quantity (e.g. per share) |
| `prevclosepx` | `decimal(38, 18)` |  | Previous closing price of security. |
| `lastpx` | `decimal(38, 18)` |  | Price of this (last) fill. |
| `avgpx` | `decimal(38, 18)` |  | Calculated average price of all fills on this order. |
| `orderqty` | `decimal(38, 18)` |  | Quantity ordered. This represents the number of shares for equities or par, face or nominal value for FI instruments. |
| `quantity` | `decimal(38, 18)` |  | Overall/total quantity (e.g. number of shares) |
| `lastqty` | `decimal(38, 18)` |  | Quantity (e.g. shares) bought/sold on this (last) fill. |
| `cumqty` | `decimal(38, 18)` |  | Total quantity (e.g. number of shares) filled. |
| `leavesqty` | `decimal(38, 18)` |  | Quantity open for further execution. If the OrdStatus (39) is Canceled, DoneForTheDay, Expired, Calculated, or Rejected (in which case the order is no longer active) then LeavesQty could be 0, otherwise LeavesQty = OrderQty (38) - CumQty (14). |
| `unitofmeasure` | `string` |  | The unit of measure of the underlying commodity upon which the contract is based. Two groups of units of measure enumerations are supported. |
| `currency` | `string` |  | Identifies currency used for price or quantity fields, depending on the asset class being traded. CurrencyCodeSource(2897) may be used to disambiguate the code source scheme used, and ISO 4217 is the default scheme if absent. |
| `settlcurrency` | `string` |  | Currency code of settlement denomination. |
| `qtytype` | `int` |  | Type of quantity specified in quantity field. ContractMultiplier (tag 231) is required when QtyType = 1 (Contracts). UnitOfMeasure (tag 996) and TimeUnit (tag 997) are required when QtyType = 2 (Units of Measure per Time Unit). |
| `ordtype` | `string` |  | Order type. |
| `timeinforce` | `string` |  | Specifies how long the order remains in effect. Absence of this field is interpreted as DAY. NOTE not applicable to CIV Orders. |
| `bidpx` | `decimal(38, 18)` |  | Bid price/rate |
| `bidsize` | `decimal(38, 18)` |  | Quantity of bid |
| `offerpx` | `decimal(38, 18)` |  | Offer price/rate |
| `offersize` | `decimal(38, 18)` |  | Quantity of offer |
| `lastspotrate` | `decimal(38, 18)` |  | F/X spot rate. |
| `lastforwardpoints` | `decimal(38, 18)` |  | F/X forward points added to LastSpotRate(194). May be a negative value. Expressed in decimal form. For example, 61.99 points is expressed and sent as 0.006199. |
| `bidspotrate` | `decimal(38, 18)` |  | Bid F/X spot rate. |
| `bidforwardpoints` | `decimal(38, 18)` |  | Bid F/X forward points added to spot rate. May be a negative value. |
| `offerspotrate` | `decimal(38, 18)` |  | Offer F/X spot rate. |
| `offerforwardpoints` | `decimal(38, 18)` |  | Offer F/X forward points added to spot rate. May be a negative value. |
| `state` | `int` |  | The state the message reached, as the code of a lifecycle-sorted enum: the first of OrdStatus, ExecType, ExecAckStatus, TrdRptStatus, QuoteStatus, AllocStatus, ConfirmStatus, AffirmStatus, MassActionResponse or MassCancelResponse that states one, else what its message type asks for, UNKNOWN where none does; the furthest its chain knows once followed. |
| `ordstatus` | `string` |  | Identifies current status of order. *** SOME VALUES HAVE BEEN REPLACED - See "Replaced Features and Supported Approach" *** (see Volume : "Glossary" for value definitions) |
| `exectype` | `string` |  | Describes the specific ExecutionRpt (e.g. Pending Cancel) while OrdStatus(39) will always identify the current order status (e.g. Partially Filled). |
| `quotestatus` | `int` |  | Identifies the status of the quote acknowledgement. |
| `quoteresponselevel` | `int` |  | Level of Response requested from receiver of quote messages. A default value should be bilaterally agreed. |
| `quoteentryrejectreason` | `int` |  | Reason Quote Entry was rejected: |
| `ordrejreason` | `int` |  | Code to identify reason for order rejection. Note: Values 3, 4, and 5 will be used when rejecting an order due to pre-allocation information errors. |
| `cxlrejreason` | `int` |  | Code to identify reason for rejection of cancellation or modification. |
| `text` | `string` |  | Free format text string |
| `nopartyids` | `int` |  | Number of PartyID (448), PartyIDSource (447), and PartyRole (452) entries |
| `parties` | `list<struct<partyid: string, partyidsource: string, partyrole: int, partyrolequalifier: int, nopartysubids: int, partysubids: list<struct<partysubid: string, partysubidtype: int>>>>` |  | Repeating group below should contain unique combinations of PartyID, PartyIDSource, and PartyRole |
| `nosecurityaltid` | `int` |  | Number of SecurityAltID (455) entries. |
| `secaltids` | `list<struct<securityaltid: string, securityaltidsource: string, symbolpositionnumber: int>>` |  |  |
| `notrdregtimestamps` | `int` |  | Number of timestamp entries. |
| `trdregtimestamps` | `list<struct<trdregtimestamp: timestamptz, trdregtimestamptype: int, trdregtimestamporigin: string, trdregtimestampmanualindicator: boolean, desktype: string, desktypesource: int, deskorderhandlinginst: string, informationbarrierid: string, nbboentrytype: int, nbboprice: decimal(38, 18), nbboqty: decimal(38, 18), nbbosource: int>>` |  | Required if NoTrdRegTimestamps(768) > 0. |
| `noregulatorytradeids` | `int` |  | Number of regulatory IDs in the repeating group. |
| `regulatorytradeids` | `list<struct<regulatorytradeid: string, regulatorytradeidsource: string, regulatorytradeidevent: int, regulatorytradeidtype: int, regulatorylegrefid: string, regulatorytradeidscope: int>>` |  | Required if NoRegulatoryTradeIDs(1907) > 0. |
| `bodylength` | `int` |  | Message length, in bytes, forward to the CheckSum field. ALWAYS SECOND FIELD IN MESSAGE. (Always unencrypted) |
| `onbehalfofcompid` | `string` |  | Assigned value used to identify firm originating message if the message was delivered by a third party i.e. the third party firm identifier would be delivered in the SenderCompID field and the firm originating the message in this field. |
| `delivertocompid` | `string` |  | Assigned value used to identify the firm targeted to receive the message if the message is delivered by a third party i.e. the third party firm identifier would be delivered in the TargetCompID (56) field and the ultimate receiver firm ID in this field. |
| `securedatalen` | `int` |  | Length of encrypted message |
| `securedata` | `binary` |  | Actual encrypted data stream |
| `sendersubid` | `string` |  | Assigned value used to identify specific message originator (desk, trader, etc.) |
| `senderlocationid` | `string` |  | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) |
| `targetsubid` | `string` |  | Assigned value used to identify specific individual or unit intended to receive message. "ADMIN" reserved for administrative messages not intended for a specific user. |
| `targetlocationid` | `string` |  | Assigned value used to identify specific message destination's location (i.e. geographic location and/or desk, trader) |
| `onbehalfofsubid` | `string` |  | Assigned value used to identify specific message originator (i.e. trader) if the message was delivered by a third party |
| `onbehalfoflocationid` | `string` |  | Assigned value used to identify specific message originator's location (i.e. geographic location and/or desk, trader) if the message was delivered by a third party |
| `delivertosubid` | `string` |  | Assigned value used to identify specific message recipient (i.e. trader) if the message is delivered by a third party |
| `delivertolocationid` | `string` |  | Assigned value used to identify specific message recipient's location (i.e. geographic location and/or desk, trader) if the message was delivered by a third party |
| `possresend` | `boolean` |  | Indicates that message may contain information that has been sent under another sequence number. |
| `xmldatalen` | `int` |  | Length of the XmlData data block. |
| `xmldata` | `binary` |  | Actual XML data stream (e.g. FIXML). See appropriate XML reference (e.g. FIXML). Note: may contain embedded SOH characters. |
| `signaturelength` | `int` |  | Number of bytes in signature field |
| `signature` | `binary` |  | Electronic signature |
| `checksum` | `string` |  | Three byte, simple checksum (see Volume 2: "Checksum Calculation" for description). ALWAYS LAST FIELD IN MESSAGE; i.e. serves, with the trailing <SOH>, as the end-of-message delimiter. Always defined as three characters. (Always unencrypted) |
| `metadata` | `map<string, string>` |  | What a message stated that is no field: a bridge's namespaced keys - a `TECH.` or an `AMON.` key - and every key no dictionary resolved, each under the key as it was spelled, folded, in sorted order. |
| `fixentries` | `map<string, string>` |  | Content no other column represents, keyed by each field's tag:name: a scalar's wire text, a group or a component as the JSON of what it holds, keyed the same way. |

## `state` codes

`state` stores the code of a lifecycle-sorted enum: the codes order from the
first state to the terminal ones, and a code's hundreds are its rank.
[States](../states.md) lists every member.
