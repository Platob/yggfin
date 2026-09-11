# Orders products

Orders are modeled as immutable events first and a current-state projection
second. This preserves replace chains, pending states, rejects, cancels, and
late messages without making one mutable row the audit trail.

## orders.events

One source message emits one event when it carries an order identity and an
order lifecycle fact. Messages without enough identity remain in
`fix.messages`; they are counted as rejected derivations rather than assigned
a guessed order.

### Planned schema

| column | Arrow type | null | contract |
| --- | --- | :---: | --- |
| `orderkey` | `fixed_size_binary[16]` | no | digest of stable session/account/client-or-venue identity |
| `eventkey` | `fixed_size_binary[16]` | no | digest of source position plus event index; primary key |
| `url` | `string` | no | source object |
| `rownum` | `int64` | no | source line |
| `eventindex` | `int32` | no | zero for one-event messages; supports future exploded groups |
| `msghash` | `fixed_size_binary[16]` | no | parsed source identity |
| `eventtime` | `timestamp[us, UTC]` | no | `transacttime`, then fixed market timestamp |
| `timepartition` | `timestamp[us, UTC]` | no | Iceberg day transform of `eventtime` |
| `sessionid` | `string` | yes | protocol/bridge session |
| `account` | `string` | yes | order account |
| `clordid` | `string` | yes | current client order id |
| `origclordid` | `string` | yes | immediately replaced/cancelled client order id |
| `orderid` | `string` | yes | venue order id |
| `parentclordid` | `string` | yes | parent client order id |
| `parentorderid` | `string` | yes | parent venue order id |
| `symbolticker` | `string` | yes | normalized instrument identity |
| `side` | `string` | yes | normalized side |
| `ordtype` | `string` | yes | order type |
| `price` | `double` | yes | limit/working price |
| `orderqty` | `double` | yes | ordered quantity |
| `cumqty` | `double` | yes | cumulative filled quantity stated by event |
| `leavesqty` | `double` | yes | remaining quantity stated by event |
| `avgpx` | `double` | yes | average fill price stated by event |
| `state` | `string` | yes | normalized lifecycle state |
| `exectype` | `string` | yes | event reason/type |
| `ordrejreason` | `int32` | yes | rejection code |
| `text` | `string` | yes | protocol explanation |
| `parties` | `list<struct>` | yes | copied party identities/roles |

### Identity precedence

`orderkey` should prefer a scoped `clordid`, then a scoped `orderid`; scope is
the session, sender/target pair, account, trading date, and instrument facts
available on the event. `origclordid` links replace/cancel requests to the
previous key. Parent ids express hierarchy and never replace the event's own
identity.

### State rules

- Keep both `state` and `exectype`; status and event type answer different
  questions.
- Order by `eventtime`, then source `(url, rownum, eventindex)` as a stable
  tie-breaker.
- A late event is appended and changes current state only through deterministic
  ordering.
- A reject is an event even when it never creates a live order.
- Missing cumulative values stay null; do not infer them from fills in this
  product.

## orders.current

One row is the latest settled event for one `orderkey`. It is rebuilt or
incrementally overwritten from `orders.events`, never written directly from
FIX.

| column | Arrow type | contract |
| --- | --- | --- |
| `orderkey` | `fixed_size_binary[16]` | primary key |
| `last_eventkey` | `fixed_size_binary[16]` | lineage to the winning event |
| `openedat` | `timestamp[us, UTC]` | first accepted/new event |
| `updatedat` | `timestamp[us, UTC]` | winning event time |
| `closedat` | `timestamp[us, UTC]` | terminal event time, else null |
| identifiers | same types as events | current client, venue, parent, session, account |
| instrument/order terms | same types as events | symbol, side, type, price, quantity |
| execution totals | `double` | latest stated cumulative/leaves/average values |
| `state` | `string` | latest normalized state |
| `eventcount` | `int64` | number of events folded into the row |

Acceptance requires replacement-chain fixtures, out-of-order events,
duplicate captures, rejects, terminal states, and exact reconstruction from
`orders.events` alone.
