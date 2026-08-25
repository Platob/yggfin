# FixMsg

`Message` is a source record. `FixMsg` is what that record becomes after
its payload has been parsed against a FIX dictionary.

```python
from rekep import FixCodec, FixMsg, TextFiles

source = TextFiles.from_folder(
    "s3://bucket/capture",
    pattern="*.log*",
    timezone="Europe/Paris",
)
codec = FixCodec()

for batch in source.read_arrow_reader(batch_row_size=65_536):
    parsed = FixMsg.from_message_arrow_batch(
        batch, codec, FixMsg.into_message_rules()
    )
```

`TextFile` and `TextFiles` only extract the log header and raw payload. The
`FixMsg` conversion owns protocol classification, pair parsing, dictionary
resolution, structured components, event time and market identities.
`MessageRules` itself is protocol-neutral and empty; the `FixMsg` factory
returns a fresh set of FIX event patterns for each parse.

The published protocol-neutral `Message` contract is version 1. The canonical
FIX names introduced at this boundary make the `FixMsg` contract version 3.

## Parsed record

`FixMsg` carries the generic event envelope, source provenance, the raw
payload where it is still needed, and its FIX reading. `unix` is the best event
time the message states; `runix` remains the recording time from `Message`.
`code` and `xhash` identify the lifecycle, while `codes` retains every other
identifier under stable analytical keys such as `order_id` and `cl_ord_id`.

Promoted FIX fields use their canonical registry names directly as Python and
Arrow names. Examples include:

- `MsgSeqNum`;
- `MsgType`;
- `OrigClOrdID`;
- `TransactTime`;
- `CFICode` and `AvgPx`.

There is no snake-case alias beside them. The field metadata retains the FIX
tag, datatype, description, versions and enumerated values. FIX UTC timestamps
are stored as Arrow timestamps at microsecond precision; a timestamp whose
timezone is not documented remains naive.

## Ordered residue

`kwargs` retains every field that no promoted column or structured component
took. Each list item contains:

- `tag`: the resolved FIX tag, or `0` for an unresolved rendered name;
- `key`: the canonical field name where one resolved;
- `value`: the value carried by the message;
- `comp`: its FIX component or repeating-group entry, such as
  `NoPartyIDs[0]`;
- `namespace`: a vendor prefix, such as `TECH` in `TECH.CLIENTID`.

The outer value is a list, not a map, because repeated fields and wire order
are data. A missing message has null `kwargs`; a parsed message with no residual
fields has an empty list.

Structured FIX components also use their FIX spellings:

- `Parties`, with `PartyID`, `PartyIDSource`, `PartyRole`, and a flexible
  `buffer`;
- `TrdRegTimestamps`;
- `SideTrdRegTS`.

`FixMsg.get` reads promoted columns and `kwargs` through the same registry
accessor, whether the caller names a numeric tag, canonical field name,
component path or namespace-qualified key.

## Stored categories

`fix.market`, `fix.misc` and `fix.unknown` share the same
contract. A row that cannot become a market event therefore remains queryable
without a second row model. Market rows may store `message` as null because
their ordered resolved fields carry the payload; redirected rows retain the raw
message.

The market readers consume only `fix.market`, ordered by
`(unix, MsgSeqNum, hash)`. Normalized Instrument rows use the package-owned
user-defined `MsgType` `U1` so they remain distinguishable from captured FIX
messages.
