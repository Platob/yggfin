# FixMsg

`Message` is a source record. `FixMsg` is what that record becomes after
its payload has been parsed against a FIX dictionary.

```python
from rekep import FixCodec, FixMsg, FixRegistry, TextFiles

registry = FixRegistry(cache_dir="data/fix", offline=True)

source = TextFiles.from_folder(
    "s3://bucket/capture",
    pattern="*.log*",
    timezone="Europe/Paris",
    msg_type_event_types=registry.msg_type_event_types(),
)
codec = FixCodec(registry=registry)

for batch in source.read_arrow_reader(batch_row_size=65_536):
    parsed = FixMsg.from_message_arrow_batch(batch, codec)
```

`TextFile` and `TextFiles` extract the log header, retain the raw payload, and
split structured key/value syntax once into ordered `Kwarg` values. They assign
`etype` through the registry's MsgType metadata and retain the unambiguous
`MsgType` plus a syntax-only `protocol_code`. The `FixMsg` conversion owns
dictionary resolution, structured components, event time and market identities;
it consumes those stored arguments instead of tokenizing the payload again.
Long prose and diagnostics that contain neither a discriminator nor two
delimiter-separated assignments skip tokenization entirely. Use
`exclude_msgtypes=("0", "1")` on the text reader to discard operational
traffic before argument tokenization; the empty default retains it.

The published `Message` and `FixMsg` contracts are version 1.
`kwargs` keeps a raw audit sidecar only when a typed column cannot reproduce
the source spelling, such as `0010.5000` stored as a numeric `10.5`.

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

A raw `Message.kwargs` and a resolved `FixMsg.kwargs` use the same `Kwarg`
shape. The generic parser stores `#SIDE` as `SIDE`; the leading marker is
dropped. The message discriminator is promoted to `Message.MsgType` and is not
duplicated in the residual list.

Each list item contains:

- `tag`: a numeric key already present in the payload, a later resolved FIX
  tag, or `0` while unresolved;
- `key`: the terminal spelling without a leading `#`, canonicalized by the FIX
  stage where the registry resolves it;
- `value`: the value carried by the message;
- `comp`: an indexed container prefix, such as `NoPartyIDs[0]`;
- `namespace`: a non-indexed prefix, such as `TECH` in `TECH.CLIENTID`.

The outer value is a list, not a map, because repeated fields and wire order
are data. `value` is always present; an explicitly empty value is `""`. Raw
`Message.kwargs` is always a list. A `FixMsg` carrying no recognized message
has null `kwargs`; a parsed message with no residual or audit fields has an
empty list. After resolution, `kwargs` retains every field that no promoted
column or structured component took. It also retains a promoted field's raw
text when its typed value cannot reproduce the exact wire spelling.

Structured FIX components also use their FIX spellings:

- `Parties`, with `PartyID`, `PartyIDSource`, `PartyRole`, and a flexible
  `buffer`;
- `TrdRegTimestamps`;
- `SideTrdRegTS`;
- `SecurityAltID`, with `SecurityAltID`, `SecurityAltIDSource`, and `buffer`;
- `Legs`, with the `InstrumentLeg` members `rekep.market.instrument.Leg`
  reads, and `buffer` for the rest.

`FixMsg.get` reads promoted columns and `kwargs` through the same registry
accessor, whether the caller names a numeric tag, canonical field name,
component path or namespace-qualified key.

`direction` says which way a line moved where its header verb says so --
`Receiving : 8=FIX...` reads False, `Sending : ...` True -- resolved at the
FIX stage against `rekep.fix.rules.DIRECTION_PATTERNS`, and only where the
verb opens the line before the payload's first token, so the same words
inside a payload never answer. Null is most rows: bridge re-log lines repeat
a payload without repeating the verb, and no answer beats a guessed one.

A `35=U...` wrapper may carry a rendered bridge payload with its own
`MSGTYPE`. In that form the named discriminator and named flat fields are
authoritative, so numeric copies of the same registry identities are removed;
indexed group members are never treated as duplicates.

## Stored categories

`parse_fix` uses disjoint pushed scans. Rows whose stored `Message.etype` is at
least `INTENT` go to `fix.market`; non-technical `MISC` rows go to `fix.misc`.
An unknown discriminator also goes to `fix.misc` when the transport is
recognized; only an unknown event on an unrecognized transport goes to
`fix.unknown`. Registry-declared technical MsgTypes and plugins do not enter a
FIX table. Both scans project the raw `message` column out: the already parsed
arguments carry the transcription input, so every resulting `FixMsg.message`
is null.

The market readers consume only `fix.market`, ordered by
`(unix, MsgSeqNum, hash)`. Normalized Instrument rows use the package-owned
user-defined `MsgType` `U1` so they remain distinguishable from captured FIX
messages.
