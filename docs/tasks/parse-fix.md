# Parse FIX

`tasks/parse_fix/parse_fix.ipynb` reads `text.messages` -- the structured,
unresolved rows `parse_messages` wrote -- resolves them against the FIX
dictionary, and routes each one to a table.

It never reads `message` on a row it resolves. Everything comes from the
stored `kwargs`, `protocol_code` and `protocol_version`, which is what makes a
re-parse cheap: the batch splits by a stored value rather than by
re-categorising and re-splitting every line.

## What it fills

The same `kwargs` column, three members further along:

- `tag`, for a key the dictionary answers for;
- `key`, canonicalized to the registry's own spelling (`PARTYID` becomes
  `PartyID`);
- `value`, resolved through the dictionary's translations (`Side=Buy` becomes
  `1`).

`namespace` and `comp` are byte-identical before and after: where a field
stood is a fact about the spelling, settled at the message stage, and a
dictionary has nothing to add to it. Nothing is rewritten from scratch --
`FixMessage` to `FixMessage` is a fill, not a shape conversion.

Then the fields that earn a column of their own are lifted out of `kwargs`,
the structured components are built, the transaction time is resolved now that
those columns exist (see [Market](../market.md#when-it-happened)), and the
digest is taken over the parsed values.

## The redirection test

One condition, applied in one place, decides where a row goes:

> A row is usable as a FIX message when its `etype` is anything other than
> `UNKNOWN` -- that is, when the event rules recognised what the line is.

`Rules.into_arrow_category_array` is that condition, and it runs once per
batch:

| table | the row |
| --- | --- |
| `fixmessage.market` | resolved as FIX: order, quote, execution, book or instrument traffic |
| `fixmessage.misc` | not usable as FIX, but its protocol is one the rules recognise |
| `fixmessage.unknown` | not usable as FIX, and its protocol is not recognised either |

All three hold the same `FixMessage` class under one contract, so a reader
unions them with one schema and no cast.

## What is guaranteed non-null

| column | `market` | `misc` | `unknown` |
| --- | --- | --- | --- |
| `unix`, `unix_hour`, `hash`, `etype`, `runix` | yes | yes | yes |
| `source_url`, `source_rownum`, `protocol_code` | yes | yes | yes |
| `message` | **null** | yes | yes |
| `kwargs` | yes, resolved | as the line split | as the line split |
| `protocol_version` | where one resolved | where one resolved | usually null |
| `msg_type` | where the message carried one | usually null | usually null |

`message` is null on `market` rows because `kwargs` carries everything it
held; an all-null column run-length and dictionary encodes to nothing on
disk. On a redirected row the raw string is still the content of record, so it
stays.

A redirected row's `kwargs` is *not* empty in the general case: a `misc` line
that split into pairs keeps them, structured, so it is queryable by key and
value without ever having been FIX. It is only null where the line split into
nothing at all.

## What a redirected row's `unix` holds

A redirected row has no transaction time for the chain to resolve -- it
carries no FIX clock at all -- so `unix` is the recording clock and
`unix_source` says `recorded`. `runix` holds the same instant. The two agree
on such a row, and that agreement is the signal: a row whose `unix` equals its
`runix` and whose `unix_source` is `recorded` was never dated by anything but
the log itself.

## Instruments

After the routing, the notebook reads the prior interval's latest
`market.instruments` snapshots and the newly sorted `fixmessage.market` slice.
It versions and snapshots instrument facts once, then appends them to
`fixmessage.market` as normalized rows carrying `MsgType` `U1` -- a
user-defined type of this package's own, so a synthesized instrument is
distinguishable from a `SecurityDefinition <d>` a real bridge sent, by the
message alone.

The adjacent `parse_fix.yml` selects the message table, the offline FIX
registry, the catalog, batch and commit sizes, and an optional `[start, end)`
interval. That interval is read against the recording clock, because that is
what the message stage partitioned on: `unix` moves when a transaction time
resolves, so filtering on it here would drop rows the interval owns.
