# Parse FIX

`tasks/parse_fix/parse_fix.ipynb` reads protocol-neutral `Message` rows from
`logs.messages`, parses their payloads as FIX where the configured rules say
they are FIX, and routes the resulting `FixMsg` rows.

This is the first stage that opens the FIX dictionary. For each Arrow batch it:

1. classifies each raw `message` and splits it into ordered pairs;
2. infers the FIX application version;
3. resolves names, tags, types and configured value spellings;
4. lifts declared fields and structured components;
5. derives the event category, venue, transaction time and identities.

Repeated tags and wire order remain in `kwargs`. A resolved entry records the
canonical FIX key, its numeric tag, its value, and either its component path or
vendor namespace. Fields promoted for filtering use the registry spelling as
their physical column name: `MsgType`, `MsgSeqNum`, `OrigClOrdID`,
`TransactTime`, and so on.

## Routing

`Rules.into_arrow_category_array` selects one destination per parsed row:

| Table | Rows |
| --- | --- |
| `fix.market` | Orders, quotes, executions, books and instruments. |
| `fix.misc` | Recognized operational traffic that is not a market event. |
| `fix.unknown` | Payloads no configured protocol recognizes. |

All destinations use the same `FixMsg` contract. A market row may drop
the raw `message` after the ordered resolved fields carry its content; misc and
unknown rows retain the raw payload as the content of record.

The source interval is filtered on `Message.unix`, the recording clock. The
resulting `FixMsg.unix` may instead come from a regulatory timestamp,
`TransactTime`, market-data entry time, sending time, or finally the recording
clock. Output tables are sorted by `(unix, MsgSeqNum, hash)`.

## Instruments

After routing, the notebook resumes recent Instrument state and derives new
versions from the sorted market stream. Normalized Instrument rows are written
back to `fix.market` with the package-owned user-defined `MsgType` `U1`,
then `flatten_instruments` writes the Instrument table.

## Configuration

The adjacent `parse_fix.yml` owns every FIX setting: `fix_dictionary`,
`null_values`, event `rules`, protocol rules, and declared `fields`. It also
selects the catalog, branch, source interval, static columns and batch sizes.
A dictionary or rule change reruns this stage against retained `Message` rows;
the raw payload is parsed again under the new declaration.
