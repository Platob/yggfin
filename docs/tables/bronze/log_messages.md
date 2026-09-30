# bronze.record_keeping.log_messages

One row per captured line: the event the read settled over it, the line past its row header as `body`, and one column per row-header capture.

| | |
| --- | --- |
| written by | [`parse_log_messages`](../../tasks/parse-log-messages.md) |
| key | `curruuid` |
| partitioned by | `hour(currunix)` |
| sorted by | `currunix`, `seqnum`, `curruuid` |
| columns | 22 |
| Iceberg contract | `schemas/bronze/record_keeping/log_messages.json` |
| dbt source | `{{ source('bronze', 'log_messages') }}`, `schemas/bronze/schema.yml` |
| sample rows | [bronze.record_keeping.log_messages](../../samples/bronze/log_messages.md) |

## Columns

| column | type | required | description |
| --- | --- | :---: | --- |
| `currunix` | `timestamptz` | yes | When the event happened: the settled instant, UTC. |
| `creaunix` | `timestamptz` |  | When the event was created, where that is known; the earliest its chain knows once followed. |
| `recdunix` | `timestamptz` |  | When this event was recorded, where that is known; the earliest its statements know. |
| `exprunix` | `timestamptz` |  | When the event stops being good, where it does; the latest its chain knows once followed. |
| `prevunix` | `timestamptz` |  | When the event this one follows happened, where it follows one. |
| `snapunix` | `timestamptz` |  | The grid instant a walk read this event as the snapshot of; empty on every row no snapshot was taken of. |
| `curruuid` | `fixed[16]` | yes | The event's identity: UUIDv7 ordered by millisecond and sequence, with a content payload seeded by its cross hash. |
| `crossuuid` | `fixed[16]` | yes | The identity every event of one chain shares, derived from the code they share; the event's own where it names none. |
| `crosscode` | `string` |  | The code every event of one chain shares, as the event spells it; empty where none. |
| `currhashcode` | `long` | yes | The XXH3-64 of what the event states. |
| `crosshashcode` | `long` | yes | The XXH3-64 of the cross code; zero where the event names none. |
| `prevuuid` | `fixed[16]` |  | The identity of the event this one follows, where it follows one. |
| `seqnum` | `long` |  | The event's place among the events of its instant: 0 for the first its stream hands over there, one more for each next. |
| `srcuuids` | `list<fixed[16]>` |  | The sorted unique identities of the elements this event was read from: provenance, never its chain - no walk moves it. |
| `state` | `int` |  | The state the event reached, as the code of a lifecycle-sorted enum; UNKNOWN where nothing states one, the furthest its chain knows once followed. |
| `body` | `string` | yes | The line past its row header, as text: the edges stripped, the byte limit applied; never empty, because a line with no body is no line. |
| `msgthreadid` | `long` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |
| `msgsessionid` | `string` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |
| `msgctxid` | `string` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |
| `msgseqnum` | `long` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |
| `msgpluginid` | `string` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |
| `loglevel` | `string` |  | One row-header capture, read at the datatype its syntax matches; empty on every line the header declared it for and did not match. |

## `state` codes

`state` stores the code of a lifecycle-sorted enum: the codes order from the
first state to the terminal ones, and a code's hundreds are its rank.
[States](../states.md) lists every member.
