# silver.record_keeping.executions

The executions the books carry, one row per execution.

| | |
| --- | --- |
| written by | [`parse_executions`](../../tasks/parse-orders-quotes-executions.md) |
| key | `curruuid` |
| partitioned by | `hour(currunix)` |
| sorted by | `currunix`, `seqnum`, `curruuid` |
| columns | 48 |
| Iceberg contract | `schemas/silver/record_keeping/executions.json` |
| dbt source | `{{ source('silver', 'executions') }}`, `schemas/silver/schema.yml` |
| sample rows | [silver.record_keeping.executions](../../samples/silver/executions.md) |

## Columns

| column | type | required | description |
| --- | --- | :---: | --- |
| `marketdatakind` | `int` |  |  |
| `currunix` | `timestamptz` | yes | When the event happened: the settled instant, UTC. |
| `creaunix` | `timestamptz` |  | When the event was created, where that is known; the earliest its chain knows once followed. |
| `execunix` | `timestamptz` |  | The latest execution clock this lifecycle reached as of this event; an execution dates itself, following carries it, and duplicate statements keep their earliest observation. |
| `recdunix` | `timestamptz` |  | When this event was recorded, where that is known; the earliest its statements know. |
| `exprunix` | `timestamptz` |  | When the event stops being good, where it does; the latest its chain knows once followed. |
| `prevunix` | `timestamptz` |  | When the event this one follows happened, where it follows one. |
| `snapunix` | `timestamptz` |  | The grid instant a walk read this event as the snapshot of; empty on every row no snapshot was taken of. |
| `curruuid` | `uuid` | yes | The event's identity: UUIDv7 ordered by millisecond and sequence, with a content payload seeded by its cross hash. |
| `crossuuid` | `uuid` | yes | The identity every event of one chain shares, derived from the code they share; the event's own where it names none. |
| `crosscode` | `string` |  | The code every event of one chain shares, as the event spells it; empty where none. |
| `currhashcode` | `long` | yes | The XXH3-64 of what the event states. |
| `crosshashcode` | `long` | yes | The XXH3-64 of the cross code; zero where the event names none. |
| `prevuuid` | `uuid` |  | The identity of the event this one follows, where it follows one. |
| `seqnum` | `long` |  | The event's place in its chain: how many came before it. |
| `srcuuids` | `list<uuid>` |  | The sorted unique identities of the elements this event was read from: provenance, never its chain - no walk moves it. |
| `state` | `int` |  | The state the event reached, as the code of a lifecycle-sorted enum; UNKNOWN where nothing states one, the furthest its chain knows once followed. |
| `price` | `decimal(38, 18)` |  |  |
| `currency` | `string` | yes |  |
| `quantity` | `decimal(38, 18)` |  |  |
| `unit` | `string` | yes |  |
| `side` | `int` | yes |  |
| `securityids` | `map<string, string>` |  |  |
| `isincode` | `string` |  |  |
| `cficode` | `string` |  |  |
| `miccode` | `string` |  |  |
| `lastpx` | `decimal(38, 18)` |  |  |
| `lastqty` | `decimal(38, 18)` |  |  |
| `avgpx` | `decimal(38, 18)` |  |  |
| `cumqty` | `decimal(38, 18)` |  |  |
| `leavesqty` | `decimal(38, 18)` |  |  |
| `prevpx` | `decimal(38, 18)` |  |  |
| `prevqty` | `decimal(38, 18)` |  |  |
| `spotrate` | `decimal(38, 18)` |  |  |
| `forwardpoints` | `decimal(38, 18)` |  |  |
| `bidpx` | `decimal(38, 18)` |  |  |
| `bidqty` | `decimal(38, 18)` |  |  |
| `bidccy` | `string` |  |  |
| `askpx` | `decimal(38, 18)` |  |  |
| `askqty` | `decimal(38, 18)` |  |  |
| `askccy` | `string` |  |  |
| `fxrates` | `map<string, decimal(38, 18)>` |  |  |
| `ticker` | `string` |  |  |
| `metadata` | `map<string, string>` |  |  |
| `tif` | `string` |  |  |
| `tradable` | `boolean` |  |  |
| `altids` | `map<string, string>` |  |  |
| `bookscope` | `string` |  |  |

## `state` codes

`state` stores the code of a lifecycle-sorted enum: the codes order from the
first state to the terminal ones, and a code's hundreds are its rank.
[States](../states.md) lists every member.
