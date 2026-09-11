# Executions product

`executions.fills` records economic execution changes. One execution report
may state a new fill, correction, cancellation, bust, or status-only event;
only economically meaningful occurrences become rows.

## Grain and identity

One row is one execution occurrence. `execid` is preferred inside its session;
`tradeid` and venue transaction identifiers strengthen scope. Corrections and
cancels retain both their own event identity and the execution they reference.

## Planned schema

| column | Arrow type | null | contract |
| --- | --- | :---: | --- |
| `executionkey` | `fixed_size_binary[16]` | no | scoped execution identity; primary key |
| `eventkey` | `fixed_size_binary[16]` | no | source-event identity |
| `originalexecutionkey` | `fixed_size_binary[16]` | yes | corrected/cancelled execution |
| `url` | `string` | no | source object |
| `rownum` | `int64` | no | source line |
| `msghash` | `fixed_size_binary[16]` | no | parsed source identity |
| `executiontime` | `timestamp[us, UTC]` | no | transaction time, then market timestamp |
| `timepartition` | `timestamp[us, UTC]` | no | Iceberg day transform |
| `sessionid` | `string` | yes | scoped session |
| `execid` | `string` | yes | venue/broker execution id |
| `tradeid` | `string` | yes | trade identity |
| `orderkey` | `fixed_size_binary[16]` | yes | linked logical order |
| `clordid` | `string` | yes | client order id at execution |
| `orderid` | `string` | yes | venue order id at execution |
| `symbolticker` | `string` | yes | normalized instrument |
| `isincode` | `string` | yes | ISIN |
| `miccode` | `string` | yes | execution venue MIC |
| `side` | `string` | yes | economic side |
| `lastqty` | `double` | no | quantity changed by this occurrence |
| `lastpx` | `double` | no | occurrence price |
| `currency` | `string` | yes | price currency |
| `cumqty` | `double` | yes | source cumulative quantity |
| `leavesqty` | `double` | yes | source remaining quantity |
| `avgpx` | `double` | yes | source average price |
| `exectype` | `string` | no | fill/correct/cancel semantic |
| `state` | `string` | yes | order state after event |
| `liquidity` | `string` | yes | added/removed/auction where stated |
| `parties` | `list<struct>` | yes | executing/client/trader roles |
| `regulatorytimestamps` | `list<struct>` | yes | copied regulatory clocks |

## Economic rules

- Status-only reports with no economic quantity do not fabricate a zero fill.
- A correction and cancellation are immutable rows linked to the original;
  consumers calculate settled quantity from the event chain.
- `lastqty` and `lastpx` come from the occurrence. Cumulative fields are audit
  checks, not the source of occurrence quantity.
- Currency and unit mismatches are quality failures, not automatic conversion.
- Duplicate source positions are skipped; separately captured relay copies may
  share `msghash` and are reconciled by `executionkey` plus provenance.

## Reconciliation

Acceptance compares the settled execution chain with the latest order
`cumqty`, checks correction/cancel references, reports unlinked orders, and
retains every source position used in a conflict decision.
