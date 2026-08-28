# State

```python
from rekep.enums import State

state = State.from_str("PARTIALLY_FILLED")
assert state.band is State.PARTIAL
assert state.is_live
assert not state.is_terminal
```

Lifecycle states are ordered by completion, and each mnemonic carries its
rank as a two-digit prefix -- `10PENDNG`, `41FILLED`, `62INTREJ` -- so a code
reads as itself and sorts as the lifecycle does. `State.live_codes()` and
`State.terminal_codes()` spell the finite code sets a storage scan pushes
down.

Because the prefix leads the packed bytes, the stored `int64` sorts by
lifecycle as well. A range predicate over the column is therefore as honest
as the code sets: `state >= int(State.FILLED)` selects exactly the states at
or past that point.

| Key | Mnemonic | Stored value | Rank | Meaning |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` |  | 0 | 0 | Nothing has been stated. |
| `PENDING` | `10PENDNG` | 3544421165336645191 | 100 | Band floor: requested but not acknowledged. |
| `PENDING_NEW` | `11PNDNEW` | 3544702678800942423 | 110 | Awaiting first venue acknowledgement. |
| `OPEN` | `20OPEN` | 3616477706957225984 | 200 | Band floor: live at the venue. |
| `NEW` | `21NEW` | 3616758035474546688 | 210 | Acknowledged and working. |
| `ACCEPTED` | `22ACCEPT` | 3617025207879159892 | 220 | Accepted but not yet working. |
| `PENDING_REPLACE` | `23PNDRPL` | 3617323222792556620 | 230 | Amendment pending while the original remains live. |
| `PENDING_CANCEL` | `24PNDCNL` | 3617604697768283724 | 240 | Cancellation pending while the order remains live. |
| `SUSPENDED` | `25SUSPND` | 3617889501597158980 | 250 | Held by the venue and resumable. |
| `STOPPED` | `26STOPPD` | 3618170972211793988 | 260 | Stopped at a price awaiting a trade. |
| `PARTIAL` | `30PARTL` | 3688536336300788736 | 300 | Band floor: live and partly complete. |
| `PARTIALLY_FILLED` | `31PRTFIL` | 3688817884324579660 | 310 | Some quantity traded; the rest remains live. |
| `DONE` | `40DONE` | 3760580796260614144 | 400 | Band floor and first terminal state. |
| `FILLED` | `41FILLED` | 3760864444457698628 | 410 | Every share traded. |
| `DONE_FOR_DAY` | `42DONEDY` | 3761143746214052953 | 420 | Over for the session. |
| `CALCULATED` | `43CALCD` | 3761424061515908096 | 430 | Priced and closed by the venue. |
| `CLOSED` | `50CLOSED` | 3832637277919724868 | 500 | Band floor: over without completion. |
| `CANCELLED` | `51CANCLD` | 3832918705633971268 | 510 | Withdrawn before completion. |
| `REPLACED` | `52REPLCD` | 3833216690499109700 | 520 | Superseded by an amendment. |
| `EXPIRED` | `53EXPIRD` | 3833483953428845124 | 530 | Reached expiry while live. |
| `INTERNAL_EXPIRED` | `54INTEXP` | 3833769783569242192 | 540 | Expired locally after one day without a newer observation. |
| `FAILED` | `60FAILED` | 3904698123146773828 | 600 | Band floor: refused. |
| `REJECTED` | `61REJCTD` | 3904992809459078212 | 610 | Refused; reason fields explain why. |
| `INTERNAL_REJECTED` | `62INTREJ` | 3905264427654595914 | 620 | Refused by this pipeline before it could change market state. |
