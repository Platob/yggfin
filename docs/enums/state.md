# State

```python
from rekep.enums import State

state = State.from_str("PARTIALLY_FILLED")
assert state.band is State.PARTIAL
assert state.is_live
assert not state.is_terminal
```

Lifecycle states are ordered by completion, and that order rides in each
member's *rank* rather than in the stored value: the stored value is the
readable mnemonic. `State.live_codes()` and `State.terminal_codes()` spell
the finite code sets a storage scan pushes down.

| Key | Mnemonic | Stored value | Rank | Meaning |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | | 0 | 0 | Nothing has been stated. |
| `PENDING` | `PENDING` | 22594200592272967 | 100 | Band floor: requested but not acknowledged. |
| `PENDING_NEW` | `PENDNEW` | 22594200592598359 | 110 | Awaiting first venue acknowledgement. |
| `OPEN` | `OPEN` | 1330660686 | 200 | Band floor: live at the venue. |
| `NEW` | `NEW` | 5129559 | 210 | Acknowledged and working. |
| `ACCEPTED` | `ACCEPTED` | 4702676400884434244 | 220 | Accepted but not yet working. |
| `PENDING_REPLACE` | `PENDRPLC` | 5784115351773006915 | 230 | Amendment pending while the original remains live. |
| `PENDING_CANCEL` | `PENDCNCL` | 5784115351521215308 | 240 | Cancellation pending while the order remains live. |
| `SUSPENDED` | `SUSPEND` | 23456239384350276 | 250 | Held by the venue and resumable. |
| `STOPPED` | `STOPPED` | 23455122693571908 | 260 | Stopped at a price awaiting a trade. |
| `PARTIAL` | `PARTIAL` | 22589819994063180 | 300 | Band floor: live and partly complete. |
| `PARTIALLY_FILLED` | `PARTFILL` | 5782993918430366796 | 310 | Some quantity traded; the rest remains live. |
| `DONE` | `DONE` | 1146048069 | 400 | Band floor and first terminal state. |
| `FILLED` | `FILLED` | 77280626623812 | 410 | Every share traded. |
| `DONE_FOR_DAY` | `DONEDAY` | 19227496004469081 | 420 | Over for the session. |
| `CALCULATED` | `CALCULTD` | 4846238526104949828 | 430 | Priced and closed by the venue. |
| `CLOSED` | `CLOSED` | 73995027432772 | 500 | Band floor: over without completion. |
| `CANCELLED` | `CANCELED` | 4846240724859766084 | 510 | Withdrawn before completion. |
| `REPLACED` | `REPLACED` | 5928232772945790276 | 520 | Superseded by an amendment. |
| `EXPIRED` | `EXPIRED` | 19518875243791684 | 530 | Reached expiry while live. |
| `INTERNAL_EXPIRED` | `INTEXPRD` | 5282252069763306052 | 540 | Expired locally after one day without a newer observation. |
| `FAILED` | `FAILED` | 77246216553796 | 600 | Band floor: refused. |
| `REJECTED` | `REJECTED` | 5928226145845921092 | 610 | Refused; reason fields explain why. |
| `INTERNAL_REJECTED` | `INTREJCT` | 5282252125278716756 | 620 | Refused by this pipeline before it could change market state. |
