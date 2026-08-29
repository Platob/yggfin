# Direction

```python
from rekep.enums import Direction

assert Direction.from_str("sent") is Direction.SENT
assert Direction.from_str("recv") is Direction.RECV
assert int(Direction.SENT) == int.from_bytes(b"SENT", "big")
assert int(Direction.RECV) == int.from_bytes(b"RECV", "big")
```

`Direction` records movement stated before a message payload. A line with no
unambiguous transport verb stores `UNKNOWN`, so the column is a non-null
`int32` code.

The vocabulary is closed: its complete set is `UNKNOWN`, `SENT` and `RECV`.
The matching rules are documented with [FixMsg](../fix/fixmsg.md#direction).
