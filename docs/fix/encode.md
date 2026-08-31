# Encode FIX values

`encode` maps a meaning or alias to the wire value and leaves an unknown value
unchanged:

```python
import pyarrow

from rekep.fix import FixRegistry

side = FixRegistry().resolve("Side").fix

print(side.encode("Buy"))
print(side.encode("SELL_SHORT"))
print(side.arrow_encode(pyarrow.array(["Buy", "2", "future"])).to_pylist())
```

```text
1
5
['1', '2', 'future']
```

Matching case-folds and removes non-alphanumeric characters. The lookup is
derived from each enumerated value's meaning, aliases, and raw wire value.
If two values claim the same normalized spelling, neither wins and the input
passes through.

See [Decode FIX values](decode.md) for the reverse direction and prose.
