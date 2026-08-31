# Decode FIX values

`decode` returns the registry's preferred human spelling. `meaning` returns
its prose; both leave an undeclared value safe:

```python
import pyarrow

from rekep.fix import FixRegistry

side = FixRegistry().resolve("Side").fix

print(side.decode("1"))
print(side.meaning("1"))
print(side.arrow_decode(pyarrow.array(["1", "2", "future"])).to_pylist())
```

```text
Buy
Buy
['Buy', 'Sell', 'future']
```

Arrow conversion uses the same registry-derived mapping as the scalar method.
An identity mapping returns the original array unchanged.

See [Encode FIX values](encode.md) for aliases and collision handling.
