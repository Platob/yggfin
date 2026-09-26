# Local configuration

This directory is an empty slot: nothing here is read by default, and no
checkout needs it. It is where an operator may keep a FIX dictionary of their
own, next to the checkout rather than inside the package.

The default dictionary is bundled in the installed package, at
`python/src/rekep/_data/fix`, and importing `rekep` installs it as the
process default: `FixRegistry.from_env()` answers it, and the three FIX tasks
-- `parse_fix_messages_raw`, `parse_fix_messages_refined` and `parse_books`
-- parse under `FixCodec.from_env()` when handed no `codec`.

To parse against another dictionary -- the `fields/`, `components/` and
`groups/` JSON documents `FixRegistry.write_into` emits and
`FixRegistry.from_handle` reads back -- write it anywhere, here included,
and either name its folder in the process environment under the variable
`rekep.fix.REGISTRY_VARIABLE` holds, before the process imports `rekep`, or
hand a codec over it to each of the three tasks. From the repository root,
with `storages` an open `rekep.Storages`:

```python
from rekep import FixCodec, FixRegistry
from rekep.fix import UNDATED
from rekep.pipeline import parse_books, parse_fix_messages_raw, parse_fix_messages_refined
from rekep.times import window_of

codec = FixCodec(FixRegistry.from_handle("config/fix"), default_sending_time=UNDATED)
window = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
parse_fix_messages_raw(storages, window, codec=codec)
parse_fix_messages_refined(storages, window, codec=codec)
parse_books(storages, window, codec=codec)
```

The three share one codec because each task after the parse reads a stored
row back as the message the dictionary wrote. A folder holding no
specification field is refused wherever a FIX table's shape is built; the
crate's own fields are added for you. See
[the bundled registry](../docs/fix/index.md#another-dictionary).

This is configuration, not a table contract: `schemas/` publishes the table
shapes, and a dictionary is what the FIX ones are generated from.
