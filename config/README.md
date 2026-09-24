# Local configuration

This directory is an empty slot: nothing here is read by default, and no
checkout needs it. It is where an operator may keep a FIX dictionary of their
own, next to the checkout rather than inside the package.

The default dictionary is bundled in the installed package, at
`python/src/rekep/_data/fix`, and `fix_registry()` returns it with no location
and no environment variable; `fix_codec()` parses under it when handed no
registry. The stages that take a `codec` -- `parse_fix_raw`,
`parse_fix_refined` and `parse_books`, in `rekep.pipeline` -- default to that
codec, and all three take one because each stage after the parse reads a row
back as the message the dictionary wrote, so they run under the same one.

To parse against another dictionary -- the canonical `fields/`, `components/`
and `groups/` JSON documents `FixRegistry.write_into` emits, read back by
`FixRegistry.from_handle` -- write it anywhere, here included, and hand the
codec over it to each of those stages. From the repository root, with
`catalog` an open `IcebergCatalog`:

```python
from rekep.fix import fix_codec, fix_registry
from rekep.pipeline import parse_books, parse_fix_raw, parse_fix_refined
from rekep.times import window_of

codec = fix_codec(fix_registry("file:config/fix"))
day = window_of("2026-08-14", "2026-08-14")
parse_fix_raw(catalog, day, codec=codec)
parse_fix_refined(catalog, day, codec=codec)
parse_books(catalog, window_of("2026-08-14T12:00:00Z", "2026-08-14T13:00:00Z"), codec=codec)
```

The location must hold specification fields, and one that holds none is
refused; runtime and bridge fields are added for you. See
[Parse FIX raw](../docs/pipeline/parse-fix-raw.md#registry-override).

This is configuration, not a schema contract: `schemas/` publishes the table
shapes, and a dictionary is what the FIX one is generated from.
