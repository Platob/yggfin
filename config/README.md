# Local configuration

This directory is an empty slot: nothing here is read by default, and no
checkout needs it. It is where an operator may keep a FIX dictionary of their
own, next to the checkout rather than inside the package.

The default dictionary is bundled in the installed package, at
`python/src/rekep/_data/fix`, and `fix_registry()` returns it with no location
and no environment variable. The shipped defaults of the tasks that take a
`registry` parameter -- `parse_fix_raw`, `parse_fix_refined` and
`parse_books`, under `python/src/rekep/tasks/` -- therefore declare
`"registry": null`. All three take it because each stage after the parse
reads a row back as the message the dictionary wrote, so they run under the
same one.

To parse against another dictionary -- the canonical `fields/`, `components/`
and `groups/` JSON documents `FixRegistry.write_into` emits, read back by
`FixRegistry.from_handle` -- write it anywhere, here included, and point that
one parameter at it on each task:

```bash
for TASK in parse_fix_raw parse_fix_refined parse_books; do
  uv run --project python rekep tasks "$TASK" run \
    --parameter 'registry="file:config/fix"'
done
```

The location must hold specification fields; runtime and bridge fields are
added for you. See
[Parse FIX raw](../docs/pipeline/tasks/parse-fix-raw.md#registry-override).

This is configuration, not a schema contract: `schemas/` publishes the two
table shapes, and a dictionary is what one of them is generated from.
