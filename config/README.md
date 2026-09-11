# Local configuration

This directory is an empty slot: nothing here is read by default, and no
checkout needs it. It is where an operator may keep a FIX dictionary of their
own, next to the checkout rather than inside the package.

The default dictionary is bundled in the installed package, at
`python/src/rekep/_data/fix`, and `fix_registry()` returns it with no location
and no environment variable. `tasks/parse_fix/parse_fix.json` therefore
declares `"registry": null`, and `parse_fix` is the only task that takes a
`registry` parameter at all.

To parse against another dictionary — the canonical `fields/`, `components/`,
`groups/` and `messages/` JSON documents `FixRegistry.write_into` emits, read
back by `FixRegistry.from_handle` — write it anywhere, here included, and point
that one parameter at it:

```bash
uv run --project python rekep task run tasks/parse_fix/parse_fix.json \
  --parameter 'registry="file:config/fix"'
```

The location must hold specification fields; runtime and bridge fields are
added for you. See [Parse FIX](../docs/pipeline/tasks/parse-fix.md#registry-override).

This is configuration, not a schema contract: `schemas/` publishes the two
table shapes, and a dictionary is what one of them is generated from.
