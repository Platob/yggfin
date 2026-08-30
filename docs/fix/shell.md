# Registry CLI

Use the JSON commands for automation and the shell for guided inspection or
small, confirmed edits.

| Surface | Best for | Output |
| --- | --- | --- |
| `rekep fix registry` | scripts, CI, complete records | JSON on `stdout` |
| `rekep fix shell` | search, review, guided edits | terminal UI on `stderr` |

## Scriptable commands

```bash
rekep fix registry find --store data/fix "execution report" --limit 5
rekep fix registry show --store data/fix 35
rekep fix registry check --store data/fix
```

Reads emit only JSON to `stdout`. Status and failures stay on `stderr`, so a
pipe receives no styling or progress text. Run `--help` on a verb for its
arguments.

## Interactive shell

```bash
rekep fix shell --store data/fix
```

Start with five commands:

```text
find PartyRole          # ranked fields, one row per identity
show 452                # field summary and a bounded value preview
component Parties       # bounded newest-version member tree
edit PartyRole          # guided edit, preview, then confirmation
check                   # validate the whole store
```

`help` groups the remaining commands; `help show` explains one. Long field and
component records stay bounded in the shell—use `fix registry show` or
`fix registry component` when complete JSON is required.

Every edit, alias, removal, component change, and archive write is previewed
and defaults to **no**. `Ctrl-C` cancels the current operation without leaving
a partial write.

## Registry lifecycle

```bash
rekep fix registry scrape --output data/fix
cd python
uv run python -c "from rekep.fix.publish import publish_full; \
publish_full('../data/fix', '../data/fix.zip')"
```

See the [registry internals](index.md) for bootstrap and storage rules, or
[browse the published registry](registry.md) without installing the package.
