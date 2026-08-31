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

Fields have a complete scriptable lifecycle:

```bash
rekep fix registry add-field --store data/fix --name BRKR.VenueTier \
  --type String --version '*' --description "The venue service tier."
rekep fix registry alias-field --store data/fix --name BRKR.VenueTier \
  --alias BROKER_VENUE_TIER --source broker-a
rekep fix registry show --store data/fix BRKR.VenueTier > venue-tier.json
# Edit the emitted declaration, then write it through the registry again.
rekep fix registry update-field --store data/fix --declaration venue-tier.json
rekep fix registry remove-field --store data/fix --name BRKR.VenueTier
```

Messages use the component verbs because both are one registry record; a
message declaration differs only by its `fix.msgtype` value:

```json
{
  "name": "VenueOrder",
  "versions": ["*"],
  "declaration": {
    "name": "VenueOrder",
    "type": "struct",
    "fix": {"component": "VenueOrder", "msgtype": "U1"},
    "fields": []
  }
}
```

```bash
rekep fix registry add-component --store data/fix --declaration venue-order.json
rekep fix registry components --store data/fix Venue
rekep fix registry component --store data/fix VenueOrder > venue-order.json
rekep fix registry update-component --store data/fix --declaration venue-order.json
rekep fix registry remove-component --store data/fix --name VenueOrder
```

These commands are the supported writers. Do not edit files under
`data/fix/fields`, `data/fix/components`, or `data/fix/repgroup`;
`FixRegistry` validates the whole post-change store before replacing a shard.
The repeating-group folder is regenerated from component trees.

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

`scrape` is the only command that reads the source dictionaries. A cold default
store downloads the repository's main-branch zip; an unavailable archive uses
the packaged projection and never starts a source scrape implicitly.

See the [registry internals](index.md) for bootstrap and storage rules, or
[browse the published registry](registry.md) without installing the package.
