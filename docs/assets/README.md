# Documentation assets

`rkp-logo.svg` and `arrow-hub.svg` are original assets licensed with this
repository under Apache-2.0.

`fix.js` and `../stylesheets/fix.css` are the FIX section's three widgets: one
file, no framework, no build step. They mount on any page carrying a
`data-fix` element — `registry`, `decode` or `encode`.

`fix-registry.json` and `fix-details.json` are generated projections of the
bundled dictionary, not hand-edited sources. Rebuild them with
`python tools/fix_registry_dump.py`; see
[the registry browser page](../tools/fix-registry.md#regenerate-the-published-assets).
