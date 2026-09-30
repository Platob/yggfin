# Documentation assets

`rkp-logo.svg` and `arrow-hub.svg` are original assets licensed with this
repository under Apache-2.0.

`fix.js` and `../stylesheets/fix.css` are the FIX section's three widgets: one
file, no framework, no build step. They mount on any page carrying a
`data-fix` element — `registry`, `decode` or `encode`.

`fix-registry.json` and `fix-details.json` are generated projections of the
bundled dictionary, not hand-edited sources. Rebuild them with
`uv run --project python python tools/fix_registry_dump.py`; see
[the registry assets page](../fix/assets.md#regenerate-the-published-assets).

`market-server/` holds the [market server page](../storages/market-server.md)'s
screenshots, `chart-*` and `point-*` in each colour scheme: the page the server
serves over the feed that page exports, served under the name `books`, opened
at the view link that page states. They are taken with Playwright's Chromium,
launched with `--lang=en-GB`, in a 1440x900 viewport under locale `en-GB`,
timezone `Europe/Zurich` and `colorScheme` `light` or `dark`, nothing stored,
once the network is idle and `#bid-events` holds its table: `chart-*` clips
the page from its top to 16 pixels below `.chart-panel`, `point-*` from 16
pixels above `.summary-panel` to 16 pixels below `.audit`. Each is then
recompressed losslessly (oxipng, level 6, every ancillary chunk stripped).
