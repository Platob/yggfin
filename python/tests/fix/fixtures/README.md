# FIX fixtures

Pages and captures the FIX tests read instead of the network.

- `fix-dictionary.html`, `fields_by_tag.html`, `tagNum_*.html` and `FIX44.xml`
  mirror the two sources a scrape reads, hand-written and small.
- `capture/` holds pages captured from the live dictionary unedited, so the
  parsing is pinned a second time against the layout the site really has.
- `bridge_keys.txt` is a synthetic bridge capture in the parser's target log
  layout. Every value in it is a placeholder (`FAKE-*`, `ORD-TEST-*`,
  `PARTY-TEST-*`): what it exists to carry is *key names* -- exact matches,
  a recorded spelling, near misses, namespaces, a component path, a
  narrative-prefixed line, and names written both `#Foo` and `Foo` -- and
  `tests/fix/test_classify.py` derives its counts from it and then pins them.
