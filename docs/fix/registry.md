# FIX registry browser

Search the repository's FIX components and fields. The catalog is rebuilt from
`data/fix/` with every deployment.

Open a component or repeating group to follow nested declarations recursively;
long reference lists stay collapsed.

A query answers by identity first -- a tag, a MsgType, a name, part of one --
and reaches the record's prose only when nothing named it. `54` is one field,
not the ten whose descriptions mention 54:

```bash
rekep fix registry find --store data/fix 54 | jq -r '.[].name'
rekep fix registry find --store data/fix "order qty" | jq -r '.[].name'
```

```text
Side
---
OrderQty          CashOrderQty      OrderQty2         DayOrderQty
LegOrderQty       OrderBookingQty   OrderCapacityQty  OrderEventQty
RelatedOrderQty
```

## Namespaces and offline refresh

Standard FIX wins an unscoped lookup, followed by registered FIX UDFs and then
configured venues. Asking for a namespace selects that definition exactly:

```bash
rekep fix registry find --store data/fix 9001
rekep fix registry find --store data/fix 9001 --namespace fixtrading-udf
rekep fix registry definitions --store data/fix 9001
```

```text
MaxShow       9001  fixtrading-udf
TradeType     9001  clear-street
```

FIX Latest and registered UDFs arrive as complete Orchestra XML files. The
parser retains datatypes, code sets, messages, components, nested groups,
pedigree, and deprecation metadata. A reliable FIX datatype maps to Arrow;
unknown or disputed types use `string` while retaining the source datatype,
and unknown enumeration values stay as raw strings.

One online refresh fills the complete-file cache. The same command can then be
replayed without network access:

```bash
rekep fix registry scrape --output data/fix \
  --source-cache data/.fix-sources \
  --source fix-latest --source quickfix \
  --source fixtrading-udf --source clear-street
rekep fix registry scrape --output data/fix \
  --source-cache data/.fix-sources \
  --source fix-latest --source quickfix \
  --source fixtrading-udf --source clear-street --offline
rekep fix registry coverage --store data/fix
```

Every resulting store carries `sources.json`: source ID, namespace, version,
URL, format, SHA-256, and license or terms URL. Restricted venue entries are
catalog descriptors, not runnable adapters. Add a reviewed adapter for an
authorized local artifact before selecting it; the default refresh never
fetches or bundles those documents.

| source | use |
| --- | --- |
| [FIX Latest EP309](https://orchestrahub.org/fixtrading/fix-latest) | standard fields and structure |
| [QuickFIX EP280](https://github.com/quickfix/quickfix/tree/master/spec) | standard fallback |
| [registered FIX UDFs](https://orchestrahub.org/community/fix-udf) | tags 5000–9999 in `fixtrading-udf` |
| [Clear Street](https://github.com/clear-street/FIX-docs) | tags 9001–9011 in `clear-street` |

The published store has 6,285 standard, 4,028 registered-UDF, and 11 Clear
Street definitions. It carries 927 standard components, 525 groups, 193
messages, 126 UDF structures, 2,107 reviewed conflicts, and five string
fallbacks.

The [FIX Repository](https://www.fixtrading.org/standards/fix-repository/),
[Orchestra model](https://github.com/FIXTradingCommunity/fix-orchestra), and
[UDF PDF](https://fixtrading.org/packages/user-defined-fields-pdf/) are
reference and cross-check sources; their machine-readable counterparts above
drive publication. B2BITS, Nanoconda, and OnixS remain enrichment sources and
never outrank FIX Latest.

Cboe, Kraken, MIAX, Lime, FalconX, Eurex/Xetra, and B2BITS remain excluded
source descriptors because their current format or terms do not support a
reviewed, deterministic artifact here. The CLI reports the exact exclusion
instead of silently scraping them.

A new machine-readable source is one adapter declaration. Its checksum pins
the complete input; the shared Orchestra parser owns the nested structure:

```python
import hashlib
from pathlib import Path

from rekep.fix.adapters import OrchestraAdapter

document = Path("/srv/fix/venue.xml")
venue = OrchestraAdapter(
    source_id="venue",
    namespace="venue",
    version="1.2",
    url=document.as_uri(),
    format="orchestra",
    checksum=f"sha256:{hashlib.sha256(document.read_bytes()).hexdigest()}",
    license_url="https://venue.example/terms",
    default=False,
)
definitions = venue.load("data/.fix-sources").fields
```

Several words are every one of them, so `order qty` reaches `OrderQty` by its
name. The browser below ranks by the same rule as `FixRegistry.search`; a
result set that fell back to prose says so beside its count.

<div class="fix-registry" data-fix-registry data-source="../../assets/fix-registry.json"
     data-repository="https://github.com/Platob/yggfin/blob/main/data/fix">
  <p class="fix-registry__status" data-registry-status role="status" aria-live="polite">Loading registry…</p>
  <div data-registry-ready hidden>
    <nav class="fix-registry__jump" aria-label="Registry sections">
      <a href="#components">Components</a>
      <a href="#fields">Fields</a>
      <a href="../shell/">Registry CLI</a>
    </nav>

    <details class="fix-registry__coverage">
      <summary>Registry coverage</summary>
      <div class="fix-registry__summary" aria-label="Registry summary">
        <div><strong data-summary-components>—</strong><span>components</span></div>
        <div><strong data-summary-groups>—</strong><span>groups</span></div>
        <div><strong data-summary-fields>—</strong><span>fields</span></div>
        <div><strong data-summary-enums>—</strong><span>enumerations</span></div>
        <div><strong data-summary-namespaces>—</strong><span>namespaces</span></div>
        <div><strong data-summary-sources>—</strong><span>sources</span></div>
        <div><strong data-summary-versions>—</strong><span>versions</span></div>
      </div>
      <div class="fix-registry__namespace-coverage" data-namespace-coverage></div>
      <details class="fix-registry__sources">
        <summary>Source manifest</summary>
        <ul data-source-coverage></ul>
      </details>
    </details>

    <section id="components" class="fix-registry__section" aria-labelledby="components-title">
      <header>
        <div>
          <p class="fix-registry__eyebrow">01 / STRUCTURE</p>
          <h2 id="components-title" tabindex="-1">Components and groups</h2>
        </div>
        <output data-component-count aria-live="polite">—</output>
      </header>

      <form class="fix-registry__filters" data-component-filters>
        <label class="fix-registry__search">
          <span>Search</span>
          <input type="search" name="query" placeholder="Name, tag, MsgType, or member" autocomplete="off">
        </label>
        <label>
          <span>Version</span>
          <select name="version"><option value="">All versions</option></select>
        </label>
        <label>
          <span>Shape</span>
          <select name="kind">
            <option value="">All shapes</option>
            <option value="message">Message</option>
            <option value="repeating">Repeating group</option>
            <option value="composed">Composed</option>
            <option value="flat">Flat</option>
          </select>
        </label>
        <button type="reset">Clear</button>
      </form>

      <article class="fix-registry__detail" data-component-detail tabindex="-1" hidden></article>
      <div class="fix-registry__table-wrap">
        <table class="fix-registry__table">
          <thead><tr><th>Name</th><th>Arrow type</th><th>Shape</th><th>MsgType</th><th>Versions</th><th>Members</th></tr></thead>
          <tbody data-component-rows></tbody>
        </table>
      </div>
      <nav class="fix-registry__pager" data-component-pager aria-label="Component pages">
        <button type="button" data-previous>Previous</button>
        <span data-page>—</span>
        <button type="button" data-next>Next</button>
      </nav>
    </section>

    <section id="fields" class="fix-registry__section" aria-labelledby="fields-title">
      <header>
        <div>
          <p class="fix-registry__eyebrow">02 / DICTIONARY</p>
          <h2 id="fields-title" tabindex="-1">Fields</h2>
        </div>
        <output data-field-count aria-live="polite">—</output>
      </header>

      <form class="fix-registry__filters" data-field-filters>
        <label class="fix-registry__search">
          <span>Search</span>
          <input type="search" name="query" placeholder="Tag, name, value, or reference" autocomplete="off">
        </label>
        <label>
          <span>Namespace</span>
          <select name="namespace"><option value="">All namespaces</option></select>
        </label>
        <label>
          <span>Version</span>
          <select name="version"><option value="">All versions</option></select>
        </label>
        <label>
          <span>Arrow type</span>
          <select name="type"><option value="">All types</option></select>
        </label>
        <label>
          <span>Usage</span>
          <select name="kind">
            <option value="">All usages</option>
            <option value="enumerated">Enumerated</option>
            <option value="namespace">Namespace</option>
            <option value="component">Component member</option>
            <option value="message">Message field</option>
            <option value="plain">Unreferenced</option>
          </select>
        </label>
        <button type="reset">Clear</button>
      </form>

      <article class="fix-registry__detail" data-field-detail tabindex="-1" hidden></article>
      <div class="fix-registry__table-wrap">
        <table class="fix-registry__table">
          <thead><tr><th>Tag</th><th>Name</th><th>Arrow type</th><th>Usage</th><th>Versions</th><th>References</th></tr></thead>
          <tbody data-field-rows></tbody>
        </table>
      </div>
      <nav class="fix-registry__pager" data-field-pager aria-label="Field pages">
        <button type="button" data-previous>Previous</button>
        <span data-page>—</span>
        <button type="button" data-next>Next</button>
      </nav>
    </section>
  </div>
</div>

<noscript>This browser requires JavaScript. The source records remain available in
[`data/fix`](https://github.com/Platob/yggfin/tree/main/data/fix).</noscript>
