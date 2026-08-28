# FIX registry browser

Search the repository's FIX components and fields. The catalog is rebuilt from
`data/fix/` with every deployment.

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

Several words are every one of them, so `order qty` reaches `OrderQty` by its
name. The browser below ranks by the same rule as
[`FixRegistry.search`](index.md#resolving-a-name); a result set that fell back
to the prose says so beside its count.

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
        <div><strong data-summary-fields>—</strong><span>fields</span></div>
        <div><strong data-summary-enums>—</strong><span>enumerations</span></div>
        <div><strong data-summary-versions>—</strong><span>versions</span></div>
      </div>
    </details>

    <section id="components" class="fix-registry__section" aria-labelledby="components-title">
      <header>
        <div>
          <p class="fix-registry__eyebrow">01 / STRUCTURE</p>
          <h2 id="components-title" tabindex="-1">Components</h2>
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
          <thead><tr><th>Name</th><th>Shape</th><th>MsgType</th><th>Versions</th><th>Members</th></tr></thead>
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
          <span>Version</span>
          <select name="version"><option value="">All versions</option></select>
        </label>
        <label>
          <span>Datatype</span>
          <select name="type"><option value="">All datatypes</option></select>
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
          <thead><tr><th>Tag</th><th>Name</th><th>Datatype</th><th>Usage</th><th>Versions</th><th>References</th></tr></thead>
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
