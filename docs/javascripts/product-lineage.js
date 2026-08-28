// One widget for every product page: a line of text, and the columns each
// stage of the pipeline reads out of it.
//
// The FIX half is not reimplemented here. `fix-transcribe.js` publishes the
// decoder its own workspaces run on, and this asks that for the resolved
// pairs -- one answer to "what does this tag mean", on both pages.
//
// What this adds is the projection: every product declares, per column, the
// FIX field it is read from, and `docs_hooks.py` publishes those declarations
// straight from the classes. So a column's origin here is the origin the
// parser uses, and a column with no declared origin is shown as derived
// rather than guessed at.
//
// The page writes the container and nothing else. Seven pages spelling this
// markup out would be seven copies to keep in step with one script, and the
// first one to drift would fail silently -- a `querySelector` that finds
// nothing renders nothing.
(() => {
  "use strict";

  const app = document.querySelector("[data-product-lineage]");
  if (!app) return;

  const SEPARATORS = [
    ["auto", "Auto"],
    ["pipe", "Pipe |"],
    ["soh", "SOH"],
    ["eot-etx", "EOT + ETX"],
    ["caret-a", "Caret A ^A"],
    ["caret", "Caret ^"],
    ["semicolon", "Semicolon ;"],
    ["hash", "Hash #"],
    ["newline", "New line"],
  ];

  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (character) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character],
    );
  // A column carries the value of the FIX field it declares, addressed by tag
  // where the field has one and by name where it does not.
  const address = (origin) => String(origin.tag ?? origin.name ?? "");
  // How many values an enumeration names, beside the name a column declares.
  const sizeOf = (enums, name) => {
    const values = enums[name];
    return values ? ` (${Object.keys(values).length})` : "";
  };

  app.classList.add("fix-registry", "product-lineage");
  app.innerHTML = `
    <p class="fix-registry__status" data-lineage-status role="status" aria-live="polite">
      Loading contracts…
    </p>
    <div data-lineage-ready hidden>
      <form class="product-lineage__form" data-lineage-form>
        <label class="visually-hidden" for="lineage-text">Message text</label>
        <textarea id="lineage-text" name="text" rows="4" spellcheck="false"
                  autocomplete="off"></textarea>
        <div class="product-lineage__controls">
          <label>
            <span>Separator</span>
            <select name="separator">
              ${SEPARATORS.map(([value, label]) => `<option value="${value}">${label}</option>`).join("")}
            </select>
          </label>
          <button type="submit">Parse</button>
          <button type="button" data-lineage-reset>Reset</button>
        </div>
      </form>
      <ol class="product-lineage__stages" data-lineage-stages></ol>
      <div class="fix-registry__summary" data-lineage-summary></div>
      <div class="fix-registry__table-wrap" tabindex="0" role="region"
           aria-label="Columns and what they are read from">
        <table class="fix-registry__table product-lineage__table">
          <thead>
            <tr><th>Column</th><th>Reads</th><th>This line</th><th>What it is</th></tr>
          </thead>
          <tbody data-lineage-columns></tbody>
        </table>
      </div>
      <details>
        <summary>Resolved pairs</summary>
        <pre class="product-lineage__wire" tabindex="0"><code data-lineage-wire></code></pre>
      </details>
    </div>`;

  const select = (query) => app.querySelector(query);
  const status = select("[data-lineage-status]");
  const ready = select("[data-lineage-ready]");
  const transcribe = window.fixTranscribe;

  if (!transcribe) {
    status.dataset.error = "";
    status.textContent = "The FIX decoder did not load.";
    return;
  }

  Promise.all([
    fetch(new URL(app.dataset.source, window.location.href)).then((response) => {
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      return response.json();
    }),
    transcribe.load(app.dataset.registrySource),
  ])
    .then(([catalog, { registry }]) => start(catalog, registry))
    .catch((error) => {
      status.dataset.error = "";
      status.textContent = `Lineage unavailable: ${error.message}`;
    });

  function start(catalog, registry) {
    const products = new Map(catalog.products.map((one) => [one.key, one]));
    const product = products.get(app.dataset.product);
    if (!product) {
      status.dataset.error = "";
      status.textContent = `No contract published for ${app.dataset.product}.`;
      return;
    }
    // The chain that reaches this product, upstream first: every stage a row
    // passes through is a stage the page shows, and the catalog is the only
    // place that order is written down.
    const chain = [];
    for (let step = product; step; step = products.get(step.source)) chain.unshift(step);

    const form = select("[data-lineage-form]");
    const stages = select("[data-lineage-stages]");
    const columns = select("[data-lineage-columns]");
    const summary = select("[data-lineage-summary]");
    const wire = select("[data-lineage-wire]");
    let shown = product.key;
    let scheduled = 0;

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      render();
    });
    form.addEventListener("input", () => {
      window.cancelAnimationFrame(scheduled);
      scheduled = window.requestAnimationFrame(render);
    });
    stages.addEventListener("click", (event) => {
      const button = event.target.closest("[data-stage]");
      if (!button) return;
      shown = button.dataset.stage;
      render();
    });
    select("[data-lineage-reset]").addEventListener("click", () => {
      form.elements.text.value = app.dataset.sample || "";
      shown = product.key;
      render();
    });

    form.elements.text.value = app.dataset.sample || "";
    render();
    status.hidden = true;
    ready.hidden = false;

    function render() {
      const decoded = transcribe.decode(form.elements.text.value, registry, {
        separator: form.elements.separator.value,
      });
      // Every resolved pair, addressable the two ways a column declares an
      // origin. A repeated tag keeps its first reading, which is the one a
      // scalar column holds.
      const found = new Map();
      decoded.records.forEach((record) => {
        if (!record.resolved || record.shadowed) return;
        for (const key of [record.tag, record.name]) {
          if (key !== null && key !== undefined && !found.has(String(key))) {
            found.set(String(key), record);
          }
        }
      });

      wire.textContent = decoded.records.length
        ? decoded.records
            .filter((record) => !record.shadowed)
            .map((record) => `${record.output_key}=${record.output_value}`)
            .join("\n")
        : "No parsed pairs.";

      stages.innerHTML = chain
        .map((step) => {
          const carried = step.columns.filter(
            (column) => column.fix && found.has(address(column.fix)),
          ).length;
          const current = step.key === shown ? ' aria-current="step"' : "";
          return `<li><button type="button" data-stage="${escape(step.key)}"${current}>
            <span class="product-lineage__stage-name">${escape(step.name)}</span>
            <span class="product-lineage__stage-count">${carried} of ${step.columns.length} carried</span>
          </button></li>`;
        })
        .join("");

      const step = products.get(shown) || product;
      const rows = step.columns.map((column) => row(column, found, catalog.enums));
      const carried = rows.filter((one) => one.carried).length;
      const derived = step.columns.filter((column) => !column.fix).length;

      summary.innerHTML = [
        `<div><strong>${step.columns.length}</strong><span>columns</span></div>`,
        `<div><strong>${carried}</strong><span>carried by this line</span></div>`,
        `<div><strong>${derived}</strong><span>derived here</span></div>`,
        `<div><strong>${escape(decoded.protocol.code)} ${escape(decoded.version.value || "?")}</strong><span>read as</span></div>`,
      ].join("");

      columns.innerHTML = rows.map((one) => one.html).join("");
    }
  }

  // One column as the page draws it: what it is, where it is read from, and
  // what this line put in it.
  function row(column, found, enums) {
    const origin = column.fix;
    const record = origin ? found.get(address(origin)) : undefined;
    const carried = Boolean(record);
    const reads = origin
      ? `<code>${escape(origin.name || "")}</code>${
          origin.tag
            ? ` <span class="product-lineage__tag">&lt;${escape(origin.tag)}&gt;</span>`
            : ""
        }`
      : `<span class="product-lineage__derived">derived</span>`;
    // A carried value is the wire value and, where the field enumerates one,
    // what it means. A column with an origin the line does not carry says so,
    // because null here and null in the table are the same fact.
    const value = carried
      ? `<code>${escape(record.wire_value)}</code>${
          record.meaning
            ? `<span class="product-lineage__meaning">${escape(record.meaning)}</span>`
            : ""
        }`
      : origin
        ? `<span class="product-lineage__absent">not in this line</span>`
        : "";
    const marks = [];
    if (column.key === "primary") marks.push("key");
    if (column.partition) marks.push(`partition ${column.partition}`);
    if (column.enum) marks.push(`${column.enum}${sizeOf(enums, column.enum)}`);
    if (column.unit) marks.push(column.unit);
    if (column.nullable) marks.push("nullable");
    return {
      carried,
      html: `<tr${carried ? ' data-carried=""' : ""}>
        <th scope="row"><code>${escape(column.name)}</code>
          <span class="product-lineage__type">${escape(column.type)}</span></th>
        <td>${reads}</td>
        <td>${value}</td>
        <td><p>${escape(column.description)}</p>${
          marks.length
            ? `<p class="product-lineage__marks">${marks
                .map((mark) => `<span>${escape(mark)}</span>`)
                .join("")}</p>`
            : ""
        }</td>
      </tr>`,
    };
  }
})();
