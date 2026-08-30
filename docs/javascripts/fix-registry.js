(() => {
  "use strict";

  const app = document.querySelector("[data-fix-registry]");
  if (!app) return;

  const PAGE_SIZE = 20;
  const number = new Intl.NumberFormat();
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const select = (query, root = app) => root.querySelector(query);
  const status = select("[data-registry-status]");
  const ready = select("[data-registry-ready]");
  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (character) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
          character
        ],
    );
  const normalized = (value) => String(value ?? "").trim().toLowerCase();
  const encodedKey = (value) => String(value ?? "").toLowerCase().replace(/[^a-z0-9]+/g, "");

  // One field's spelling lookup, derived from its values exactly as the
  // package derives it: a spelling two values share is emitted for neither.
  // One direction only -- every spelling a value carries reaches the value,
  // and nothing converts back out of it.
  function encodings(field) {
    const claimed = new Map();
    for (const one of list(field.values)) {
      const value = String(one.value);
      for (const spelled of [one.meaning || "", ...list(one.aliases), value]) {
        const key = encodedKey(spelled);
        if (!key) continue;
        const owners = claimed.get(key) || [];
        if (!owners.includes(value)) owners.push(value);
        claimed.set(key, owners);
      }
    }
    const found = {};
    for (const [key, owners] of claimed) if (owners.length === 1) found[key] = owners[0];
    return found;
  }

  const list = (value) => (Array.isArray(value) ? value : []);
  const object = (value) => (value && typeof value === "object" ? value : {});
  const chips = (values) =>
    list(values)
      .map((value) => `<span class="fix-registry__chip">${escape(value)}</span>`)
      .join("");
  const badge = (value, modifier = "") =>
    `<span class="fix-registry__badge${modifier ? ` fix-registry__badge--${modifier}` : ""}">${escape(value)}</span>`;
  const tagCode = (value, angled = false) => {
    if (value === undefined || value === null || value === "") return "";
    const label = angled ? `&lt;${escape(value)}&gt;` : escape(value);
    return `<code class="fix-registry__tag" title="FIX tag ${escape(value)}">${label}</code>`;
  };
  const href = (kind, value) => `#${kind}=${encodeURIComponent(value)}`;

  // What a query matched, best first, exactly as `FixRegistry.search` ranks it:
  // an identity -- a tag, a MsgType, a name, part of one -- and only then the
  // record's own prose. Searching a whole record for the text is what buried
  // `Side` behind `Account` and `Currency`: 4,343 of 6,101 fields carry "side"
  // somewhere in their JSON.
  const IDENTIFIED = 3;
  const BY_TEXT = 4;

  function rank(entry, query, parts) {
    const name = entry._name;
    if (query === name || query === entry._tag || query === entry._msgtype) return 0;
    if (name.startsWith(query)) return 1;
    if (parts.every((part) => name.includes(part))) return 2;
    if (parts.every((part) => entry._text.includes(part))) return BY_TEXT;
    return null;
  }

  // The rows one query answers: those that named something, or -- when nothing
  // did -- those that only mention it. Never both, so an answer is never
  // padded with the records that merely say the word.
  function ranked(entries, query) {
    const wanted = normalized(query);
    if (!wanted) return { rows: entries, by: "" };
    const parts = wanted.split(/\s+/).filter(Boolean);
    const scored = [];
    for (const entry of entries) {
      const score = rank(entry, wanted, parts);
      if (score !== null) scored.push([score, entry]);
    }
    if (!scored.length) return { rows: [], by: "" };
    const best = Math.min(...scored.map(([score]) => score));
    const kept = scored.filter(([score]) => (best < IDENTIFIED ? score < IDENTIFIED : true));
    kept.sort((one, other) => one[0] - other[0]);
    return { rows: kept.map(([, entry]) => entry), by: best < IDENTIFIED ? "name" : "text" };
  }

  fetch(new URL(app.dataset.source, window.location.href))
    .then((response) => {
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      return response.json();
    })
    .then(start)
    .catch((error) => {
      status.dataset.error = "";
      status.textContent = `Registry unavailable: ${error.message}`;
    });

  function start(catalog) {
    const components = list(catalog.components);
    const fields = list(catalog.fields);
    const versions = list(catalog.versions);
    const componentByName = new Map(
      components.map((component) => [normalized(component.name), component]),
    );
    const fieldByName = new Map(fields.map((field) => [normalized(field.name), field]));
    const fieldByTag = new Map(
      fields.filter((field) => field.tag !== undefined).map((field) => [String(field.tag), field]),
    );
    const componentBacklinks = new Map();
    const componentFields = new Map();

    components.forEach((component) => {
      component._tree = fixDeclaration.members(component.declaration);
      component._msgType = fixDeclaration.msgType(component.declaration);
      component._members = flattenMembers(component._tree);
      component._shape = componentShape(component);
      component._name = normalized(component.name);
      component._tag = "";
      component._msgtype = normalized(component._msgType);
      component._text = normalized(JSON.stringify(component));
      component._members
        .filter((member) => member.kind === "component")
        .forEach((member) => {
          const owners = componentBacklinks.get(normalized(member.name)) || [];
          owners.push(component);
          componentBacklinks.set(normalized(member.name), owners);
        });
    });
    fields.forEach((field) => {
      field._usages = fieldUsages(field);
      field._name = normalized(field.name);
      field._tag = field.tag === undefined ? "" : String(field.tag);
      field._msgtype = "";
      field._text = normalized(JSON.stringify(field));
      list(field.components).forEach((name) => {
        const members = componentFields.get(normalized(name)) || [];
        members.push(field);
        componentFields.set(normalized(name), members);
      });
    });

    const state = {
      component: { query: "", version: "", kind: "", page: 0 },
      field: { query: "", version: "", type: "", kind: "", page: 0 },
    };
    const componentForm = select("[data-component-filters]");
    const fieldForm = select("[data-field-filters]");
    const componentRows = select("[data-component-rows]");
    const fieldRows = select("[data-field-rows]");
    const componentDetail = select("[data-component-detail]");
    const fieldDetail = select("[data-field-detail]");
    const datatypes = new Map();
    fields.forEach((field) => {
      const key = normalized(field.type);
      const current = datatypes.get(key);
      if (key && (!current || (current === current.toUpperCase() && field.type !== current))) {
        datatypes.set(key, field.type);
      }
    });

    fillSelect(componentForm.elements.version, versions);
    fillSelect(fieldForm.elements.version, versions);
    fillSelect(
      fieldForm.elements.type,
      [...datatypes].sort((left, right) => left[1].localeCompare(right[1])),
    );

    select("[data-summary-components]").textContent = number.format(components.length);
    select("[data-summary-fields]").textContent = number.format(fields.length);
    select("[data-summary-enums]").textContent = number.format(
      fields.filter((field) => list(field.values).length > 0).length,
    );
    select("[data-summary-versions]").textContent = number.format(versions.length);

    bindForm(componentForm, state.component, renderComponents);
    bindForm(fieldForm, state.field, renderFields);
    bindPager(select("[data-component-pager]"), state.component, renderComponents);
    bindPager(select("[data-field-pager]"), state.field, renderFields);
    app.addEventListener("click", (event) => {
      const message = event.target.closest("[data-message-filter]");
      if (message) {
        event.preventDefault();
        Object.assign(state.field, {
          query: message.dataset.messageFilter,
          version: "",
          type: "",
          kind: "",
          page: 0,
        });
        writeForm(fieldForm, state.field);
        const url = urlForState();
        url.hash = "fields";
        window.history.pushState(null, "", url);
        renderFields();
        route(false);
        select("#fields").scrollIntoView({
          behavior: reducedMotion ? "auto" : "smooth",
          block: "start",
        });
        fieldForm.elements.query.focus({ preventScroll: true });
      }

      const close = event.target.closest("[data-detail-close]");
      if (close) {
        const target = select(close.getAttribute("href"));
        window.setTimeout(() => target?.focus({ preventScroll: true }));
      }
    });

    function fillSelect(element, values) {
      values.forEach((value) => {
        const [optionValue, label] = Array.isArray(value) ? value : [value, value];
        element.add(new Option(label, optionValue));
      });
    }

    function readState() {
      const query = new URLSearchParams(window.location.search);
      Object.assign(state.component, {
        query: query.get("cq") || "",
        version: query.get("cv") || "",
        kind: query.get("ck") || "",
        page: pageOf(query.get("cp")),
      });
      Object.assign(state.field, {
        query: query.get("fq") || "",
        version: query.get("fv") || "",
        type: normalized(query.get("ft") || ""),
        kind: query.get("fk") || "",
        page: pageOf(query.get("fp")),
      });
      writeForm(componentForm, state.component);
      writeForm(fieldForm, state.field);
    }

    function pageOf(value) {
      const parsed = Number.parseInt(value || "0", 10);
      return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
    }

    function writeForm(form, values) {
      [...form.elements].forEach((element) => {
        if (element.name && Object.hasOwn(values, element.name)) element.value = values[element.name];
      });
    }

    function bindForm(form, values, render) {
      form.addEventListener("submit", (event) => event.preventDefault());
      form.addEventListener("input", () => {
        [...form.elements].forEach((element) => {
          if (element.name && Object.hasOwn(values, element.name)) values[element.name] = element.value;
        });
        values.page = 0;
        writeUrl();
        render();
      });
      form.addEventListener("reset", () => {
        window.setTimeout(() => {
          Object.keys(values).forEach((key) => {
            values[key] = key === "page" ? 0 : "";
          });
          writeUrl();
          render();
        });
      });
    }

    function bindPager(pager, values, render) {
      select("[data-previous]", pager).addEventListener("click", () => {
        values.page = Math.max(0, values.page - 1);
        writeUrl();
        render();
        pager.scrollIntoView({ block: "nearest" });
      });
      select("[data-next]", pager).addEventListener("click", () => {
        values.page += 1;
        writeUrl();
        render();
        pager.scrollIntoView({ block: "nearest" });
      });
    }

    function urlForState() {
      const url = new URL(window.location.href);
      const values = {
        cq: state.component.query,
        cv: state.component.version,
        ck: state.component.kind,
        cp: state.component.page || "",
        fq: state.field.query,
        fv: state.field.version,
        ft: state.field.type,
        fk: state.field.kind,
        fp: state.field.page || "",
      };
      Object.entries(values).forEach(([key, value]) => {
        if (value === "") url.searchParams.delete(key);
        else url.searchParams.set(key, value);
      });
      return url;
    }

    function writeUrl() {
      window.history.replaceState(null, "", urlForState());
    }

    function renderComponents() {
      const found = ranked(components, state.component.query);
      const filtered = found.rows.filter(
        (component) =>
          (!state.component.version || list(component.versions).includes(state.component.version)) &&
          (!state.component.kind || component._shape === state.component.kind),
      );
      state.component.page = validPage(state.component.page, filtered.length);
      componentRows.innerHTML = page(filtered, state.component.page)
        .map(
          (component) => `<tr>
            <td><a class="fix-registry__name" href="${href("component", component.name)}">${escape(component.name)}</a></td>
            <td>${badge(shapeLabel(component._shape))}</td>
            <td>${component._msgType ? `<code>${escape(component._msgType)}</code>` : '<span class="fix-registry__muted">—</span>'}</td>
            <td>${chips(component.versions)}</td>
            <td>${number.format(component._members.length)}</td>
          </tr>`,
        )
        .join("");
      if (!filtered.length) componentRows.innerHTML = emptyRow(5);
      updateCount(select("[data-component-count]"), filtered.length, components.length, found.by);
      updatePager(select("[data-component-pager]"), state.component.page, filtered.length);
    }

    function renderFields() {
      const found = ranked(fields, state.field.query);
      const filtered = found.rows.filter(
        (field) =>
          (!state.field.version || list(field.versions).includes(state.field.version)) &&
          (!state.field.type || normalized(field.type) === state.field.type) &&
          (!state.field.kind || field._usages.includes(state.field.kind)),
      );
      state.field.page = validPage(state.field.page, filtered.length);
      fieldRows.innerHTML = page(filtered, state.field.page)
        .map(
          (field) => `<tr>
            <td>${field.tag === undefined ? '<span class="fix-registry__muted">—</span>' : tagCode(field.tag)}</td>
            <td><div class="fix-registry__identity">
              <a class="fix-registry__name" href="${href("field", field.tag ?? field.name)}">${escape(field.name)}</a>
              ${field.description ? `<span class="fix-registry__description fix-registry__description--row">${escape(field.description)}</span>` : ""}
            </div></td>
            <td><code>${escape(field.type || "—")}</code></td>
            <td>${field._usages.map((usage) => badge(usage)).join("")}</td>
            <td>${chips(field.versions)}</td>
            <td>${number.format(list(field.components).length + list(field.used_in).length)}</td>
          </tr>`,
        )
        .join("");
      if (!filtered.length) fieldRows.innerHTML = emptyRow(6);
      updateCount(select("[data-field-count]"), filtered.length, fields.length, found.by);
      updatePager(select("[data-field-pager]"), state.field.page, filtered.length);
    }

    function validPage(current, count) {
      return Math.min(current, Math.max(0, Math.ceil(count / PAGE_SIZE) - 1));
    }

    function page(entries, index) {
      return entries.slice(index * PAGE_SIZE, (index + 1) * PAGE_SIZE);
    }

    function emptyRow(columns) {
      return `<tr><td class="fix-registry__empty" colspan="${columns}">No records match these filters.</td></tr>`;
    }

    function updateCount(output, count, total, by = "") {
      // Which tier answered, because a search that found nothing by name and
      // fell back to the prose has to say so -- otherwise a reader reads a
      // loose match as the strict one they asked for.
      const tier = by === "text" ? " matched in text" : "";
      output.textContent = `${number.format(count)} / ${number.format(total)}${tier}`;
    }

    function updatePager(pager, current, count) {
      const pages = Math.max(1, Math.ceil(count / PAGE_SIZE));
      select("[data-page]", pager).textContent = `Page ${current + 1} / ${pages}`;
      select("[data-previous]", pager).disabled = current === 0;
      select("[data-next]", pager).disabled = current + 1 >= pages;
      pager.hidden = count <= PAGE_SIZE;
    }

    function componentLink(name) {
      const found = componentByName.get(normalized(name));
      return found
        ? `<a class="fix-registry__name" href="${href("component", found.name)}">${escape(found.name)}</a>`
        : `<code>${escape(name)}</code>`;
    }

    function messageLink(name) {
      const query = new URLSearchParams({ fq: name });
      return `<a class="fix-registry__name" href="?${query}#fields" data-message-filter="${escape(name)}">${escape(name)}</a>`;
    }

    function findField(member) {
      return (
        (member.tag !== undefined && fieldByTag.get(String(member.tag))) ||
        fieldByName.get(normalized(member.name))
      );
    }

    function fieldLink(member) {
      const found = findField(member);
      return found
        ? `<a class="fix-registry__name" href="${href("field", found.tag ?? found.name)}">${escape(found.name)}</a>`
        : `<code>${escape(member.name)}</code>`;
    }

    function renderComponentDetail(component) {
      const relatedFields = componentFields.get(normalized(component.name)) || [];
      const owners = componentBacklinks.get(normalized(component.name)) || [];
      componentDetail.innerHTML = `<header>
          <div><p class="fix-registry__eyebrow">${escape(shapeLabel(component._shape))}</p><h3>${escape(component.name)}</h3></div>
          <a class="fix-registry__detail-close" data-detail-close href="#components-title">Close</a>
        </header>
        <dl>
          <dt>Versions</dt><dd>${chips(component.versions)}</dd>
          <dt>MsgType</dt><dd>${component._msgType ? `<code>${escape(component._msgType)}</code>` : "—"}</dd>
          <dt>Members</dt><dd>${number.format(component._members.length)}</dd>
          ${aliasDefinition(component.aliases)}
        </dl>
        ${owners.length ? `<h4>Referenced by components</h4><p>${owners.map((owner) => componentLink(owner.name)).join(" · ")}</p>` : ""}
        ${relatedFields.length ? `<h4>Fields</h4><p>${relatedFields.map((field) => `${fieldLink(field)} ${tagCode(field.tag, true)}`).join(" · ")}</p>` : ""}
        <h4>Member tree</h4>
        ${memberTree(component._tree)}
        <a class="fix-registry__source" href="${escape(`${app.dataset.repository}/components/${component.slug}.json`)}">View repository record →</a>`;
      componentDetail.hidden = false;
    }

    function renderFieldDetail(field) {
      const declared = new Map(list(field.values).map((one) => [String(one.value), one]));
      for (const code of [
        ...Object.keys(object(field.states)),
        ...Object.keys(object(field.event_types)),
      ]) {
        if (!declared.has(code)) declared.set(code, { value: code, meaning: "", aliases: [] });
      }
      const valueCodes = [...declared.values()];
      const encoded = encodings(field);
      const source =
        field.tag === undefined
          ? "fields/named.json"
          : `fields/${String(Math.floor(Number(field.tag) / 500)).padStart(6, "0")}.json`;
      const componentReferences = list(field.components);
      const messageReferences = list(field.used_in);
      fieldDetail.innerHTML = `<header>
          <div><p class="fix-registry__eyebrow">${field.tag === undefined ? "Namespace" : `Tag ${escape(field.tag)}`}</p><h3>${escape(field.name)}</h3></div>
          <a class="fix-registry__detail-close" data-detail-close href="#fields-title">Close</a>
        </header>
        ${field.description ? `<p class="fix-registry__description fix-registry__description--detail">${escape(field.description)}</p>` : ""}
        <dl>
          ${field.tag === undefined ? "" : `<dt>Tag</dt><dd>${tagCode(field.tag)}</dd>`}
          <dt>Datatype</dt><dd><code>${escape(field.type || "—")}</code></dd>
          <dt>Versions</dt><dd>${chips(field.versions)}</dd>
          ${field.column ? `<dt>Column</dt><dd><code>${escape(field.column)}</code></dd>` : ""}
          ${field.kind ? `<dt>Kind</dt><dd>${badge(field.kind)}</dd>` : ""}
          ${field.note ? `<dt>Note</dt><dd>${escape(field.note)}</dd>` : ""}
          ${aliasDefinition(field.aliases)}
        </dl>
        ${componentReferences.length ? `<h4>Components</h4><p>${componentReferences.map((name) => componentLink(name)).join(" · ")}</p>` : ""}
        ${messageReferences.length ? `<h4>Messages</h4><p>${messageReferences.map((name) => messageLink(name)).join(" · ")}</p>` : ""}
        ${valueCodes.length ? valueTable(field, valueCodes) : ""}
        ${Object.keys(encoded).length ? encodingTable(encoded) : ""}
        <a class="fix-registry__source" href="${escape(`${app.dataset.repository}/${source}`)}">View repository record →</a>`;
      fieldDetail.hidden = false;
    }

    function aliasDefinition(aliases) {
      if (!list(aliases).length) return "";
      return `<dt>Aliases</dt><dd>${aliases
        .map((alias) => {
          const source = alias.source ? ` · ${alias.source}` : "";
          return `<span class="fix-registry__chip">${escape(alias.name)}${escape(source)}</span>`;
        })
        .join("")}</dd>`;
    }

    function memberTree(members) {
      if (!list(members).length) return '<p class="fix-registry__muted">No members.</p>';
      return `<ul class="fix-registry__tree">${members
        .map((member) => {
          const field = member.kind === "component" ? null : findField(member);
          const named =
            member.kind === "component" ? componentLink(member.name) : fieldLink(member);
          const description = field?.description || member.description;
          const nested = list(member.members).length ? memberTree(member.members) : "";
          return `<li><div class="fix-registry__member-line">${badge(member.kind)} ${named} ${tagCode(member.tag ?? field?.tag, true)} ${
            member.required ? badge("required", "required") : badge("optional", "optional")
          }</div>${description ? `<span class="fix-registry__description fix-registry__description--member">${escape(description)}</span>` : ""}${nested}</li>`;
        })
        .join("")}</ul>`;
    }

    function valueTable(field, values) {
      return `<h4>Values</h4><div class="fix-registry__table-wrap fix-registry__detail-table"><table class="fix-registry__table">
        <thead><tr><th>Wire</th><th>Meaning</th><th>State / event</th></tr></thead>
        <tbody>${values
          .map((one) => {
            const code = String(one.value);
            const enumText = object(field.states)[code] || object(field.event_types)[code] || "";
            return `<tr><td><code>${escape(code)}</code></td><td>${escape(one.meaning || "")}</td><td>${escape(enumText)}</td></tr>`;
          })
          .join("")}</tbody></table></div>`;
    }

    function encodingTable(encoded) {
      const entries = Object.entries(object(encoded));
      return `<details><summary>Encoded spellings (${number.format(entries.length)})</summary>
        <div class="fix-registry__table-wrap fix-registry__detail-table"><table class="fix-registry__table">
        <thead><tr><th>Spelling</th><th>Wire</th></tr></thead><tbody>${entries
          .map(
            ([spelling, wire]) =>
              `<tr><td><code>${escape(spelling)}</code></td><td><code>${escape(wire)}</code></td></tr>`,
          )
          .join("")}</tbody></table></div></details>`;
    }

    function route(scroll = true) {
      const parameters = new URLSearchParams(window.location.hash.slice(1));
      const componentName = parameters.get("component");
      const fieldKey = parameters.get("field");
      componentDetail.hidden = true;
      fieldDetail.hidden = true;
      status.hidden = true;
      delete status.dataset.error;
      if (componentName) {
        const found = componentByName.get(normalized(componentName));
        if (found) {
          renderComponentDetail(found);
          if (scroll) {
            componentDetail.focus({ preventScroll: true });
            componentDetail.scrollIntoView({
              behavior: reducedMotion ? "auto" : "smooth",
              block: "start",
            });
          }
        } else {
          routeError("component", componentName, scroll);
        }
      } else if (fieldKey) {
        const found = fieldByTag.get(fieldKey) || fieldByName.get(normalized(fieldKey));
        if (found) {
          renderFieldDetail(found);
          if (scroll) {
            fieldDetail.focus({ preventScroll: true });
            fieldDetail.scrollIntoView({
              behavior: reducedMotion ? "auto" : "smooth",
              block: "start",
            });
          }
        } else {
          routeError("field", fieldKey, scroll);
        }
      }
    }

    function routeError(kind, key, scroll) {
      status.dataset.error = "";
      status.textContent = `FIX ${kind} “${key}” was not found.`;
      status.hidden = false;
      if (scroll) {
        status.scrollIntoView({
          behavior: reducedMotion ? "auto" : "smooth",
          block: "start",
        });
      }
    }

    function renderAll() {
      renderComponents();
      renderFields();
      route(false);
    }

    readState();
    ready.hidden = false;
    renderComponents();
    renderFields();
    route(Boolean(window.location.hash.includes("=")));

    window.addEventListener("hashchange", () => route());
    window.addEventListener("popstate", () => {
      readState();
      renderAll();
    });
  }

  function flattenMembers(members) {
    return list(members).flatMap((member) => [member, ...flattenMembers(member.members)]);
  }

  function componentShape(component) {
    if (component._msgType) return "message";
    const kinds = new Set(component._members.map((member) => member.kind));
    if (kinds.has("group")) return "repeating";
    if (kinds.has("component")) return "composed";
    return "flat";
  }

  function shapeLabel(shape) {
    return { message: "Message", repeating: "Repeating group", composed: "Composed", flat: "Flat" }[
      shape
    ];
  }

  function fieldUsages(field) {
    const usages = [];
    if (field.tag === undefined) usages.push("namespace");
    if (list(field.values).length) usages.push("enumerated");
    if (list(field.components).length) usages.push("component");
    if (list(field.used_in).length) usages.push("message");
    return usages.length ? usages : ["plain"];
  }
})();
