/*
 * The browser half of the FIX section: one scanner, three widgets.
 *
 * The scanner mirrors the Rust core's own: the same frame location, the same
 * six separator spellings, the same printed-SOH unescape, the same key/value
 * bounds, the same "a checksum closes the message" rule, and the same media
 * type and direction taxonomies -- so a line decoded on this page resolves the
 * way `parse_fix` resolves it. The dictionary is `assets/fix-registry.json`,
 * generated from rekep's bundled FIX registry.
 *
 * Nothing here parses on the server, and nothing is uploaded: a pasted line
 * stays in the tab it was pasted into.
 */
(function () {
  "use strict";

  const REGISTRY_URL = "fix-registry.json";
  const DETAILS_URL = "fix-details.json";

  /* How many entries are put in the DOM at once. The dictionary is six
   * thousand fields; a browser that builds them all pays for a list nobody
   * scrolls. The count line says what the filter matched either way. */
  const RENDER_LIMIT = 150;
  const SEPARATORS = [
    { name: "SOH", wire: "", shown: "\\x01" },
    { name: "pipe", wire: "|", shown: "|" },
    { name: "caret-A", wire: "^A", shown: "^A" },
    { name: "escaped", wire: "\\x01", shown: "\\\\x01" },
    { name: "<SOH>", wire: "<SOH>", shown: "<SOH>" },
    { name: "{SOH}", wire: "{SOH}", shown: "{SOH}" },
  ];
  const FIELD_START = new Set(["", "|", " ", "\t", "[", "(", "{", ">"]);
  const FIELD_END = new Set(["", "|", " ", "\t", "\r", "\n", "]", ")", "}", ",", ";"]);
  const MARKERS = ["^A", "\\x01", "<SOH>", "{SOH}"];
  const TRAILING = new Set([" ", "\t", "\r", "\n", "]", ")", "}", ",", ";"]);

  /* ---------------------------------------------------------------- scanner */

  function isFieldStart(line, at) {
    if (at === 0) return true;
    if (FIELD_START.has(line[at - 1])) return true;
    return MARKERS.some((marker) => line.slice(at - marker.length, at) === marker);
  }

  function isFieldEnd(line, at) {
    if (FIELD_END.has(line[at])) return true;
    return MARKERS.some((marker) => line.startsWith(marker, at));
  }

  /* One `tag=` or `NAME=` pair opening at `at`, or null. */
  function pairAt(line, at) {
    if (!isFieldStart(line, at)) return null;
    let start = at;
    const marked = line[start] === "#";
    if (marked) start += 1;
    const first = line[start];
    if (first === undefined) return null;
    let position = start;
    let key;
    if (first >= "0" && first <= "9") {
      while (position < line.length && line[position] >= "0" && line[position] <= "9") position += 1;
      key = { tag: Number(line.slice(start, position)), name: null };
    } else if (/[A-Za-z_]/.test(first)) {
      position = start + 1;
      while (position < line.length && /[A-Za-z0-9_.\-]/.test(line[position])) position += 1;
      key = { tag: null, name: line.slice(start, position) };
    } else {
      return null;
    }
    if (line[position] !== "=") return null;
    const value = line[position + 1];
    if (value === undefined || value === "'" || value === '"' || isFieldEnd(line, position + 1)) {
      return null;
    }
    return { key: key, equals: position, marked: marked };
  }

  /* Where the frame opens: tag 8 first, then tag 35, then the first pair. */
  function locateFrame(line) {
    let first = null;
    let msgtype = null;
    for (let at = 0; at < line.length; at += 1) {
      const found = pairAt(line, at);
      if (!found) continue;
      const candidate = { at: at, numeric: found.key.tag !== null };
      if (first === null) first = candidate;
      if (found.key.tag === 8) return withSeparator(line, candidate);
      if (found.key.tag === 35 && msgtype === null) msgtype = candidate;
    }
    const chosen = msgtype || first;
    return chosen === null ? null : withSeparator(line, chosen);
  }

  /* The earliest separator spelling the frame uses, or whitespace. */
  function withSeparator(line, frame) {
    const tail = line.slice(frame.at);
    if (!frame.numeric) {
      return Object.assign({}, frame, {
        separator: tail.includes("|") ? { wire: "|" } : { whitespace: true },
      });
    }
    let held = null;
    for (const separator of SEPARATORS) {
      const found = tail.indexOf(separator.wire);
      if (found >= 0 && (held === null || found < held.found)) {
        held = { found: found, separator: { wire: separator.wire, name: separator.name } };
      }
    }
    return Object.assign({}, frame, {
      separator: held === null ? { whitespace: true } : held.separator,
    });
  }

  function segment(line, at, separator) {
    if (separator.whitespace) {
      let end = at;
      while (end < line.length && !/\s/.test(line[end])) end += 1;
      let next = end;
      while (next < line.length && /\s/.test(line[next])) next += 1;
      return [end, next];
    }
    const found = line.indexOf(separator.wire, at);
    if (found < 0) return [line.length, line.length];
    return [found, found + separator.wire.length];
  }

  const SOH = "\u0001";
  const SOH_MARKERS = ["^A", "\\x01", "<SOH>", "{SOH}"];

  /* A capture that cannot print `0x01` writes one of these instead. */
  function unescaped(line) {
    if (line.includes(SOH)) return line;
    let held = null;
    for (const marker of SOH_MARKERS) {
      const at = line.indexOf(marker);
      if (at >= 0 && (held === null || at < held.at)) held = { at: at, marker: marker };
    }
    return held === null ? line : line.split(held.marker).join(SOH);
  }

  /* Every wire pair the frame carries, in arrival order. */
  function scan(line) {
    line = unescaped(line);
    const frame = locateFrame(line);
    if (frame === null) return { frame: null, entries: [] };
    const entries = [];
    let offset = frame.at;
    while (offset < line.length) {
      while (offset < line.length && /\s/.test(line[offset])) offset += 1;
      const start = offset;
      const [end, next] = segment(line, start, frame.separator);
      offset = next;
      const found = pairAt(line, start);
      if (!found) {
        if (next >= line.length) break;
        continue;
      }
      if (found.equals >= end) continue;
      let stop = end;
      while (stop > found.equals + 1 && TRAILING.has(line[stop - 1])) stop -= 1;
      if (stop <= found.equals + 1) continue;
      entries.push({
        tag: found.key.tag,
        name: found.key.name,
        marked: found.marked,
        key: found.key.tag === null ? found.key.name : String(found.key.tag),
        value: line.slice(found.equals + 1, stop),
        at: start,
      });
      // A checksum closes the message: whatever follows it is prose.
      if (found.key.tag === 10) break;
    }
    return { frame: frame, entries: entries };
  }

  /* The `MSGTYPE=` a log prefix may carry before the numeric frame. */
  function namedValue(line, wanted) {
    for (let at = 0; at < line.length; at += 1) {
      const found = pairAt(line, at);
      if (!found || found.key.name === null) continue;
      if (found.key.name.toUpperCase() !== wanted) continue;
      let end = found.equals + 1;
      while (end < line.length && !isFieldEnd(line, end) && !"^<{\\".includes(line[end])) end += 1;
      if (end > found.equals + 1) return line.slice(found.equals + 1, end);
    }
    return null;
  }

  /* ------------------------------------------------------------ inference */

  /* The media type, MsgType and direction the raw layer names for one line.
   *
   * The taxonomy is the core's `LineInference::mime_type`: numeric tags prove
   * FIX, a `#`-marked key or a raw `MSGTYPE=` proves a bridge row, both prove
   * the mixed form, an XML payload in tag 213 proves FIXML, a line that is
   * still `key=value` throughout is the generic key/value shape, and a
   * document that opens as XML or JSON is that document.
   */
  function classify(line, dictionary) {
    const scanned = scan(line);
    let hasTag = false;
    let hasPairs = false;
    let hasSymbolic = false;
    let hasXml = false;
    let tagMsgtype = null;
    const named = namedValue(line, "MSGTYPE");
    if (named !== null) {
      hasPairs = true;
      hasSymbolic = true;
    }
    for (const entry of scanned.entries) {
      hasPairs = true;
      if (entry.marked) hasSymbolic = true;
      if (entry.tag !== null) {
        hasTag = true;
        if (entry.tag === 213) {
          if (entry.value.startsWith("<")) hasXml = true;
          else if (entry.value.includes("=")) hasSymbolic = true;
        }
        if (entry.tag === 35 && tagMsgtype === null) tagMsgtype = entry.value;
      } else if (scanned.frame && scanned.frame.numeric) {
        hasSymbolic = true;
      }
    }
    if (!scanned.frame) hasPairs = hasPairs || hasAnyPair(line);
    let mimetype;
    if (hasXml) mimetype = "text/fixml";
    else if (hasTag && hasSymbolic) mimetype = "text/fixul";
    else if (hasTag) mimetype = "text/fix";
    else if (hasSymbolic) mimetype = "text/ullink";
    else if (hasPairs) mimetype = "text/key-value";
    else mimetype = "application/octet-stream";
    if (mimetype === "application/octet-stream" || mimetype === "text/key-value") {
      const document = documentType(line);
      if (document) mimetype = document;
    }
    let msgtype = named !== null ? named : tagMsgtype;
    if (msgtype && msgtype.length > 1 && msgtype[0] === "U" && /^[A-Za-z0-9]+$/.test(msgtype.slice(1))) {
      msgtype = "UDF";
    }
    void dictionary;
    return {
      mimetype: mimetype,
      msgtype: msgtype === null ? "unknown" : msgtype,
      msgdirection: direction(line, "SENT"),
      scanned: scanned,
    };
  }

  /* Whether the line holds any symbolic pair at all, marked or not. */
  function hasAnyPair(line) {
    for (let at = 0; at < line.length; at += 1) {
      const found = pairAt(line, at);
      if (found && found.key.name !== null) return true;
    }
    return false;
  }

  /* The media type one whole document opens as, before any pair rule runs. */
  function documentType(line) {
    const trimmed = line.trim();
    if (!trimmed) return null;
    if (trimmed[0] === "<") {
      return /[A-Za-z?!/]/.test(trimmed[1] || "") ? "application/xml" : null;
    }
    if (trimmed[0] === "{" || trimmed[0] === "[") {
      const last = trimmed[trimmed.length - 1];
      return last === "}" || last === "]" ? "application/json" : null;
    }
    return null;
  }

  /* The verbs the core reads a direction from, in its own order. */
  const VERBS = [
    ["sending", "SENT", false], ["sent", "SENT", false], ["send", "SENT", false],
    ["outbound", "SENT", false], ["outgoing", "SENT", false], ["out", "SENT", true],
    ["receiving", "RECV", false], ["received", "RECV", false], ["receive", "RECV", false],
    ["recv", "RECV", false], ["inbound", "RECV", false], ["incoming", "RECV", false],
    ["in", "RECV", true],
  ];

  /* A bare `in` or `out` is chosen only at the start of the prefix or after a
   * bracket, and only where a delimiter closes it: `direct:out` is a route
   * endpoint and `MCFID-IN-XPAR` is a session name. It still counts against an
   * opposite verb, which is what makes `sending in session 3` answer nothing.
   */
  function bareMarker(prefix, at, end) {
    const opens = at === 0 || "[(".includes(prefix[at - 1]);
    const closes = end >= prefix.length || "]):".includes(prefix[end]);
    return opens && closes;
  }

  /* Which way a line moved, read from the prose in front of its frame.
   *
   * A prefix carrying both directions answers nothing rather than guessing;
   * a prefix carrying none takes the capture's declared default.
   */
  function direction(line, fallback) {
    const frame = locateFrame(line);
    const prefix = (frame === null ? line : line.slice(0, frame.at)).toLowerCase();
    let found = null;
    let conflict = false;
    for (const [verb, moved, bare] of VERBS) {
      const at = prefix.search(new RegExp("\\b" + verb + "\\b"));
      if (at < 0) continue;
      // A bare verb still counts against an opposite one even where the
      // shape rule would not let it be chosen.
      if (found !== null && found.moved !== moved) conflict = true;
      if (bare && !bareMarker(prefix, at, at + verb.length)) continue;
      if (found === null || at < found.at) found = { at: at, moved: moved };
    }
    if (conflict) return "unknown";
    return found === null ? fallback : found.moved;
  }

  /* -------------------------------------------------------------- typing */

  /* One wire value as the declared Arrow type reads it, or a stated refusal. */
  function typed(field, raw) {
    const text = raw.trim();
    if (!field) return { text: text, note: "no dictionary entry" };
    const type = field.type;
    if (!text) return { text: null, note: "empty" };
    if (/^(int|uint)/.test(type)) {
      return /^[+-]?\d+$/.test(text)
        ? { text: text }
        : { text: null, note: "not an integer -- stays null" };
    }
    if (/^(float|double|decimal)/.test(type)) {
      return /^[+-]?\d*\.?\d+([eE][+-]?\d+)?$/.test(text)
        ? { text: text }
        : { text: null, note: "not a number -- stays null" };
    }
    if (type.startsWith("timestamp")) {
      const instant = rfc3339(text);
      return instant ? { text: instant } : { text: null, note: "not a UTCTimestamp -- stays null" };
    }
    if (type.startsWith("date")) {
      return /^\d{8}$/.test(text)
        ? { text: text.slice(0, 4) + "-" + text.slice(4, 6) + "-" + text.slice(6, 8) }
        : { text: text };
    }
    if (type === "bool") {
      return { text: ["Y", "y", "true", "TRUE", "1"].includes(text) ? "true" : "false" };
    }
    return { text: text };
  }

  /* `YYYYMMDD-HH:MM:SS[.frac]` as RFC 3339, the spelling the cast reads. */
  function rfc3339(text) {
    const split = text.split(/[.,]/);
    const head = split[0];
    const digits = (split[1] || "").replace(/\D/g, "");
    const micros = (digits + "000000").slice(0, 6);
    if (/^\d{8}-\d{2}:\d{2}:\d{2}$/.test(head)) {
      return (
        head.slice(0, 4) + "-" + head.slice(4, 6) + "-" + head.slice(6, 8) +
        "T" + head.slice(9) + "." + micros + "Z"
      );
    }
    if (/^\d{14}$/.test(head)) {
      return (
        head.slice(0, 4) + "-" + head.slice(4, 6) + "-" + head.slice(6, 8) +
        "T" + head.slice(8, 10) + ":" + head.slice(10, 12) + ":" + head.slice(12, 14) +
        "." + micros + "Z"
      );
    }
    return null;
  }

  /* -------------------------------------------------------------- helpers */

  function element(tag, attributes, children) {
    const node = document.createElement(tag);
    for (const key in attributes || {}) {
      if (key === "class") node.className = attributes[key];
      else if (key === "text") node.textContent = attributes[key];
      else if (key === "html") node.innerHTML = attributes[key];
      else node.setAttribute(key, attributes[key]);
    }
    for (const child of children || []) node.appendChild(child);
    return node;
  }

  function table(headers, rows) {
    const head = element("tr", null, headers.map((name) => element("th", { text: name })));
    const body = rows.map((row) =>
      element(
        "tr",
        null,
        row.map((cell) =>
          cell && cell.node ? element("td", null, [cell.node]) : element("td", { text: cell === null || cell === undefined ? "" : String(cell) })
        )
      )
    );
    return element("div", { class: "fix-scroll" }, [
      element("table", { class: "fix-table" }, [
        element("thead", null, [head]),
        element("tbody", null, body),
      ]),
    ]);
  }

  function details(summary, body, open) {
    const node = element("details", { class: "fix-details" }, [
      element("summary", { text: summary }),
      body,
    ]);
    if (open) node.setAttribute("open", "open");
    return node;
  }

  function note(text, kind) {
    return element("p", { class: "fix-note fix-" + (kind || "info"), text: text });
  }

  /* ------------------------------------------------------------- widgets */

  function registryBrowser(mount, dictionary) {
    const search = element("input", {
      type: "search",
      class: "fix-input",
      placeholder: "Tag, name, alias, or description",
      "aria-label": "Search the dictionary",
    });
    const shape = element("select", { class: "fix-input", "aria-label": "Shape" });
    for (const [label, value] of [["Fields and groups", ""], ["Fields", "field"], ["Repeating groups", "group"]]) {
      shape.appendChild(element("option", { value: value, text: label }));
    }
    const branch = element("select", { class: "fix-input", "aria-label": "Branch" });
    branch.appendChild(element("option", { value: "", text: "All branches" }));
    for (const name of dictionary.branches) {
      branch.appendChild(element("option", { value: name, text: name }));
    }
    const count = element("p", { class: "fix-count" });
    const results = element("div");

    function render() {
      const terms = search.value.toLowerCase().split(/\s+/).filter(Boolean);
      const visible = dictionary.fields.filter((field) => {
        if (shape.value && field.shape !== shape.value) return false;
        if (branch.value && field.branch !== branch.value) return false;
        return terms.every((term) => field.search.includes(term));
      });
      const groups = visible.filter((field) => field.shape === "group").length;
      const shown = visible.slice(0, RENDER_LIMIT);
      count.textContent =
        visible.length.toLocaleString() + " of " +
        dictionary.fields.length.toLocaleString() + " definitions · " +
        groups.toLocaleString() + " repeating groups" +
        (visible.length > shown.length
          ? " · showing the first " + shown.length + ", narrow the search for the rest"
          : "");
      results.textContent = "";
      if (!visible.length) {
        results.appendChild(note("No definition matches these filters.", "warn"));
        return;
      }
      for (const field of shown) results.appendChild(fieldCard(field));
    }

    search.addEventListener("input", render);
    shape.addEventListener("change", render);
    branch.addEventListener("change", render);
    mount.appendChild(
      element("div", { class: "fix-controls" }, [
        element("div", { class: "fix-grow" }, [search]),
        shape,
        branch,
      ])
    );
    mount.appendChild(count);
    mount.appendChild(results);
    render();
  }

  let details$ = null;

  /* The members, code sets and lineage, fetched once when one is first asked
   * for. Five megabytes of them is not what a page load owes a reader. */
  function deepRecords() {
    if (details$ === null) {
      details$ = fetch(new URL(DETAILS_URL, resolveAssets())).then(function (answer) {
        if (!answer.ok) throw new Error("details " + answer.status);
        return answer.json();
      });
    }
    return details$;
  }

  /* One dictionary entry, collapsed to its heading until it is opened. */
  function fieldCard(field) {
    const body = element("div", { class: "fix-card" });
    body.appendChild(
      table(
        ["property", "value"],
        [
          ["tag", field.tag],
          ["identifier", field.id],
          ["canonical name", field.name],
          ["branch", field.branch],
          ["Arrow type", field.type],
          ["nullable", String(field.nullable)],
          ["alternate tags", field.tags.join(", ")],
          ["aliases", field.aliases.join(", ")],
        ].filter((row) => row[1] !== "" && row[1] !== null && row[1] !== undefined)
      )
    );
    if (field.description) body.appendChild(element("p", { class: "fix-doc", text: field.description }));
    const deep = element("div");
    body.appendChild(deep);
    const heading =
      field.tag + " · " + field.display +
      (field.shape === "group" ? "  [group]" : "") +
      "  — " + field.type;
    const card = details(heading, body, false);
    card.addEventListener(
      "toggle",
      function () {
        if (!card.open || card.dataset.fixDeep) return;
        card.dataset.fixDeep = "1";
        deep.appendChild(note("Reading the dictionary…", "info"));
        deepRecords().then(
          function (held) {
            deep.textContent = "";
            for (const part of deepParts(held[String(field.tag)] || {})) {
              deep.appendChild(part);
            }
          },
          function (error) {
            deep.textContent = "";
            deep.appendChild(note("Could not read them: " + error.message, "warn"));
          }
        );
      },
      false
    );
    return card;
  }

  /* One entry's members, code set and lineage, where the dictionary has them. */
  function deepParts(held) {
    const parts = [];
    const members = held.members || [];
    const codes = held.codes || [];
    const lineage = held.lineage || [];
    if (members.length) {
      parts.push(
        details(
          "Members (" + members.length + ")",
          table(
            ["path", "tag", "name", "type"],
            members.map((member) => [member.path, member.tag, member.name, member.type])
          )
        )
      );
    }
    if (codes.length) {
      parts.push(
        details(
          "Code set (" + codes.length + ")",
          table(
            ["value", "name", "since"],
            codes.map((code) => [code.value, code.name, code.since || ""])
          )
        )
      );
    }
    if (lineage.length) {
      parts.push(
        details(
          "Lineage (" + lineage.length + ")",
          table(
            ["since", "name", "FIX type"],
            lineage.map((entry) => [entry.since || "", entry.name || "", entry.type || ""])
          )
        )
      );
    }
    if (!parts.length) {
      parts.push(note("This definition carries no members, code set or lineage.", "info"));
    }
    return parts;
  }

  function decoder(mount, dictionary) {
    // The code sets name a value; they arrive once, and the table is drawn
    // again when they do rather than blocking the first decode on them.
    let codes = {};
    deepRecords().then(function (held) {
      codes = held;
      render();
    }, function () {});
    const input = element("textarea", {
      class: "fix-input fix-area",
      rows: "4",
      spellcheck: "false",
      "aria-label": "One captured line",
    });
    input.value =
      "2026-08-14 00:05:01,148 [77-e725] [FixSession_XPAR] (INFO) sending >> " +
      "8=FIX.4.2|9=176|35=D|34=1092|49=BUYSIDE|56=XPAR|52=20260814-00:05:01.147|" +
      "11=ORD-0000038106|55=TTF|54=1|38=1200|40=2|44=41.2500|59=0|10=203|";
    const output = element("div");

    function render() {
      const line = input.value.replace(/\n+$/, "");
      output.textContent = "";
      if (!line.trim()) {
        output.appendChild(note("Paste a captured line to decode it.", "info"));
        return;
      }
      const found = classify(line, dictionary);
      output.appendChild(
        table(
          ["mimetype", "msgtype", "msgdirection", "frame at", "separator", "pairs"],
          [[
            found.mimetype,
            found.msgtype,
            found.msgdirection,
            found.scanned.frame ? found.scanned.frame.at : "none",
            found.scanned.frame
              ? found.scanned.frame.separator.whitespace
                ? "whitespace"
                : found.scanned.frame.separator.name || "pipe"
              : "none",
            found.scanned.entries.length,
          ]]
        )
      );
      if (!found.scanned.entries.length) {
        output.appendChild(note("No FIX-shaped frame here: parse_fix leaves this record in logs.messages.", "warn"));
        return;
      }
      const resolved = [];
      const unmapped = [];
      for (const entry of found.scanned.entries) {
        const field = entry.tag !== null
          ? dictionary.byTag.get(entry.tag)
          : dictionary.byName.get(entry.name.toLowerCase());
        if (!field) {
          unmapped.push([entry.key, entry.value]);
          continue;
        }
        const read = typed(field, entry.value);
        resolved.push([
          field.tag,
          field.display,
          entry.key,
          entry.value,
          read.text === null ? "" : read.text,
          read.note || "",
          codeName(field, entry.value),
        ]);
      }
      output.appendChild(
        details(
          "Resolved columns (" + resolved.length + ")",
          table(
            ["column", "field", "wire key", "raw value", "typed value", "note", "code"],
            resolved
          ),
          true
        )
      );
      output.appendChild(
        details(
          "Unmapped pairs (" + unmapped.length + ")",
          unmapped.length
            ? table(["key", "value"], unmapped)
            : note("Every pair resolved against this dictionary.", "ok"),
          unmapped.length > 0
        )
      );
      output.appendChild(
        details(
          "entries (" + found.scanned.entries.length + ")",
          table(
            ["#", "key", "value", "offset"],
            found.scanned.entries.map((entry, index) => [index, entry.key, entry.value, entry.at])
          )
        )
      );
    }

    function codeName(field, value) {
      const held = codes[String(field.tag)];
      if (!held) return "";
      const found = (held.codes || []).find((code) => code.value === value.trim());
      return found ? found.name : "";
    }

    input.addEventListener("input", render);
    mount.appendChild(input);
    mount.appendChild(output);
    render();
  }

  function encoder(mount, dictionary) {
    const rows = element("div", { class: "fix-rows" });
    const separator = element("select", { class: "fix-input", "aria-label": "Separator" });
    for (const spelling of SEPARATORS) {
      separator.appendChild(element("option", { value: spelling.wire, text: spelling.name }));
    }
    separator.value = "|";
    const output = element("pre", { class: "fix-wire" });
    const report = element("div");

    function line() {
      const node = element("div", { class: "fix-row" });
      const tag = element("input", {
        class: "fix-input fix-tag",
        list: "fix-tags",
        placeholder: "tag or name",
        "aria-label": "Tag",
      });
      const value = element("input", { class: "fix-input", placeholder: "value", "aria-label": "Value" });
      const drop = element("button", { class: "fix-button", type: "button", text: "×", title: "Remove" });
      drop.addEventListener("click", function () {
        node.remove();
        render();
      });
      tag.addEventListener("input", render);
      value.addEventListener("input", render);
      node.appendChild(tag);
      node.appendChild(value);
      node.appendChild(drop);
      return node;
    }

    function resolve(spelling) {
      const key = spelling.trim().toLowerCase();
      if (!key) return null;
      if (/^\d+$/.test(key)) return dictionary.byTag.get(Number(key)) || null;
      return dictionary.byName.get(key) || null;
    }

    function render() {
      const wire = separator.value;
      const pairs = [];
      const unknown = [];
      for (const node of rows.querySelectorAll(".fix-row")) {
        const inputs = node.querySelectorAll("input");
        const spelling = inputs[0].value.trim();
        const value = inputs[1].value;
        if (!spelling || !value) continue;
        const field = resolve(spelling);
        if (!field) {
          unknown.push(spelling);
          continue;
        }
        pairs.push({ tag: field.tag, field: field, value: value });
      }
      report.textContent = "";
      if (!pairs.length) {
        output.textContent = "";
        report.appendChild(note("Add at least one resolved tag to build a frame.", "info"));
        return;
      }
      const head = pairs.filter((pair) => pair.tag === 8 || pair.tag === 9 || pair.tag === 35);
      const rest = pairs.filter((pair) => pair.tag !== 8 && pair.tag !== 9 && pair.tag !== 35 && pair.tag !== 10);
      const begin = head.find((pair) => pair.tag === 8) || { tag: 8, value: "FIX.4.4" };
      const msgtype = head.find((pair) => pair.tag === 35);
      const ordered = [];
      if (msgtype) ordered.push(msgtype);
      for (const pair of rest) ordered.push(pair);
      const body = ordered.map((pair) => pair.tag + "=" + pair.value + wire).join("");
      const length = body.length;
      const framed = "8=" + begin.value + wire + "9=" + length + wire + body;
      const sum = checksum(framed);
      const complete = framed + "10=" + sum + wire;
      output.textContent = complete.split("").join("\\x01");
      report.appendChild(
        table(
          ["BodyLength (9)", "CheckSum (10)", "pairs", "bytes"],
          [[length, sum, ordered.length + 3, complete.length]]
        )
      );
      if (unknown.length) {
        report.appendChild(
          note("Not in this dictionary, so not encoded: " + unknown.join(", "), "warn")
        );
      }
      report.appendChild(
        details(
          "Field-by-field",
          table(
            ["tag", "field", "Arrow type", "value"],
            ordered.map((pair) => [pair.tag, pair.field.display, pair.field.type, pair.value])
          )
        )
      );
    }

    function checksum(text) {
      let total = 0;
      for (let at = 0; at < text.length; at += 1) total += text.charCodeAt(at) & 0xff;
      return String(total % 256).padStart(3, "0");
    }

    const add = element("button", { class: "fix-button", type: "button", text: "Add field" });
    add.addEventListener("click", function () {
      rows.appendChild(line());
      render();
    });
    separator.addEventListener("change", render);

    const list = element("datalist", { id: "fix-tags" });
    for (const field of dictionary.fields) {
      list.appendChild(element("option", { value: String(field.tag), label: field.display }));
    }
    mount.appendChild(list);
    mount.appendChild(rows);
    mount.appendChild(element("div", { class: "fix-controls" }, [add, separator]));
    mount.appendChild(output);
    mount.appendChild(report);

    for (const seed of [["8", "FIX.4.4"], ["35", "D"], ["55", "TTF"], ["54", "1"], ["38", "1200"], ["44", "41.2500"]]) {
      const node = line();
      const inputs = node.querySelectorAll("input");
      inputs[0].value = seed[0];
      inputs[1].value = seed[1];
      rows.appendChild(node);
    }
    render();
  }

  /* ---------------------------------------------------------------- mount */

  const WIDGETS = { registry: registryBrowser, decode: decoder, encode: encoder };
  let loading = null;

  function dictionaryOf(payload) {
    const byTag = new Map();
    const byName = new Map();
    for (const field of payload.fields) {
      field.aliases = field.aliases || [];
      field.tags = field.tags || [];
      field.search = [
        String(field.tag),
        field.name,
        field.display,
        field.branch,
        field.description,
      ]
        .concat(field.aliases, field.tags.map(String))
        .join(" ")
        .toLowerCase();
      if (field.tag !== null) byTag.set(field.tag, field);
      byName.set(field.name.toLowerCase(), field);
      for (const alias of field.aliases) byName.set(alias.toLowerCase(), field);
      for (const tag of field.tags) if (!byTag.has(tag)) byTag.set(tag, field);
    }
    return { fields: payload.fields, branches: payload.branches, byTag: byTag, byName: byName };
  }

  function mountAll() {
    const mounts = document.querySelectorAll("[data-fix]");
    if (!mounts.length) return;
    if (loading === null) {
      const base = document.querySelector("link[rel=canonical]");
      loading = fetch(new URL(REGISTRY_URL, resolveAssets())).then(function (answer) {
        if (!answer.ok) throw new Error("registry " + answer.status);
        return answer.json();
      });
      void base;
    }
    loading.then(
      function (payload) {
        const dictionary = dictionaryOf(payload);
        for (const mount of mounts) {
          if (mount.dataset.fixMounted) continue;
          mount.dataset.fixMounted = "1";
          const widget = WIDGETS[mount.dataset.fix];
          mount.textContent = "";
          if (widget) widget(mount, dictionary);
        }
      },
      function (error) {
        for (const mount of mounts) {
          mount.textContent = "";
          mount.appendChild(note("The dictionary could not be loaded: " + error.message, "warn"));
        }
      }
    );
  }

  /* `assets/` sits beside the site root whatever page depth asks for it. */
  function resolveAssets() {
    const script = document.currentScript || document.querySelector("script[src*='fix.js']");
    if (script) return script.src;
    return new URL("assets/fix.js", document.baseURI).href;
  }

  const ASSETS = resolveAssets();
  void ASSETS;

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(mountAll);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mountAll);
  } else {
    mountAll();
  }
})();
