(() => {
  "use strict";

  const app = document.querySelector("[data-fix-transcribe]");
  if (!app) return;

  const DECODE_SAMPLE =
    "8=FIX.4.4|35=D|11=ORD-42|54=1|38=100|40=2|9999=raw|10=000|";
  const ENCODE_SAMPLE =
    "BeginString=FIX.4.4|MsgType=NewOrderSingle|ClOrdID=ORD-42|Side=Buy|OrderQty=100|OrdType=Limit|VENDOR.CODE=raw|CheckSum=000|";
  const SEPARATORS = new Map([
    ["soh", "\x01"],
    ["pipe", "|"],
    ["eot-etx", "\x04\x03"],
    ["caret-a", "^A"],
    ["caret", "^"],
    ["semicolon", ";"],
    ["newline", "\n"],
  ]);
  // Multi-character candidates precede any candidate contained inside them.
  const DETECTED_SEPARATORS = ["\x01", "|", "\x04\x03", "^A", "^", ";", "\n"];
  const APPLICATION_VERSIONS = {
    2: "4.0",
    3: "4.1",
    4: "4.2",
    5: "4.3",
    6: "4.4",
    7: "5.0",
    8: "5.0.SP1",
    9: "5.0.SP2",
  };
  const INTEGER_TYPES = new Set([
    "DAYOFMONTH",
    "INT",
    "LENGTH",
    "NUMINGROUP",
    "SEQNUM",
    "TAGNUM",
  ]);
  const NUMBER_TYPES = new Set([
    "AMT",
    "FLOAT",
    "PERCENTAGE",
    "PRICE",
    "PRICEOFFSET",
    "QTY",
  ]);
  const DATE_TYPES = new Set(["LOCALMKTDATE", "UTCDATEONLY"]);
  const TIME_TYPES = new Set(["LOCALMKTTIME", "TZTIMEONLY", "UTCTIMEONLY"]);
  const NAME = /^(?:\d+|[A-Za-z_][A-Za-z0-9_.-]*)(?:\[[A-Za-z0-9_.-]+\])?(?:\.[A-Za-z0-9_.-]+)*$/;
  const number = new Intl.NumberFormat();
  const select = (query, root = app) => root.querySelector(query);
  const list = (value) => (Array.isArray(value) ? value : []);
  const object = (value) => (value && typeof value === "object" ? value : {});
  const own = (mapping, key) => Object.prototype.hasOwnProperty.call(mapping, key);
  const folded = (value) => String(value ?? "").trim().toLowerCase();
  const encodedKey = (value) => String(value ?? "").toLowerCase().replace(/[^a-z0-9]+/g, "");
  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (character) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
          character
        ],
    );
  const status = select("[data-transcribe-status]");
  const ready = select("[data-transcribe-ready]");

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
    const registry = registryOf(catalog);
    const decodeForm = select("[data-decode-form]");
    const encodeForm = select("[data-encode-form]");
    const versions = list(catalog.versions);
    fillVersions(decodeForm.elements.version, versions);
    fillVersions(encodeForm.elements.version, versions);
    select("[data-registry-count]").textContent = `${number.format(registry.fields.length)} fields`;

    let decodedText = "";
    let decodedDebug = "";
    let encodedText = "";
    let encodedDebug = "";
    let scheduled = 0;

    const schedule = (render) => {
      window.cancelAnimationFrame(scheduled);
      scheduled = window.requestAnimationFrame(render);
    };

    decodeForm.addEventListener("submit", (event) => {
      event.preventDefault();
      renderDecode();
    });
    encodeForm.addEventListener("submit", (event) => {
      event.preventDefault();
      renderEncode();
    });
    decodeForm.addEventListener("input", () => schedule(renderDecode));
    encodeForm.addEventListener("input", () => schedule(renderEncode));
    select("[data-decode-sample]").addEventListener("click", () => {
      decodeForm.elements.text.value = DECODE_SAMPLE;
      renderDecode();
    });
    select("[data-encode-sample]").addEventListener("click", () => {
      encodeForm.elements.text.value = ENCODE_SAMPLE;
      renderEncode();
    });
    select("[data-decode-clear]").addEventListener("click", () => {
      decodeForm.elements.text.value = "";
      renderDecode();
      decodeForm.elements.text.focus();
    });
    select("[data-encode-clear]").addEventListener("click", () => {
      encodeForm.elements.text.value = "";
      renderEncode();
      encodeForm.elements.text.focus();
    });
    select("[data-copy-decode]").addEventListener("click", (event) =>
      copy(decodedText, event.currentTarget),
    );
    select("[data-copy-decode-debug]").addEventListener("click", (event) =>
      copy(decodedDebug, event.currentTarget),
    );
    select("[data-copy-encode]").addEventListener("click", (event) =>
      copy(encodedText, event.currentTarget),
    );
    select("[data-copy-encode-debug]").addEventListener("click", (event) =>
      copy(encodedDebug, event.currentTarget),
    );

    function renderDecode() {
      const parsed = parseInput(
        decodeForm.elements.text.value,
        decodeForm.elements.separator.value,
        "decode",
      );
      const version = versionOf(parsed.pairs, decodeForm.elements.version.value);
      const records = parsed.pairs.map((pair) => decodePair(pair, version.value, registry));
      const issues = issueRecords(records, parsed.unparsed);
      decodedText = records.map((record) => `${record.output_key}=${record.output_value}`).join("|");
      decodedDebug = JSON.stringify(debugRecord("decode", parsed, version, records), null, 2);

      select("[data-decode-output]").textContent = decodedText || "—";
      select("[data-decode-debug]").textContent = decodedDebug;
      select("[data-decode-rows]").innerHTML = records.length
        ? records.map(decodeRow).join("")
        : emptyRow(8, "No parsed pairs.");
      renderIssues("decode", issues);
      renderSummary("decode", records, parsed.unparsed);
      renderVersion("decode", version, parsed.separator);
    }

    function renderEncode() {
      const parsed = parseInput(
        encodeForm.elements.text.value,
        encodeForm.elements.separator.value,
        "encode",
      );
      const version = versionOf(parsed.pairs, encodeForm.elements.version.value);
      const records = parsed.pairs.map((pair) => encodePair(pair, version.value, registry));
      const outputSeparator = SEPARATORS.get(encodeForm.elements.output_separator.value) || "|";
      const issues = issueRecords(records, parsed.unparsed);
      encodedText = records
        .map((record) => `${record.output_key}=${record.output_value}`)
        .join(outputSeparator);
      encodedDebug = JSON.stringify(
        debugRecord("encode", parsed, version, records, outputSeparator),
        null,
        2,
      );

      select("[data-encode-output]").textContent = visible(encodedText) || "—";
      select("[data-encode-debug]").textContent = encodedDebug;
      select("[data-encode-rows]").innerHTML = records.length
        ? records.map(encodeRow).join("")
        : emptyRow(8, "No parsed pairs.");
      renderIssues("encode", issues);
      renderSummary("encode", records, parsed.unparsed);
      renderVersion("encode", version, parsed.separator);
    }

    status.hidden = true;
    ready.hidden = false;
    renderDecode();
    renderEncode();
  }

  function registryOf(catalog) {
    const fields = list(catalog.fields);
    const byTag = new Map();
    const byName = new Map();
    const byAlias = new Map();
    const containers = new Set();
    fields.forEach((field) => {
      if (field.tag !== undefined) byTag.set(String(field.tag), field);
      byName.set(folded(field.name), field);
      list(field.aliases).forEach((alias) => claim(byAlias, folded(alias.name), field));
    });
    list(catalog.components).forEach((component) => {
      containers.add(folded(component.name));
      collectContainers(component.members, containers);
    });
    return { fields, byTag, byName, byAlias, containers };
  }

  function claim(index, key, field) {
    if (!key) return;
    const present = index.get(key);
    if (present === undefined) index.set(key, field);
    else if (present !== field) index.set(key, null);
  }

  function collectContainers(members, target) {
    list(members).forEach((member) => {
      if (member.kind === "component" || member.kind === "group") {
        target.add(folded(member.name));
      }
      collectContainers(member.members, target);
    });
  }

  function lookupField(key, registry) {
    const spelling = String(key).replace(/^#/, "");
    if (/^\d+$/.test(spelling)) return registry.byTag.get(spelling) || null;
    const direct = registry.byName.get(folded(spelling)) || registry.byAlias.get(folded(spelling));
    if (direct) return direct;

    const indexed = spelling.match(/^(.+)\[\d+\]$/);
    if (indexed) {
      return registry.byName.get(folded(indexed[1])) || registry.byAlias.get(folded(indexed[1]));
    }

    const selected = spelling.match(/^(.+)\[([^\d][^\]]*)\]$/);
    if (selected && registry.containers.has(folded(selected[1]))) {
      return registry.byName.get(folded(selected[2])) || registry.byAlias.get(folded(selected[2]));
    }
    const parts = spelling.split(".");
    if (parts.length < 2) return null;
    const container = parts.at(-2).replace(/\[[^\]]+\]$/, "");
    if (!registry.containers.has(folded(container))) return null;
    const terminal = parts.at(-1);
    return registry.byName.get(folded(terminal)) || registry.byAlias.get(folded(terminal));
  }

  function parseInput(text, selectedSeparator, direction) {
    const original = String(text ?? "");
    const separator =
      selectedSeparator === "auto"
        ? detectSeparator(original)
        : SEPARATORS.get(selectedSeparator) || "\x01";
    let source = original;
    const unparsed = [];
    const begin = direction === "decode" ? source.search(/\b8\s*=\s*FIX(?:T)?\.\d/i) : -1;
    if (begin > 0) {
      const prefix = source.slice(0, begin).trim();
      if (prefix) unparsed.push(unparsedRecord(0, prefix, "prefix", "Before BeginString"));
      source = source.slice(begin);
    }

    const tokens = split(source, separator);
    const preliminary = tokens.map(parseToken).filter(Boolean);
    const hasWireBegin = preliminary.some(
      (pair) => pair.input_key.replace(/^#/, "") === "8" && /^FIX(?:T)?\./i.test(pair.input_value),
    );
    const hybrid = preliminary.some(
      (pair) => pair.input_key.replace(/^#/, "") === "35" && /^U/i.test(pair.input_value),
    );
    const named = direction === "encode" || !hasWireBegin || hybrid;
    const pairs = [];
    let ended = false;
    let position = 0;

    tokens.forEach((token) => {
      if (!token.trim()) return;
      position += 1;
      if (ended) {
        unparsed.push(unparsedRecord(position, token, "after-checksum", "After CheckSum <10>"));
        return;
      }
      const pair = parseToken(token);
      if (!pair) {
        unparsed.push(unparsedRecord(position, token, "syntax", "Expected key=value"));
        return;
      }
      if (!NAME.test(pair.input_key.replace(/^#/, ""))) {
        unparsed.push(unparsedRecord(position, token, "syntax", "Key does not match FIX syntax"));
        return;
      }
      if (!named && !/^\d+$/.test(pair.input_key)) {
        unparsed.push(
          unparsedRecord(position, token, "wire-name", "Named key in a numeric wire message"),
        );
        return;
      }
      const normalized = {
        ...pair,
        index: position,
        source: token,
        input_key: named ? pair.input_key.replace(/^#/, "") : pair.input_key,
      };
      pairs.push(...expandEntryPair(normalized, separator));
      if (isChecksum(normalized.input_key)) ended = true;
    });
    return { direction, separator, named, pairs, unparsed };
  }

  function parseToken(token) {
    const at = token.indexOf("=");
    if (at < 0) return null;
    let inputKey = token.slice(0, at).trim();
    if (!inputKey) return null;
    let inputValue = token.slice(at + 1).trim();
    const marker = inputKey.startsWith("#") ? "#" : "";
    const spelling = inputKey.replace(/^#/, "");
    const selected = spelling.match(/^(.+)\[([^\d][^\]]*)\]$/);
    if (selected) inputKey = `${marker}${selected[1]}.${selected[2]}`;
    const indexed = inputKey.replace(/^#/, "").match(/^(.+\[\d+\])(?:\.([A-Za-z0-9_.-]+))?$/);
    if (indexed && !indexed[2]) {
      const memberAt = inputValue.indexOf("=");
      const member = memberAt < 0 ? "" : inputValue.slice(0, memberAt).trim();
      if (/^[A-Za-z_][A-Za-z0-9_.-]*$/.test(member)) {
        inputKey = `${marker}${indexed[1]}.${member}`;
        inputValue = inputValue.slice(memberAt + 1).trim();
      }
    }
    return {
      input_key: inputKey,
      input_value: inputValue,
    };
  }

  function expandEntryPair(pair, outerSeparator) {
    const indexed = pair.input_key.match(/^(.+\[\d+\])(?:\.[A-Za-z0-9_.-]+)?$/);
    if (!indexed) return [pair];
    const entrySeparator = DETECTED_SEPARATORS.find(
      (candidate) => candidate !== outerSeparator && pair.input_value.includes(candidate),
    );
    if (!entrySeparator) return [pair];
    const chunks = split(pair.input_value, entrySeparator);
    if (chunks.at(-1) === "") chunks.pop();
    const [head, ...members] = chunks;
    const expanded = [
      {
        ...pair,
        input_value: head.trim(),
        entry_index: 0,
        entry_separator: separatorName(entrySeparator),
      },
    ];
    members.forEach((chunk, index) => {
      const member = parseToken(chunk);
      expanded.push({
        ...pair,
        input_key: member ? `${indexed[1]}.${member.input_key.replace(/^#/, "")}` : indexed[1],
        input_value: member ? member.input_value : chunk.trim(),
        source: chunk,
        entry_index: index + 1,
        entry_separator: separatorName(entrySeparator),
      });
    });
    return expanded;
  }

  function isChecksum(key) {
    const spelling = String(key);
    if (spelling === "10" || spelling.includes("[")) return spelling === "10";
    return folded(spelling.split(".").at(-1)) === "checksum";
  }

  function unparsedRecord(index, source, state, reason) {
    return { index, source, parsed: false, resolved: false, state, reason };
  }

  function detectSeparator(text) {
    const begin = String(text).match(/\b8\s*=\s*FIX(?:T)?\.[A-Za-z0-9.]+/i);
    if (begin) {
      const after = (begin.index || 0) + begin[0].length;
      const following = String(text).slice(after).replace(/^[ \t]+/, "");
      if (following.startsWith("\r\n")) return "\n";
      const declared = DETECTED_SEPARATORS.find((candidate) => following.startsWith(candidate));
      if (declared) return declared;
      if (following[0] && !/\s/.test(following[0])) return following[0];
    }
    const marked = DETECTED_SEPARATORS.find((candidate) =>
      split(text, candidate)
        .slice(1)
        .some((piece) => /^\s*#(?:\d+|[A-Za-z_][A-Za-z0-9_.-]*)\s*=/.test(piece)),
    );
    if (marked) return marked;
    return DETECTED_SEPARATORS.find((candidate) => String(text).includes(candidate)) || "\x01";
  }

  function split(text, separator) {
    return separator === "\n" ? String(text).split(/\r?\n/) : String(text).split(separator);
  }

  function versionOf(pairs, selected) {
    if (selected) return { value: selected, source: "selected", transport: null };
    const begin = pairValue(pairs, "8", "BeginString");
    if (!begin) return { value: null, source: "unresolved", transport: null };
    const application = String(begin).match(/^FIX\.(\d+)\.(\d+)(?:\.?SP(\d+))?$/i);
    if (application) {
      const suffix = application[3] ? `.SP${application[3]}` : "";
      return {
        value: `${application[1]}.${application[2]}${suffix}`,
        source: "BeginString",
        transport: null,
      };
    }
    const transport = String(begin).match(/^FIXT\.(\d+)\.(\d+)$/i);
    if (!transport) return { value: null, source: "unresolved", transport: null };
    const applicationCode = pairValue(pairs, "1128", "ApplVerID") ||
      pairValue(pairs, "1137", "DefaultApplVerID");
    return {
      value: APPLICATION_VERSIONS[applicationCode] || null,
      source: applicationCode ? "ApplVerID" : "unresolved",
      transport: `FIXT${transport[1]}.${transport[2]}`,
    };
  }

  function pairValue(pairs, tag, name) {
    return pairs.find(
      (pair) => pair.input_key === tag || folded(pair.input_key) === folded(name),
    )?.input_value;
  }

  function declaration(field, version) {
    if (!version) return "unresolved";
    const versions = list(field.versions);
    return versions.includes("*") || versions.includes(version) ? "declared" : "other-version";
  }

  function decodePair(pair, version, registry) {
    const field = lookupField(pair.input_key, registry);
    if (!field) return unresolvedPair(pair, "decode");
    const raw = pair.input_value;
    const decoded = own(object(field.decoded), raw) ? field.decoded[raw] : raw;
    const meaning = object(field.values)[raw] ?? object(field.value_names)[raw] ?? null;
    const typed = parseValue(raw, field.type);
    const versionStatus = declaration(field, version);
    const enumerated = Object.keys(object(field.values)).length > 0 ||
      Object.keys(object(field.value_names)).length > 0;
    const knownValue = own(object(field.values), raw) || own(object(field.value_names), raw);
    const state =
      versionStatus === "other-version"
        ? "other-version"
        : !typed.valid
          ? "invalid-value"
          : enumerated && !knownValue
            ? "unknown-value"
            : "resolved";
    return {
      index: pair.index,
      source: pair.source,
      input_key: pair.input_key,
      input_value: raw,
      parsed: true,
      resolved: true,
      tag: field.tag ?? null,
      name: field.name,
      datatype: field.type || null,
      typed_value: typed.valid ? typed.value : null,
      decoded,
      meaning,
      version_status: versionStatus,
      output_key: field.name,
      output_value: decoded,
      state,
      reason: typed.reason || versionReason(versionStatus, version),
      description: field.description || null,
    };
  }

  function encodePair(pair, version, registry) {
    const field = lookupField(pair.input_key, registry);
    if (!field) return unresolvedPair(pair, "encode");
    const versionStatus = declaration(field, version);
    const mayTranscribe = versionStatus === "declared";
    const encodings = object(field.encoded);
    const valueKey = encodedKey(pair.input_value);
    const encoded = mayTranscribe && own(encodings, valueKey);
    const enumerated = Object.keys(object(field.values)).length > 0 ||
      Object.keys(object(field.value_names)).length > 0;
    const outputValue = encoded ? encodings[valueKey] : pair.input_value;
    const outputKey = mayTranscribe ? String(field.tag ?? field.name) : pair.input_key;
    const typed = parseValue(outputValue, field.type);
    const state =
      versionStatus === "unresolved"
        ? "version-required"
        : versionStatus === "other-version"
          ? "other-version"
          : !typed.valid
            ? "invalid-value"
            : enumerated && !encoded
              ? "unknown-value"
              : encoded
                ? "encoded"
                : "resolved";
    return {
      index: pair.index,
      source: pair.source,
      input_key: pair.input_key,
      input_value: pair.input_value,
      parsed: true,
      resolved: true,
      tag: field.tag ?? null,
      name: field.name,
      datatype: field.type || null,
      typed_value: typed.valid ? typed.value : null,
      wire_value: outputValue,
      decoded: own(object(field.decoded), outputValue) ? field.decoded[outputValue] : outputValue,
      meaning: object(field.values)[outputValue] ?? object(field.value_names)[outputValue] ?? null,
      version_status: versionStatus,
      output_key: outputKey,
      output_value: outputValue,
      state,
      reason: typed.reason || versionReason(versionStatus, version),
      description: field.description || null,
    };
  }

  function unresolvedPair(pair, direction) {
    return {
      index: pair.index,
      source: pair.source,
      input_key: pair.input_key,
      input_value: pair.input_value,
      parsed: true,
      resolved: false,
      tag: /^\d+$/.test(pair.input_key) ? Number(pair.input_key) : null,
      name: null,
      datatype: null,
      typed_value: null,
      decoded: pair.input_value,
      meaning: null,
      version_status: "unknown-field",
      output_key: pair.input_key,
      output_value: pair.input_value,
      state: "unresolved",
      reason: `No registry field for ${pair.input_key}`,
      direction,
    };
  }

  function parseValue(value, datatype) {
    const raw = String(value);
    const kind = String(datatype || "").toUpperCase();
    if (INTEGER_TYPES.has(kind)) {
      if (!/^[+-]?\d+$/.test(raw)) return invalid(`Expected ${datatype} integer`);
      const numeric = Number(raw);
      return {
        valid: true,
        value: Number.isSafeInteger(numeric) ? numeric : raw,
        representation: Number.isSafeInteger(numeric) ? "number" : "integer-string",
      };
    }
    if (NUMBER_TYPES.has(kind)) {
      if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(raw)) {
        return invalid(`Expected ${datatype} number`);
      }
      const numeric = Number(raw);
      return Number.isFinite(numeric)
        ? { valid: true, value: numeric, representation: "number" }
        : invalid(`Expected finite ${datatype}`);
    }
    if (kind === "BOOLEAN") {
      if (raw === "Y") return { valid: true, value: true, representation: "boolean" };
      if (raw === "N") return { valid: true, value: false, representation: "boolean" };
      return invalid("Expected Boolean Y or N");
    }
    if (DATE_TYPES.has(kind)) return parseDate(raw, datatype);
    if (kind === "UTCTIMESTAMP" || kind === "TZTIMESTAMP") {
      return parseTimestamp(raw, datatype);
    }
    if (TIME_TYPES.has(kind)) return parseTime(raw, datatype);
    if (kind === "MONTHYEAR") {
      return /^\d{6}(?:\d{2}|w[1-5])?$/i.test(raw)
        ? { valid: true, value: raw, representation: "month-year" }
        : invalid(`Expected ${datatype} YYYYMM[DD|wN]`);
    }
    return { valid: true, value: raw, representation: "string" };
  }

  function invalid(reason) {
    return { valid: false, value: null, representation: null, reason };
  }

  function parseDate(raw, datatype) {
    const match = raw.match(/^(\d{4})(\d{2})(\d{2})$/);
    if (!match || !validDate(match[1], match[2], match[3])) {
      return invalid(`Expected ${datatype} YYYYMMDD`);
    }
    return {
      valid: true,
      value: `${match[1]}-${match[2]}-${match[3]}`,
      representation: "date",
    };
  }

  function parseTimestamp(raw, datatype) {
    const match = raw.match(
      /^(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|[+-]\d{2}(?::?\d{2})?)?$/,
    );
    if (
      !match ||
      !validDate(match[1], match[2], match[3]) ||
      !validTime(match[4], match[5], match[6])
    ) {
      return invalid(`Expected ${datatype} YYYYMMDD-HH:MM:SS[.sss]`);
    }
    const zone = match[8] || (String(datatype).toUpperCase() === "UTCTIMESTAMP" ? "Z" : "");
    return {
      valid: true,
      value: `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}${match[7] || ""}${zone}`,
      representation: "timestamp",
    };
  }

  function parseTime(raw, datatype) {
    const match = raw.match(/^(\d{2}):(\d{2}):(\d{2})(\.\d+)?(?:Z|[+-]\d{2}(?::?\d{2})?)?$/);
    return match && validTime(match[1], match[2], match[3])
      ? { valid: true, value: raw, representation: "time" }
      : invalid(`Expected ${datatype} HH:MM:SS[.sss]`);
  }

  function validDate(year, month, day) {
    const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
    return (
      date.getUTCFullYear() === Number(year) &&
      date.getUTCMonth() === Number(month) - 1 &&
      date.getUTCDate() === Number(day)
    );
  }

  function validTime(hour, minute, second) {
    return Number(hour) < 24 && Number(minute) < 60 && Number(second) < 61;
  }

  function versionReason(state, version) {
    if (state === "unresolved") return "Select or include a FIX application version";
    if (state === "other-version") return `Field is not declared in ${version}`;
    return null;
  }

  function decodeRow(record) {
    return `<tr>
      <td>${record.index}</td>
      <td><code>${escape(record.input_key)}</code></td>
      <td>${registryField(record)}</td>
      <td><code>${escape(record.input_value)}</code></td>
      <td><code>${escape(displayValue(record.typed_value))}</code></td>
      <td><code>${escape(record.decoded)}</code></td>
      <td>${record.meaning ? escape(record.meaning) : '<span class="fix-registry__muted">—</span>'}</td>
      <td>${stateBadge(record.state, record.reason)}</td>
    </tr>`;
  }

  function encodeRow(record) {
    return `<tr>
      <td>${record.index}</td>
      <td><code>${escape(record.input_key)}</code></td>
      <td>${registryField(record)}</td>
      <td><code>${escape(record.input_value)}</code></td>
      <td><code>${escape(record.output_key)}</code></td>
      <td><code>${escape(record.output_value)}</code></td>
      <td><code>${escape(record.datatype || "—")}</code></td>
      <td>${stateBadge(record.state, record.reason)}</td>
    </tr>`;
  }

  function registryField(record) {
    if (!record.resolved) return '<span class="fix-registry__muted">—</span>';
    const key = record.tag ?? record.name;
    const href = `${app.dataset.registry}#field=${encodeURIComponent(key)}`;
    const tag = record.tag === null ? "namespace" : `&lt;${escape(record.tag)}&gt;`;
    return `<a class="fix-registry__name" href="${escape(href)}">${escape(record.name)} ${tag}</a>`;
  }

  function stateBadge(state, reason) {
    return `<span class="fix-transcribe__state fix-transcribe__state--${escape(state)}"${
      reason ? ` title="${escape(reason)}"` : ""
    }>${escape(state.replaceAll("-", " "))}</span>`;
  }

  function displayValue(value) {
    if (value === null || value === undefined) return "—";
    return typeof value === "string" ? value : JSON.stringify(value);
  }

  function issueRecords(records, unparsed) {
    return [
      ...records
        .filter((record) => !record.resolved)
        .map((record) => ({
          index: record.index,
          source: record.source,
          state: "unresolved",
          reason: record.reason,
        })),
      ...unparsed.map((record) => ({
        index: record.index,
        source: record.source,
        state: "unparsed",
        reason: record.reason,
      })),
    ].sort((left, right) => left.index - right.index);
  }

  function renderIssues(direction, issues) {
    const wrap = select(`[data-${direction}-issues-wrap]`);
    wrap.hidden = issues.length === 0;
    select(`[data-${direction}-issue-count]`).textContent = `(${number.format(issues.length)})`;
    select(`[data-${direction}-issues]`).innerHTML = issues
      .map(
        (issue) => `<tr>
          <td>${issue.index}</td>
          <td><code>${escape(issue.source)}</code></td>
          <td>${stateBadge(issue.state)}</td>
          <td>${escape(issue.reason)}</td>
        </tr>`,
      )
      .join("");
  }

  function renderSummary(direction, records, unparsed) {
    select(`[data-${direction}-parsed]`).textContent = number.format(records.length);
    select(`[data-${direction}-resolved]`).textContent = number.format(
      records.filter((record) => record.resolved).length,
    );
    select(`[data-${direction}-unresolved]`).textContent = number.format(
      records.filter((record) => !record.resolved).length,
    );
    select(`[data-${direction}-unparsed]`).textContent = number.format(unparsed.length);
    select(`[data-${direction}-state]`).textContent = `${number.format(records.length)} pairs`;
  }

  function renderVersion(direction, version, separator) {
    const target = select(`[data-${direction}-version]`);
    const separatorText = separatorName(separator);
    if (version.value) {
      target.textContent = `FIX ${version.value} ${version.source === "selected" ? "selected" : `inferred from ${version.source}`} · ${separatorText}`;
    } else if (version.transport) {
      target.textContent = `${version.transport} transport · application version unresolved · ${separatorText}`;
    } else {
      target.textContent = `Version unresolved; cross-version matches are suggestions · ${separatorText}`;
    }
  }

  function debugRecord(direction, parsed, version, records, outputSeparator = "|") {
    return {
      direction,
      input_separator: separatorName(parsed.separator),
      output_separator: direction === "encode" ? separatorName(outputSeparator) : undefined,
      named_input: parsed.named,
      version: version.value,
      version_source: version.source,
      transport: version.transport,
      parsed: records,
      unparsed: parsed.unparsed,
    };
  }

  function separatorName(separator) {
    return {
      "\x01": "SOH (\\x01)",
      "\x04\x03": "EOT + ETX (\\x04\\x03)",
      "^A": "caret A (^A)",
      "^": "caret (^)",
      "|": "pipe (|)",
      ";": "semicolon (;)",
      "\n": "new line",
    }[separator] || JSON.stringify(separator);
  }

  function visible(value) {
    return String(value).replaceAll("\x01", "␁").replaceAll("\x04", "␄").replaceAll("\x03", "␃");
  }

  function fillVersions(element, versions) {
    versions.forEach((version) => element.add(new Option(version, version)));
  }

  function emptyRow(columns, message) {
    return `<tr><td class="fix-registry__empty" colspan="${columns}">${escape(message)}</td></tr>`;
  }

  async function copy(value, button) {
    const label = button.textContent;
    try {
      if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(value);
      else fallbackCopy(value);
      button.textContent = "Copied";
    } catch (_error) {
      fallbackCopy(value);
      button.textContent = "Copied";
    }
    window.setTimeout(() => {
      button.textContent = label;
    }, 1200);
  }

  function fallbackCopy(value) {
    const input = document.createElement("textarea");
    input.value = value;
    input.setAttribute("readonly", "");
    input.style.position = "fixed";
    input.style.opacity = "0";
    document.body.append(input);
    input.select();
    document.execCommand("copy");
    input.remove();
  }
})();
