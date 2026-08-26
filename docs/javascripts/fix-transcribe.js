(() => {
  "use strict";

  const app = document.querySelector("[data-fix-transcribe]");
  if (!app) return;

  const DECODE_SAMPLES = {
    fix: "8=FIX.4.4|35=D|11=ORD-42|453=2|448=BUY-A|447=D|452=3|448=BUY-B|447=D|452=3|10=000|",
    ul: "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|#CLORDID=ORD-42|#NoPartyIDs[0]=PartyID=BUY-A\x04\x03PartyIDSource=proprietary/customcode\x04\x03PartyRole=clientid\x04\x03|#NoPartyIDs[1].PartyID=BUY-B|#NoPartyIDs[1].PartyIDSource=proprietary/customcode|#NoPartyIDs[1].PartyRole=clientid|",
    "ul-wire":
      "sending >> 8=FIX.4.4|35=UL|55=wire-copy|#MSGTYPE=D|#CLORDID=ORD-42|#SYMBOL=named-copy|#NoPartyIDs[0].PartyID=BUY-A|#NoPartyIDs[0].PartyIDSource=proprietary/customcode|#NoPartyIDs[0].PartyRole=clientid|10=000|",
  };
  const ENCODE_SAMPLES = {
    fix: "BeginString=FIX.4.4|MsgType=NewOrderSingle|ClOrdID=ORD-42|NoPartyIDs=2|NoPartyIDs[0].PartyID=BUY-A|NoPartyIDs[0].PartyIDSource=proprietary/customcode|NoPartyIDs[0].PartyRole=clientid|NoPartyIDs[1].PartyID=BUY-B|NoPartyIDs[1].PartyIDSource=proprietary/customcode|NoPartyIDs[1].PartyRole=clientid|CheckSum=000|",
    ul: "#BEGINSTRING=FIX.4.4|#MSGTYPE=NewOrderSingle|#CLORDID=ORD-42|#NoPartyIDs[0].PartyID=BUY-A|#NoPartyIDs[0].PartyIDSource=proprietary/customcode|#NoPartyIDs[0].PartyRole=clientid|",
    "ul-wire":
      "8=FIX.4.4|35=UL|#MSGTYPE=NewOrderSingle|#CLORDID=ORD-42|#NoPartyIDs[0].PartyID=BUY-A|#NoPartyIDs[0].PartyIDSource=proprietary/customcode|#NoPartyIDs[0].PartyRole=clientid|10=000|",
  };
  const SEPARATORS = new Map([
    ["soh", "\x01"],
    ["pipe", "|"],
    ["eot-etx", "\x04\x03"],
    ["caret-a", "^A"],
    ["caret", "^"],
    ["semicolon", ";"],
    ["hash", "#"],
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
    app.querySelectorAll("[data-decode-sample]").forEach((button) => {
      button.addEventListener("click", () => {
        decodeForm.elements.text.value = DECODE_SAMPLES[button.dataset.decodeSample];
        renderDecode();
      });
    });
    app.querySelectorAll("[data-encode-sample]").forEach((button) => {
      button.addEventListener("click", () => {
        encodeForm.elements.text.value = ENCODE_SAMPLES[button.dataset.encodeSample];
        renderEncode();
      });
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
      const transcribed = { ...parsed, pairs: expandPayloadPairs(parsed.pairs, registry) };
      const records = applyNamedPrecedence(
        transcribed.pairs.map((pair, ordinal) => ({
          ...decodePair(pair, version.value, registry),
          ordinal,
        })),
        transcribed,
      );
      const issues = issueRecords(records, parsed.unparsed);
      const structure = structureOf(records, version.value, registry, "decode");
      decodedText = records
        .filter((record) => !record.shadowed)
        .map((record) => `${record.output_key}=${record.output_value}`)
        .join("|");
      decodedDebug = JSON.stringify(
        debugRecord("decode", transcribed, version, records, structure),
        null,
        2,
      );

      select("[data-decode-output]").textContent = decodedText || "—";
      select("[data-decode-debug]").textContent = decodedDebug;
      select("[data-decode-rows]").innerHTML = records.length
        ? records.map(decodeRow).join("")
        : emptyRow(8, "No parsed pairs.");
      renderIssues("decode", issues);
      renderSummary("decode", records, parsed.unparsed);
      renderVersion("decode", version, parsed.separator);
      renderProtocol("decode", transcribed.protocol);
      renderStructure("decode", structure);
    }

    function renderEncode() {
      const parsed = parseInput(
        encodeForm.elements.text.value,
        encodeForm.elements.separator.value,
        "encode",
      );
      const version = versionOf(parsed.pairs, encodeForm.elements.version.value);
      const records = applyNamedPrecedence(
        parsed.pairs.map((pair, ordinal) => ({
          ...encodePair(pair, version.value, registry),
          ordinal,
        })),
        parsed,
      );
      const outputSeparator = SEPARATORS.get(encodeForm.elements.output_separator.value) || "|";
      const issues = issueRecords(records, parsed.unparsed);
      const structure = structureOf(records, version.value, registry, "encode");
      encodedText = records
        .filter((record) => !record.shadowed)
        .map((record) => `${record.output_key}=${record.output_value}`)
        .join(outputSeparator);
      encodedDebug = JSON.stringify(
        debugRecord("encode", parsed, version, records, structure, outputSeparator),
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
      renderProtocol("encode", parsed.protocol);
      renderStructure("encode", structure);
    }

    status.hidden = true;
    ready.hidden = false;
    renderDecode();
    renderEncode();
  }

  function registryOf(catalog) {
    const fields = list(catalog.fields);
    const components = list(catalog.components);
    const byTag = new Map();
    const byName = new Map();
    const byAlias = new Map();
    const byComponent = new Map();
    const groupsByName = new Map();
    const groupsByTag = new Map();
    const containers = new Set();
    fields.forEach((field) => {
      if (field.tag !== undefined) byTag.set(String(field.tag), field);
      byName.set(folded(field.name), field);
      containers.add(folded(field.name));
      list(field.aliases).forEach((alias) => claim(byAlias, folded(alias.name), field));
    });
    components.forEach((component) => {
      byComponent.set(folded(component.name), component);
      containers.add(folded(component.name));
      collectContainers(component.members, containers);
    });
    const registry = {
      fields,
      components,
      byTag,
      byName,
      byAlias,
      byComponent,
      containers,
      groupsByName,
      groupsByTag,
    };
    components.forEach((component) => {
      collectGroups(component.members, component, registry);
    });
    return registry;
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

  function collectGroups(members, component, registry) {
    list(members).forEach((member) => {
      if (member.kind === "group") {
        const expanded = expandedMembers(member.members, registry, new Set());
        const definition = {
          name: member.name,
          tag: member.tag ?? null,
          component: component.name,
          versions: list(component.versions),
          members: expanded,
          delimiter: firstMember(expanded),
          signature: groupSignature(member.name, member.tag, expanded),
        };
        appendUnique(registry.groupsByName, folded(member.name), definition);
        if (member.tag !== undefined) {
          appendUnique(registry.groupsByTag, String(member.tag), definition);
        }
        collectGroups(member.members, component, registry);
      }
    });
  }

  function expandedMembers(members, registry, seen) {
    return list(members).flatMap((member) => {
      if (member.kind !== "component") {
        return member.kind === "group"
          ? [{ ...member, members: expandedMembers(member.members, registry, seen) }]
          : [member];
      }
      const key = folded(member.name);
      if (seen.has(key)) return [];
      const component = registry.byComponent.get(key);
      return component
        ? expandedMembers(component.members, registry, new Set([...seen, key]))
        : [];
    });
  }

  function firstMember(members) {
    for (const member of list(members)) {
      if (member.kind === "field" && member.tag !== undefined) return String(member.tag);
      if (member.kind === "group" && member.tag !== undefined) return String(member.tag);
    }
    return null;
  }

  function groupSignature(name, tag, members) {
    const shape = list(members)
      .map((member) => `${member.kind}:${member.tag ?? member.name}:${groupSignature("", "", member.members)}`)
      .join(",");
    return `${name}:${tag ?? ""}:${shape}`;
  }

  function appendUnique(index, key, definition) {
    const present = index.get(key) || [];
    if (!present.some((candidate) => candidate.signature === definition.signature)) {
      present.push(definition);
      index.set(key, present);
    }
  }

  function lookupField(key, registry) {
    const spelling = String(key).replace(/^#/, "");
    if (/^\d+$/.test(spelling)) return registry.byTag.get(spelling) || null;
    const direct = registry.byName.get(folded(spelling)) || registry.byAlias.get(folded(spelling));
    if (direct) return direct;

    const indexed = spelling.match(/^(.+)\[\d+\]$/);
    if (indexed) {
      if (registry.groupsByName.has(folded(indexed[1]))) return null;
      return registry.byName.get(folded(indexed[1])) || registry.byAlias.get(folded(indexed[1]));
    }

    const selected = spelling.match(/^(.+)\[([^\d][^\]]*)\]$/);
    if (selected && registry.containers.has(folded(selected[1]))) {
      return registry.byName.get(folded(selected[2])) || registry.byAlias.get(folded(selected[2]));
    }
    const parts = spelling.split(".");
    if (parts.length < 2) return null;
    const terminal = parts.at(-1);
    if (/^\d+$/.test(terminal)) return registry.byTag.get(terminal) || null;
    const container = parts.at(-2).replace(/\[[^\]]+\]$/, "");
    if (!registry.containers.has(folded(container))) return null;
    return registry.byName.get(folded(terminal)) || registry.byAlias.get(folded(terminal));
  }

  function parseInput(text, selectedSeparator, direction) {
    const original = String(text ?? "");
    const separator =
      selectedSeparator === "auto"
        ? detectSeparator(original)
        : SEPARATORS.get(selectedSeparator) || "\x01";
    const begin = beginIndex(original);
    const bridge = bridgeIndex(original);
    const start = begin >= 0 ? begin : bridge;
    let source = start >= 0 ? original.slice(start) : original;
    const unparsed = [];
    const ignored = [];
    if (start > 0) {
      const prefix = original.slice(0, start).trim();
      if (prefix) {
        ignored.push(
          unparsedRecord(
            0,
            prefix,
            "envelope",
            begin >= 0 ? "Before BeginString" : "Before first marked UL field",
          ),
        );
      }
    }

    const tokens = split(source, separator);
    const preliminary = tokens.map(parseToken).filter(Boolean);
    const protocol = protocolOf(source, preliminary, begin >= 0, bridge >= 0, direction);
    const named = protocol.named;
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
        input_form:
          pair.marked || separator === "#" || !/^\d+$/.test(pair.input_key)
            ? "named"
            : "numeric",
      };
      pairs.push(...expandEntryPair(normalized, separator));
      if (isChecksum(normalized.input_key)) ended = true;
    });
    return { direction, separator, named, protocol, pairs, unparsed, ignored };
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
      marked: marker === "#",
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

  function expandPayloadPairs(pairs, registry) {
    return pairs.flatMap((pair) => {
      const field = lookupField(pair.input_key, registry);
      if (field?.name !== "XmlData") return [pair];
      const separator = detectSeparator(pair.input_value);
      const members = split(pair.input_value, separator)
        .map(parseToken)
        .filter((member) => member && NAME.test(member.input_key.replace(/^#/, "")));
      if (members.length < 2) return [pair];
      return members.flatMap((member, payloadIndex) =>
        expandEntryPair(
          {
            ...pair,
            input_key: `XmlData.${member.input_key.replace(/^#/, "")}`,
            input_value: member.input_value,
            input_form: "named",
            payload_index: payloadIndex,
            payload_separator: separatorName(separator),
            payload_source: pair.input_value,
          },
          separator,
        ),
      );
    });
  }

  function isChecksum(key) {
    const spelling = String(key);
    if (spelling.includes("[")) return spelling === "10";
    const terminal = spelling.split(".").at(-1);
    return terminal === "10" || folded(terminal) === "checksum";
  }

  function unparsedRecord(index, source, state, reason) {
    return { index, source, parsed: false, resolved: false, state, reason };
  }

  function detectSeparator(text) {
    const begin = String(text).match(/\b8=FIX(?:T)?\.[A-Za-z0-9.]+/i);
    if (begin) {
      const after = (begin.index || 0) + begin[0].length;
      const following = String(text).slice(after).replace(/^[ \t]+/, "");
      if (following.startsWith("\r\n")) return "\n";
      const declared = DETECTED_SEPARATORS.find((candidate) => following.startsWith(candidate));
      if (declared) return declared;
      if (following[0] && !/\s/.test(following[0])) return following[0];
    }
    const marks = markedTokens(text);
    if (marks.length > 1) {
      const before = marks[1].index;
      const marked = DETECTED_SEPARATORS.find((candidate) =>
        String(text).endsWith(candidate, before),
      );
      return marked || "#";
    }
    return DETECTED_SEPARATORS.find((candidate) => String(text).includes(candidate)) || "\x01";
  }

  function beginIndex(text) {
    const match = /(?:^|[^0-9])(?=8=FIX(?:T)?\.[A-Za-z0-9.]+)/i.exec(String(text));
    return match ? match.index + match[0].length : -1;
  }

  function bridgeIndex(text) {
    const matches = markedTokens(text);
    return matches.length > 1 ? matches[0].index : -1;
  }

  function markedTokens(text) {
    return [
      ...String(text).matchAll(
        /#(?:\d+|[A-Za-z][A-Za-z0-9_.-]*)(?:\[(?:\d+|[A-Za-z][A-Za-z0-9_.-]*)\])?(?:\.[A-Za-z0-9_.-]+)?[ \t\r\n\f\x0b]*=/g,
      ),
    ];
  }

  function protocolOf(source, pairs, hasBegin, hasBridge, direction) {
    const msgType = pairs.find((pair) => pair.input_key.replace(/^#/, "") === "35")
      ?.input_value;
    if (hasBegin && msgType === "UL") {
      return { code: "UL", variant: "fix-wrapper", label: "UL · FIX wrapper", named: true };
    }
    if (hasBegin && /^U[A-Za-z0-9]*$/i.test(msgType || "")) {
      return { code: "FIX", variant: "user-defined", label: "FIX · user-defined", named: true };
    }
    if (hasBegin) return { code: "FIX", variant: "wire", label: "FIX", named: false };
    if (hasBridge) return { code: "UL", variant: "bridge", label: "UL", named: true };
    const namedBegin = pairs.some(
      (pair) =>
        folded(pair.input_key.replace(/^#/, "")) === "beginstring" &&
        /^FIX(?:T)?\./i.test(pair.input_value),
    );
    if (namedBegin) {
      return { code: "FIX", variant: "named", label: "FIX · named", named: true };
    }
    if (/^[ \t\r\n\f\x0b]*#?35[ \t\r\n\f\x0b]*=/.test(source)) {
      return { code: "FIX", variant: "fragment", label: "FIX · fragment", named: false };
    }
    if (direction === "encode" || pairs.some((pair) => !/^#?\d+$/.test(pair.input_key))) {
      return { code: "UL", variant: "named", label: "UL · named", named: true };
    }
    return { code: "OTHER", variant: "pairs", label: "Pairs", named: true };
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
    const encodings = object(field.encoded);
    const encoded = pair.input_form === "named" && own(encodings, encodedKey(raw));
    const wireValue = encoded ? encodings[encodedKey(raw)] : raw;
    const decoded = own(object(field.decoded), wireValue) ? field.decoded[wireValue] : raw;
    const meaning =
      object(field.values)[wireValue] ?? object(field.value_names)[wireValue] ?? null;
    const typed = parseValue(wireValue, field.type);
    const versionStatus = declaration(field, version);
    const enumerated = Object.keys(object(field.values)).length > 0 ||
      Object.keys(object(field.value_names)).length > 0;
    const knownValue =
      own(object(field.values), wireValue) || own(object(field.value_names), wireValue);
    const state =
      versionStatus === "other-version"
        ? "other-version"
        : !typed.valid
          ? "invalid-value"
          : enumerated && !knownValue
            ? "unknown-value"
            : "resolved";
    return {
      ...pair,
      input_value: raw,
      parsed: true,
      resolved: true,
      tag: field.tag ?? null,
      name: field.name,
      datatype: field.type || null,
      typed_value: typed.valid ? typed.value : null,
      wire_value: wireValue,
      decoded,
      meaning,
      version_status: versionStatus,
      output_key: namedOutputKey(pair.input_key, field.name),
      output_value: decoded,
      state,
      reason:
        typed.reason ||
        versionReason(versionStatus, version) ||
        (state === "unknown-value" ? "Value is not declared by this registry field" : null),
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
      ...pair,
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
      reason:
        typed.reason ||
        versionReason(versionStatus, version) ||
        (state === "unknown-value" ? "Value is not declared by this registry field" : null),
      description: field.description || null,
    };
  }

  function unresolvedPair(pair, direction) {
    return {
      ...pair,
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

  function namedOutputKey(inputKey, fieldName) {
    const spelling = String(inputKey).replace(/^#/, "");
    if (/^\d+$/.test(spelling)) return fieldName;
    const parts = spelling.split(".");
    if (parts.length < 2) return fieldName;
    parts[parts.length - 1] = fieldName;
    return parts.join(".");
  }

  function applyNamedPrecedence(records, parsed) {
    if (!parsed.named || !["fix-wrapper", "user-defined"].includes(parsed.protocol.variant)) {
      return records;
    }
    const namedTags = new Set(
      records
        .filter(
          (record) =>
            record.resolved && record.input_form === "named" && !record.input_key.includes("["),
        )
        .map((record) => String(record.tag)),
    );
    return records.map((record) => {
      if (
        !record.resolved ||
        record.input_form !== "numeric" ||
        record.input_key.includes("[") ||
        !namedTags.has(String(record.tag))
      ) {
        return record;
      }
      return {
        ...record,
        shadowed: true,
        state: "shadowed",
        reason: "A named UL field is authoritative over this numeric wrapper copy",
      };
    });
  }

  function structureOf(records, version, registry, direction) {
    const containers = new Map();
    const diagnostics = [];
    const explicitNames = new Set();
    const structured = new Set();

    records.forEach((record) => {
      const path = indexedPath(record.input_key, registry);
      if (!path) return;
      const definition = definitionForName(path.groups[0].name, version, registry);
      const component = definition?.component || path.component || "Rendered groups";
      const reference = component === "Rendered groups"
        ? { kind: "none", key: "" }
        : { kind: "component", key: component };
      const container = structureContainer(containers, component, reference);
      let groups = container.groups;
      let node = null;
      path.groups.forEach((segment, depth) => {
        const nestedDefinition = definitionForName(segment.name, version, registry);
        node = structureGroup(groups, segment.name, nestedDefinition, "indexed");
        const entry = structureEntry(node, segment.index);
        if (depth === path.groups.length - 1) {
          entry.records.push(record);
          record.structure_path = path.groups
            .map((part) => `${part.name}[${part.index}]`)
            .join(".");
          structured.add(record.ordinal);
        }
        groups = entry.groups;
      });
      explicitNames.add(folded(path.groups[0].name));
    });

    containers.forEach((container) => {
      container.groups.forEach((group) => validateIndexedGroup(group, records, diagnostics));
    });
    countedGroups(
      records,
      version,
      registry,
      explicitNames,
      containers,
      diagnostics,
      structured,
      direction,
    );

    records.forEach((record) => {
      if (structured.has(record.ordinal) || record.input_key.includes("[")) return;
      const path = componentPath(record.input_key, registry);
      if (!path) return;
      structureContainer(containers, path.component, path.reference).fields.push(record);
      structured.add(record.ordinal);
    });

    const materialized = [...containers.values()]
      .map(materializeContainer)
      .filter((container) => container.fields.length || container.groups.length);
    return {
      containers: materialized,
      diagnostics,
      group_count: countGroups(materialized),
      entry_count: countEntries(materialized),
    };
  }

  function indexedPath(key, registry) {
    const parts = String(key).replace(/^#/, "").split(".");
    const groups = [];
    const prefix = [];
    let found = false;
    parts.forEach((part, position) => {
      const indexed = part.match(/^(.+)\[(\d+)\]$/);
      if (
        indexed &&
        (registry.groupsByName.has(folded(indexed[1])) || position < parts.length - 1)
      ) {
        found = true;
        groups.push({ name: indexed[1], index: Number(indexed[2]) });
      } else if (!found) {
        prefix.push(part);
      }
    });
    return groups.length
      ? { groups, component: prefix.length ? prefix.join(".") : null }
      : null;
  }

  function componentPath(key, registry) {
    const spelling = String(key).replace(/^#/, "");
    if (/^\d+$/.test(spelling) || spelling.includes("[")) return null;
    const parts = spelling.split(".");
    if (parts.length < 2) return null;
    const container = parts[parts.length - 2];
    const component = registry.byComponent.get(folded(container));
    if (component) {
      return {
        component: component.name,
        reference: { kind: "component", key: component.name },
      };
    }
    const field = registry.byName.get(folded(container));
    return field
      ? {
          component: field.name,
          reference: { kind: "field", key: field.tag ?? field.name },
        }
      : null;
  }

  function structureContainer(containers, name, reference = null) {
    if (!containers.has(name)) {
      containers.set(name, {
        name,
        reference: reference || { kind: "component", key: name },
        fields: [],
        groups: [],
      });
    }
    return containers.get(name);
  }

  function structureGroup(groups, name, definition, source) {
    let group = groups.find((candidate) => folded(candidate.name) === folded(name));
    if (!group) {
      group = {
        name: definition?.name || name,
        tag: definition?.tag ?? null,
        component: definition?.component || null,
        source,
        expected: null,
        state: source === "indexed" ? "inferred" : "resolved",
        entries: new Map(),
        diagnostics: [],
        _definition: definition || null,
      };
      groups.push(group);
    }
    return group;
  }

  function structureEntry(group, index) {
    if (!group.entries.has(index)) {
      group.entries.set(index, { index, records: [], groups: [] });
    }
    return group.entries.get(index);
  }

  function definitionForName(name, version, registry) {
    const candidates = eligibleDefinitions(registry.groupsByName.get(folded(name)), version);
    return candidates.length === 1 ? candidates[0] : null;
  }

  function eligibleDefinitions(candidates, version) {
    const available = list(candidates);
    if (!version) return available;
    const exact = available.filter(
      (candidate) => candidate.versions.includes(version) || candidate.versions.includes("*"),
    );
    return exact.length ? exact : available;
  }

  function validateIndexedGroup(group, records, diagnostics) {
    const entries = [...group.entries.values()].sort((left, right) => left.index - right.index);
    group.entries = entries;
    entries.forEach((entry) => {
      entry.groups.forEach((nested) => validateIndexedGroup(nested, records, diagnostics));
    });
    const indexes = entries.map((entry) => entry.index);
    if (indexes.some((index, position) => index !== position)) {
      group.state = "invalid";
      group.diagnostics.push("Entry indexes must be contiguous and start at zero");
    }
    if (group._definition?.delimiter) {
      const allowed = memberTags(group._definition.members);
      entries.forEach((entry) => {
        if (entry.records.length && recordTag(entry.records[0]) !== group._definition.delimiter) {
          group.state = "invalid";
          group.diagnostics.push(`Entry ${entry.index} must start with the declared delimiter`);
        }
        if (entry.records.some((record) => !allowed.has(recordTag(record)))) {
          group.state = "invalid";
          group.diagnostics.push(`Entry ${entry.index} contains a field outside this declaration`);
        }
      });
    }
    if (group.tag === null) {
      group.diagnostics.forEach((reason) =>
        diagnostics.push({ state: group.state, source: group.name, reason }),
      );
      return;
    }
    const counts = records.filter(
      (record) =>
        !record.input_key.includes("[") &&
        (String(record.tag) === String(group.tag) || folded(record.input_key) === folded(group.name)),
    );
    if (!counts.length) {
      group.diagnostics.forEach((reason) =>
        diagnostics.push({ state: group.state, source: group.name, reason }),
      );
      return;
    }
    if (counts.length > 1 || !/^\d+$/.test(counts[0].input_value)) {
      group.state = "invalid";
      group.diagnostics.push("The group count must occur once and be an unsigned integer");
    } else {
      group.expected = Number(counts[0].input_value);
      if (group.expected !== entries.length) {
        group.state = "mismatch";
        group.diagnostics.push(`Count declares ${group.expected}; indexes contain ${entries.length}`);
      } else if (!group.diagnostics.length) {
        group.state = "resolved";
      }
    }
    group.diagnostics.forEach((reason) =>
      diagnostics.push({ state: group.state, source: group.name, reason }),
    );
  }

  function countedGroups(
    records,
    version,
    registry,
    explicitNames,
    containers,
    diagnostics,
    structured,
    direction,
  ) {
    const occurrences = new Map();
    records.forEach((record, position) => {
      const tag = recordTag(record);
      if (!tag || !registry.groupsByTag.has(tag)) return;
      if (!occurrences.has(tag)) occurrences.set(tag, []);
      occurrences.get(tag).push(position);
    });
    const consumed = new Set();
    occurrences.forEach((positions, tag) => {
      const namedCandidates = eligibleDefinitions(registry.groupsByTag.get(tag), version).filter(
        (candidate) => !explicitNames.has(folded(candidate.name)),
      );
      if (!namedCandidates.length) return;
      if (positions.length !== 1) {
        diagnostics.push({
          state: "ambiguous",
          source: `${namedCandidates[0].name} <${tag}>`,
          reason: "A counted group must have exactly one count field",
        });
        return;
      }
      const start = positions[0];
      if (consumed.has(start)) return;
      const attempts = namedCandidates
        .map((definition) => parseCounted(records, start, definition))
        .filter((attempt) => attempt.valid);
      const unique = attempts.filter(
        (attempt, index) =>
          attempts.findIndex(
            (candidate) =>
              candidate.definition.signature === attempt.definition.signature &&
              candidate.end === attempt.end,
          ) === index,
      );
      if (unique.length !== 1) {
        diagnostics.push({
          state: unique.length ? "ambiguous" : "invalid",
          source: `${namedCandidates[0].name} <${tag}>`,
          reason: unique.length
            ? `Message context is required to choose between ${unique.length} registry shapes`
            : "Count, delimiter, or entry boundary does not match the registry declaration",
        });
        return;
      }
      const found = unique[0];
      const container = structureContainer(containers, found.definition.component);
      container.groups.push(found.group);
      for (let position = start; position < found.end; position += 1) {
        consumed.add(position);
        structured.add(records[position].ordinal);
      }
      qualifyCountedEntries(found.group, "", direction);
    });
  }

  function parseCounted(records, start, definition) {
    const countRecord = records[start];
    if (!/^\d+$/.test(countRecord.input_value)) return { valid: false, definition };
    const expected = Number(countRecord.input_value);
    if (!Number.isSafeInteger(expected)) return { valid: false, definition };
    const group = {
      name: definition.name,
      tag: definition.tag,
      component: definition.component,
      source: "counted",
      expected,
      state: "resolved",
      entries: [],
      diagnostics: [],
      count_record: countRecord,
    };
    if (expected === 0) return { valid: true, definition, group, end: start + 1 };
    if (!definition.delimiter || recordTag(records[start + 1]) !== definition.delimiter) {
      return { valid: false, definition };
    }
    const allowed = memberTags(definition.members);
    let end = start + 1;
    while (end < records.length && allowed.has(recordTag(records[end]))) end += 1;
    const body = records.slice(start + 1, end);
    const delimiterPositions = [];
    body.forEach((record, index) => {
      if (recordTag(record) === definition.delimiter) delimiterPositions.push(index);
    });
    if (delimiterPositions.length !== expected) return { valid: false, definition };
    delimiterPositions.forEach((position, index) => {
      const next = delimiterPositions[index + 1] ?? body.length;
      group.entries.push({
        index,
        records: body.slice(position, next),
        groups: [],
      });
    });
    group.entries.forEach((entry) => nestCountedGroups(entry, definition));
    return { valid: true, definition, group, end };
  }

  function nestCountedGroups(entry, definition) {
    const nested = list(definition.members).filter((member) => member.kind === "group");
    if (!nested.length) return;
    const retained = [];
    for (let position = 0; position < entry.records.length; ) {
      const member = nested.find((candidate) => String(candidate.tag) === recordTag(entry.records[position]));
      if (!member) {
        retained.push(entry.records[position]);
        position += 1;
        continue;
      }
      const nestedDefinition = {
        name: member.name,
        tag: member.tag,
        component: definition.component,
        versions: definition.versions,
        members: list(member.members),
        delimiter: firstMember(member.members),
        signature: groupSignature(member.name, member.tag, member.members),
      };
      const attempt = parseCounted(entry.records, position, nestedDefinition);
      if (!attempt.valid) {
        retained.push(entry.records[position]);
        position += 1;
        continue;
      }
      entry.groups.push(attempt.group);
      position = attempt.end;
    }
    entry.records = retained;
  }

  function memberTags(members) {
    const tags = new Set();
    list(members).forEach((member) => {
      if (member.tag !== undefined) tags.add(String(member.tag));
      if (member.kind === "group") {
        memberTags(member.members).forEach((tag) => tags.add(tag));
      }
    });
    return tags;
  }

  function recordTag(record) {
    if (record.tag !== null && record.tag !== undefined) return String(record.tag);
    return /^\d+$/.test(record.input_key) ? record.input_key : null;
  }

  function qualifyCountedEntries(group, prefix, direction) {
    if (group.count_record) {
      group.count_record.structure_path = prefix.replace(/\.$/, "");
      if (direction === "decode") group.count_record.output_key = `${prefix}${group.name}`;
    }
    group.entries.forEach((entry) => {
      const lead = `${prefix}${group.name}[${entry.index}]`;
      entry.records.forEach((record) => {
        record.structure_path = lead;
        if (direction === "decode") {
          record.output_key = `${lead}.${record.name || record.input_key}`;
        }
      });
      entry.groups.forEach((nested) => qualifyCountedEntries(nested, `${lead}.`, direction));
    });
  }

  function materializeContainer(container) {
    return {
      ...container,
      groups: container.groups.map(materializeGroup),
    };
  }

  function materializeGroup(group) {
    const { _definition: _definition, ...publicGroup } = group;
    const entries = group.entries instanceof Map
      ? [...group.entries.values()].sort((left, right) => left.index - right.index)
      : group.entries;
    return {
      ...publicGroup,
      entries: entries.map((entry) => ({
        ...entry,
        groups: entry.groups.map(materializeGroup),
      })),
    };
  }

  function countGroups(containers) {
    const nested = (group) => 1 + group.entries.reduce(
      (total, entry) => total + entry.groups.reduce((sum, child) => sum + nested(child), 0),
      0,
    );
    return containers.reduce(
      (total, container) => total + container.groups.reduce((sum, group) => sum + nested(group), 0),
      0,
    );
  }

  function countEntries(containers) {
    const nested = (group) => group.entries.length + group.entries.reduce(
      (total, entry) => total + entry.groups.reduce((sum, child) => sum + nested(child), 0),
      0,
    );
    return containers.reduce(
      (total, container) => total + container.groups.reduce((sum, group) => sum + nested(group), 0),
      0,
    );
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
      reason ? ` title="${escape(reason)}" aria-label="${escape(`${state}: ${reason}`)}"` : ""
    }>${escape(state.replaceAll("-", " "))}</span>`;
  }

  function displayValue(value) {
    if (value === null || value === undefined) return "—";
    return typeof value === "string" ? value : JSON.stringify(value);
  }

  function issueRecords(records, unparsed) {
    return [
      ...records
        .filter((record) => !["resolved", "encoded"].includes(record.state))
        .map((record) => ({
          index: record.index,
          source: record.source,
          state: record.state,
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
    const active = records.filter((record) => !record.shadowed).length;
    select(`[data-${direction}-state]`).textContent = `${number.format(active)} effective · ${number.format(records.length)} parsed`;
  }

  function renderProtocol(direction, protocol) {
    const target = select(`[data-${direction}-protocol]`);
    target.textContent = protocol.label;
    target.dataset.protocol = protocol.code.toLowerCase();
  }

  function renderStructure(direction, structure) {
    const target = select(`[data-${direction}-structure]`);
    const count = select(`[data-${direction}-structure-count]`);
    count.textContent = `${number.format(structure.group_count)} groups · ${number.format(structure.entry_count)} entries`;
    const diagnostics = structure.diagnostics.length
      ? `<div class="fix-transcribe__structure-issues" role="status">${structure.diagnostics
          .map(
            (issue) => `<p>${stateBadge(issue.state, issue.reason)} <code>${escape(issue.source)}</code> ${escape(issue.reason)}</p>`,
          )
          .join("")}</div>`
      : "";
    const containers = structure.containers.map(structureContainerHtml).join("");
    target.innerHTML = diagnostics || containers
      ? `${diagnostics}<div class="fix-transcribe__components">${containers}</div>`
      : `<p class="fix-transcribe__structure-empty">No declared component path or validated repeating group. The ordered fields remain in the trace below.</p>`;
  }

  function structureContainerHtml(container) {
    const fields = container.fields.length
      ? `<div class="fix-transcribe__component-fields">${container.fields
          .map(structureMemberHtml)
          .join("")}</div>`
      : "";
    const reference = container.reference || { kind: "component", key: container.name };
    const href = `${app.dataset.registry}#${reference.kind}=${encodeURIComponent(reference.key)}`;
    const label = reference.kind === "field" ? "Payload field" : "Component";
    const registryLink = reference.kind === "none"
      ? ""
      : `<a href="${escape(href)}">Registry ↗</a>`;
    return `<article class="fix-transcribe__component">
      <header>
        <div><span>${label}</span><h4>${escape(container.name)}</h4></div>
        ${registryLink}
      </header>
      ${fields}
      ${container.groups.map(structureGroupHtml).join("")}
    </article>`;
  }

  function structureGroupHtml(group) {
    const declared = group.expected === null ? "index-derived" : `${group.entries.length}/${group.expected}`;
    const tag = group.tag === null ? "" : ` &lt;${escape(group.tag)}&gt;`;
    const issues = group.diagnostics.length
      ? `<div class="fix-transcribe__group-warning">${group.diagnostics.map(escape).join(" · ")}</div>`
      : "";
    const entries = group.entries.length
      ? `<ol class="fix-transcribe__entries">${group.entries
          .map(
            (entry) => `<li>
              <div class="fix-transcribe__entry-head"><span>Entry ${number.format(entry.index)}</span><span>${number.format(entry.records.length)} direct fields</span></div>
              <div class="fix-transcribe__members">${entry.records.map(structureMemberHtml).join("")}</div>
              ${entry.groups.map(structureGroupHtml).join("")}
            </li>`,
          )
          .join("")}</ol>`
      : '<p class="fix-transcribe__structure-empty">Declared empty group.</p>';
    return `<section class="fix-transcribe__group">
      <header>
        <div><span>Repeating group</span><h5>${escape(group.name)}${tag}</h5></div>
        <div class="fix-transcribe__group-meta">${stateBadge(group.state)}<span>${escape(declared)}</span></div>
      </header>
      ${issues}${entries}
    </section>`;
  }

  function structureMemberHtml(record) {
    const name = record.name || record.input_key;
    const value = record.decoded ?? record.output_value ?? record.input_value;
    const tag = record.tag === null || record.tag === undefined ? "namespace" : `<${record.tag}>`;
    return `<div class="fix-transcribe__member">
      <span><strong>${escape(name)}</strong><small>${escape(tag)}</small></span>
      <code>${escape(displayValue(value))}</code>
    </div>`;
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

  function debugRecord(
    direction,
    parsed,
    version,
    records,
    structure,
    outputSeparator = "|",
  ) {
    return {
      direction,
      protocol: parsed.protocol,
      input_separator: separatorName(parsed.separator),
      output_separator: direction === "encode" ? separatorName(outputSeparator) : undefined,
      named_input: parsed.named,
      version: version.value,
      version_source: version.source,
      transport: version.transport,
      parsed: records,
      unparsed: parsed.unparsed,
      ignored_envelope: parsed.ignored,
      structure,
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
      "#": "hash (#)",
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
