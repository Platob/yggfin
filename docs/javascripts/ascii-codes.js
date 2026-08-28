// The packing every stable code in this package is stored as, run in the page.
//
// `Ascii32` and `Ascii64` are one rule at two widths: the printable ASCII of a
// code, left-justified into four or eight bytes, padded right with NULs, read
// big-endian as a signed integer. That is the whole of it -- so the encoder
// here is the arithmetic itself rather than a table of answers, and a reader
// can type a code the package has never seen and get the value it would store.
//
// BigInt throughout: an eight-byte code packs past 2^53, so a Number would
// round `PENDING_NEW` into a different code on the way to the screen.
(() => {
  "use strict";

  const app = document.querySelector("[data-ascii-codes]");
  if (!app) return;

  const WIDTHS = { 4: "Ascii32", 8: "Ascii64" };

  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (character) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character],
    );

  const grouped = (value) => String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ",");

  // -- the packing, byte for byte as `ascii_codes.py` does it ----------------

  /** Every reason a spelling is not a code of this width, or "" when it is. */
  function refuse(text, width) {
    if (!text) return "a code is never empty";
    for (const character of text) {
      const point = character.codePointAt(0);
      if (point > 0x7f) return `${JSON.stringify(character)} is not ASCII`;
      if (point < 32 || point > 126) return `${JSON.stringify(character)} is not printable`;
    }
    if (text.length > width) {
      return `${text.length} characters does not fit ${width} bytes`;
    }
    return "";
  }

  /** The signed big-endian integer `text` is stored as at this width. */
  function pack(text, width) {
    let packed = 0n;
    for (let index = 0; index < width; index += 1) {
      const byte = index < text.length ? BigInt(text.charCodeAt(index)) : 0n;
      packed = (packed << 8n) | byte;
    }
    // Printable ASCII never sets the top bit, so a valid code is never
    // negative -- but the stored column is signed, and a value typed into the
    // decoder may be.
    const half = 1n << BigInt(8 * width - 1);
    return packed >= half ? packed - (half << 1n) : packed;
  }

  /** The spelling a stored integer decodes to, or an object saying why not. */
  function unpack(packed, width) {
    const half = 1n << BigInt(8 * width - 1);
    if (packed < -half || packed >= half) {
      return { error: `${packed} does not fit a signed ${width * 8}-bit integer` };
    }
    const bits = (packed + (half << 1n)) % (half << 1n);
    const bytes = [];
    for (let index = width - 1; index >= 0; index -= 1) {
      bytes.push(Number((bits >> BigInt(8 * index)) & 0xffn));
    }
    while (bytes.length && bytes[bytes.length - 1] === 0) bytes.pop();
    if (bytes.includes(0)) return { error: "a NUL inside the code, not padding after it" };
    const bad = bytes.find((byte) => byte < 32 || byte > 126);
    if (bad !== undefined) return { error: `byte ${bad} is not printable ASCII` };
    return { text: bytes.map((byte) => String.fromCharCode(byte)).join("") };
  }

  const hex = (packed, width) => {
    const half = 1n << BigInt(8 * width - 1);
    const bits = (packed + (half << 1n)) % (half << 1n);
    return "0x" + bits.toString(16).padStart(width * 2, "0").toUpperCase();
  };

  // -- the page --------------------------------------------------------------

  app.classList.add("fix-registry", "ascii-codes");
  app.innerHTML = `
    <form class="ascii-codes__form" data-ascii-form>
      <fieldset class="ascii-codes__width">
        <legend>Width</legend>
        <label><input type="radio" name="width" value="4"> <code>Ascii32</code> — 4 bytes, <code>int32</code></label>
        <label><input type="radio" name="width" value="8" checked> <code>Ascii64</code> — 8 bytes, <code>int64</code></label>
      </fieldset>
      <div class="ascii-codes__pair">
        <label class="ascii-codes__field">
          <span>Code</span>
          <input type="text" name="code" value="ORDER" autocomplete="off" spellcheck="false"
                 placeholder="BUY">
        </label>
        <label class="ascii-codes__field">
          <span>Stored value</span>
          <input type="text" name="packed" inputmode="numeric" autocomplete="off"
                 spellcheck="false" placeholder="5715705941605744640">
        </label>
      </div>
      <p class="ascii-codes__problem" data-ascii-problem role="status" aria-live="polite" hidden></p>
    </form>
    <div class="ascii-codes__result" data-ascii-result></div>
    <section class="ascii-codes__known" data-ascii-known hidden>
      <h3>Codes this package compiles</h3>
      <p class="fix-registry__muted" data-ascii-known-note></p>
      <label class="ascii-codes__field">
        <span>Enum</span>
        <select name="enum" data-ascii-enum></select>
      </label>
      <div class="fix-registry__table-wrap ascii-codes__table-wrap">
        <table class="fix-registry__table">
          <thead><tr><th>Key</th><th>Code</th><th class="ascii-codes__number">Stored value</th><th>FIX</th></tr></thead>
          <tbody data-ascii-members></tbody>
        </table>
      </div>
    </section>`;

  const form = app.querySelector("[data-ascii-form]");
  const problem = app.querySelector("[data-ascii-problem]");
  const result = app.querySelector("[data-ascii-result]");
  const known = app.querySelector("[data-ascii-known]");
  const knownNote = app.querySelector("[data-ascii-known-note]");
  const chooser = app.querySelector("[data-ascii-enum]");
  const members = app.querySelector("[data-ascii-members]");

  const widthOf = () => Number(form.elements.width.value);

  let catalog = { enums: [] };
  /** `{stored value: [enum.KEY, ...]}` so a packed integer names what compiles it. */
  let byValue = new Map();

  function say(message) {
    problem.textContent = message;
    problem.hidden = !message;
  }

  function render(text, packed, width) {
    const bytes = [];
    for (let index = 0; index < width; index += 1) {
      const character = index < text.length ? text[index] : "";
      bytes.push({
        at: width - 1 - index,
        character,
        byte: character ? character.charCodeAt(0) : 0,
      });
    }
    const claims = byValue.get(String(packed)) || [];
    result.innerHTML = `
      <dl class="fix-registry__facts ascii-codes__facts">
        <dt>Stored</dt><dd><code>${escape(grouped(packed))}</code></dd>
        <dt>Hex</dt><dd><code>${escape(hex(packed, width))}</code></dd>
        <dt>Type</dt><dd><code>${escape(WIDTHS[width])}</code> → <code>${width === 4 ? "int32" : "int64"}</code></dd>
        ${claims.length ? `<dt>Compiled as</dt><dd>${claims.map((one) => `<code>${escape(one)}</code>`).join(" · ")}</dd>` : ""}
      </dl>
      <div class="fix-registry__table-wrap ascii-codes__table-wrap">
        <table class="fix-registry__table ascii-codes__bytes">
          <thead><tr><th>Byte</th><th>Character</th><th class="ascii-codes__number">ASCII</th><th class="ascii-codes__number">Contribution</th></tr></thead>
          <tbody>${bytes
            .map(
              ({ at, character, byte }) => `<tr${byte ? "" : ' data-pad=""'}>
                <td><code>${width - 1 - at}</code></td>
                <td>${character ? `<code>${escape(character)}</code>` : '<span class="fix-registry__muted">NUL</span>'}</td>
                <td class="ascii-codes__number"><code>${byte}</code></td>
                <td class="ascii-codes__number"><code>${escape(grouped(BigInt(byte) << BigInt(8 * at)))}</code></td>
              </tr>`,
            )
            .join("")}</tbody>
        </table>
      </div>
      <p class="fix-registry__muted">Every byte is the character times <code>256</code> to the power of
      its distance from the right, so the sum orders exactly as the text does.</p>`;
  }

  function fromCode() {
    const width = widthOf();
    const text = form.elements.code.value.trim().toUpperCase();
    const refused = refuse(text, width);
    if (refused) {
      say(refused);
      form.elements.packed.value = "";
      result.innerHTML = "";
      return;
    }
    say("");
    const packed = pack(text, width);
    form.elements.packed.value = String(packed);
    render(text, packed, width);
  }

  function fromPacked() {
    const width = widthOf();
    const raw = form.elements.packed.value.trim().replace(/[\s,_]/g, "");
    if (!/^-?\d+$/.test(raw)) {
      say(raw ? "a stored value is an integer" : "");
      result.innerHTML = "";
      return;
    }
    const decoded = unpack(BigInt(raw), width);
    if (decoded.error) {
      say(decoded.error);
      form.elements.code.value = "";
      result.innerHTML = "";
      return;
    }
    say("");
    form.elements.code.value = decoded.text;
    render(decoded.text, BigInt(raw), width);
  }

  form.elements.code.addEventListener("input", fromCode);
  form.elements.packed.addEventListener("input", fromPacked);
  for (const radio of form.elements.width) radio.addEventListener("change", fromCode);
  form.addEventListener("submit", (event) => event.preventDefault());

  // -- the compiled vocabularies, where the build published them -------------

  function renderMembers() {
    const chosen = catalog.enums.find((one) => one.name === chooser.value);
    if (!chosen) return;
    members.innerHTML = chosen.members
      .map(
        (member) => `<tr>
          <td><code>${escape(member.key)}</code></td>
          <td>${member.code ? `<code>${escape(member.code)}</code>` : '<span class="fix-registry__muted">—</span>'}</td>
          <td class="ascii-codes__number"><code>${escape(grouped(member.value))}</code></td>
          <td>${member.fix ? `<code>${escape(member.fix)}</code>` : '<span class="fix-registry__muted">—</span>'}</td>
        </tr>`,
      )
      .join("");
    knownNote.textContent =
      `${chosen.name} is an ${chosen.base} code stored as ${chosen.stored}` +
      `, ${chosen.open ? "an open vocabulary that registers codes it meets" : "a closed set"}` +
      `, ${chosen.members.length} compiled.`;
    form.elements.width.value = String(chosen.byte_width);
  }

  members.addEventListener("click", (event) => {
    const row = event.target.closest("tr");
    const code = row && row.querySelector("td:nth-child(2) code");
    if (!code) return;
    form.elements.code.value = code.textContent;
    fromCode();
    form.elements.code.focus();
  });

  chooser.addEventListener("change", () => {
    renderMembers();
    fromCode();
  });

  fromCode();

  fetch(app.dataset.source, { cache: "no-cache" })
    .then((response) => (response.ok ? response.json() : Promise.reject(response.status)))
    .then((loaded) => {
      catalog = loaded;
      byValue = new Map();
      for (const one of catalog.enums) {
        for (const member of one.members) {
          const claims = byValue.get(member.value) || [];
          claims.push(`${one.name}.${member.key}`);
          byValue.set(member.value, claims);
        }
      }
      chooser.innerHTML = catalog.enums
        .map((one) => `<option value="${escape(one.name)}">${escape(one.name)}</option>`)
        .join("");
      known.hidden = false;
      renderMembers();
      fromCode();
    })
    .catch(() => {
      // The encoder is arithmetic and needs nothing; only the picker does.
      known.hidden = true;
    });
})();
