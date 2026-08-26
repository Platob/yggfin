# Transcribe FIX

Decode or encode FIX, UL, and FIX-wrapped UL in the browser. Pair order,
repeating groups, and unknown values are preserved.

<div class="fix-registry fix-transcribe" data-fix-transcribe
     data-source="../../assets/fix-registry.json" data-registry="../registry/">
  <p class="fix-registry__status" data-transcribe-status role="status" aria-live="polite">
    Loading FIX registry…
  </p>

  <div data-transcribe-ready hidden>
    <nav class="fix-registry__jump" aria-label="Transcription sections">
      <a href="#decode">Decode</a>
      <a href="#encode">Encode</a>
      <a href="../registry/">Browse registry</a>
      <span class="fix-transcribe__registry-count" data-registry-count></span>
    </nav>

    <section id="decode" class="fix-registry__section fix-transcribe__section"
             aria-labelledby="decode-title">
      <header>
        <div>
          <p class="fix-registry__eyebrow">01 / WIRE → DEBUG</p>
          <h2 id="decode-title" tabindex="-1">Decode</h2>
        </div>
        <div class="fix-transcribe__section-state">
          <span class="fix-transcribe__protocol" data-decode-protocol>Waiting</span>
          <output data-decode-state aria-live="polite">Ready</output>
        </div>
      </header>

      <p>Resolve tags, values, components, and unknown pairs without changing order.</p>

      <form class="fix-transcribe__form" data-decode-form>
        <div class="fix-transcribe__editor">
          <label for="fix-decode-input">FIX text</label>
          <textarea id="fix-decode-input" name="text" rows="8" spellcheck="false"
                    autocomplete="off">8=FIX.4.4|35=D|11=ORD-42|453=2|448=BUY-A|447=D|452=3|448=BUY-B|447=D|452=3|10=000|</textarea>
          <div class="fix-transcribe__controls">
            <label>
              <span>Version</span>
              <select name="version"><option value="">Auto</option></select>
            </label>
            <label>
              <span>Input separator</span>
              <select name="separator">
                <option value="auto">Auto</option>
                <option value="pipe">Pipe |</option>
                <option value="soh">SOH</option>
                <option value="eot-etx">EOT + ETX</option>
                <option value="caret-a">Caret A ^A</option>
                <option value="caret">Caret ^</option>
                <option value="semicolon">Semicolon ;</option>
                <option value="hash">Hash #</option>
                <option value="newline">New line</option>
              </select>
            </label>
          </div>
          <div class="fix-transcribe__actions">
            <button type="submit">Decode</button>
            <button type="button" data-decode-clear>Clear</button>
          </div>
          <div class="fix-transcribe__samples" aria-label="Decode examples">
            <span>Examples</span>
            <button type="button" data-decode-sample="fix">FIX</button>
            <button type="button" data-decode-sample="ul">UL</button>
            <button type="button" data-decode-sample="ul-wire">UL wire</button>
          </div>
        </div>

        <div class="fix-transcribe__output">
          <div class="fix-transcribe__output-head">
            <span>Named text</span>
            <button type="button" data-copy-decode>Copy</button>
          </div>
          <pre tabindex="0"><code data-decode-output></code></pre>
          <p class="fix-transcribe__note" data-decode-version></p>
        </div>
      </form>

      <details class="fix-transcribe__rules">
        <summary>Parsing rules</summary>
        <p>Unparsed tokens fail FIX syntax. Unresolved pairs remain ordered but have no registry identity.</p>
        <p>Indexed UL groups are exact. Numeric groups require an unambiguous count, delimiter, version, and registry shape.</p>
      </details>

      <div class="fix-registry__summary fix-transcribe__summary" aria-label="Decode summary">
        <div><strong data-decode-parsed>0</strong><span>parsed pairs</span></div>
        <div><strong data-decode-resolved>0</strong><span>resolved</span></div>
        <div><strong data-decode-unresolved>0</strong><span>unresolved</span></div>
        <div><strong data-decode-unparsed>0</strong><span>unparsed</span></div>
      </div>

      <section class="fix-transcribe__structure" aria-labelledby="decode-structure-title">
        <header>
          <div>
            <p class="fix-registry__eyebrow">Resolved structure</p>
            <h3 id="decode-structure-title">Components &amp; repeating groups</h3>
          </div>
          <span class="fix-transcribe__structure-count" data-decode-structure-count></span>
        </header>
        <div data-decode-structure></div>
      </section>

      <div class="fix-registry__table-wrap fix-transcribe__table-wrap" tabindex="0"
           role="region" aria-label="Decoded ordered fields">
        <table class="fix-registry__table">
          <thead>
            <tr><th>#</th><th>Input</th><th>Registry field</th><th>Raw</th><th>Parsed</th><th>Decoded</th><th>Meaning</th><th>Status</th></tr>
          </thead>
          <tbody data-decode-rows></tbody>
        </table>
      </div>

      <details class="fix-transcribe__issues" data-decode-issues-wrap hidden>
        <summary>Diagnostics and unparsed input <span data-decode-issue-count></span></summary>
        <div class="fix-registry__table-wrap" tabindex="0" role="region"
             aria-label="Decode issues">
          <table class="fix-registry__table">
            <thead><tr><th>#</th><th>Source</th><th>State</th><th>Reason</th></tr></thead>
            <tbody data-decode-issues></tbody>
          </table>
        </div>
      </details>

      <details class="fix-transcribe__debug">
        <summary>Full debug record</summary>
        <div class="fix-transcribe__output-head">
          <span>JSON</span>
          <button type="button" data-copy-decode-debug>Copy</button>
        </div>
        <pre tabindex="0"><code data-decode-debug></code></pre>
      </details>
    </section>

    <section id="encode" class="fix-registry__section fix-transcribe__section"
             aria-labelledby="encode-title">
      <header>
        <div>
          <p class="fix-registry__eyebrow">02 / TEXT → WIRE</p>
          <h2 id="encode-title" tabindex="-1">Encode</h2>
        </div>
        <div class="fix-transcribe__section-state">
          <span class="fix-transcribe__protocol" data-encode-protocol>Waiting</span>
          <output data-encode-state aria-live="polite">Ready</output>
        </div>
      </header>

      <p>Encode canonical names and semantic values while preserving repetitions and unknown pairs.</p>

      <form class="fix-transcribe__form" data-encode-form>
        <div class="fix-transcribe__editor">
          <label for="fix-encode-input">Named text</label>
          <textarea id="fix-encode-input" name="text" rows="8" spellcheck="false"
                    autocomplete="off">BeginString=FIX.4.4|MsgType=NewOrderSingle|ClOrdID=ORD-42|NoPartyIDs=2|NoPartyIDs[0].PartyID=BUY-A|NoPartyIDs[0].PartyIDSource=proprietary/customcode|NoPartyIDs[0].PartyRole=clientid|NoPartyIDs[1].PartyID=BUY-B|NoPartyIDs[1].PartyIDSource=proprietary/customcode|NoPartyIDs[1].PartyRole=clientid|CheckSum=000|</textarea>
          <div class="fix-transcribe__controls fix-transcribe__controls--three">
            <label>
              <span>Version</span>
              <select name="version"><option value="">Auto</option></select>
            </label>
            <label>
              <span>Input separator</span>
              <select name="separator">
                <option value="auto">Auto</option>
                <option value="pipe">Pipe |</option>
                <option value="soh">SOH</option>
                <option value="eot-etx">EOT + ETX</option>
                <option value="caret-a">Caret A ^A</option>
                <option value="caret">Caret ^</option>
                <option value="semicolon">Semicolon ;</option>
                <option value="hash">Hash #</option>
                <option value="newline">New line</option>
              </select>
            </label>
            <label>
              <span>Output separator</span>
              <select name="output_separator">
                <option value="pipe">Pipe |</option>
                <option value="soh">SOH</option>
                <option value="eot-etx">EOT + ETX</option>
                <option value="caret-a">Caret A ^A</option>
                <option value="caret">Caret ^</option>
                <option value="semicolon">Semicolon ;</option>
                <option value="hash">Hash #</option>
              </select>
            </label>
          </div>
          <div class="fix-transcribe__actions">
            <button type="submit">Encode</button>
            <button type="button" data-encode-clear>Clear</button>
          </div>
          <div class="fix-transcribe__samples" aria-label="Encode examples">
            <span>Examples</span>
            <button type="button" data-encode-sample="fix">FIX</button>
            <button type="button" data-encode-sample="ul">UL</button>
            <button type="button" data-encode-sample="ul-wire">UL wire</button>
          </div>
        </div>

        <div class="fix-transcribe__output">
          <div class="fix-transcribe__output-head">
            <span>FIX text</span>
            <button type="button" data-copy-encode>Copy wire</button>
          </div>
          <pre tabindex="0"><code data-encode-output></code></pre>
          <p class="fix-transcribe__note" data-encode-version></p>
          <p class="fix-transcribe__note">BodyLength &lt;9&gt; and CheckSum &lt;10&gt; are preserved, not validated or recalculated.</p>
        </div>
      </form>

      <div class="fix-registry__summary fix-transcribe__summary" aria-label="Encode summary">
        <div><strong data-encode-parsed>0</strong><span>parsed pairs</span></div>
        <div><strong data-encode-resolved>0</strong><span>resolved</span></div>
        <div><strong data-encode-unresolved>0</strong><span>unresolved</span></div>
        <div><strong data-encode-unparsed>0</strong><span>unparsed</span></div>
      </div>

      <section class="fix-transcribe__structure" aria-labelledby="encode-structure-title">
        <header>
          <div>
            <p class="fix-registry__eyebrow">Resolved structure</p>
            <h3 id="encode-structure-title">Components &amp; repeating groups</h3>
          </div>
          <span class="fix-transcribe__structure-count" data-encode-structure-count></span>
        </header>
        <div data-encode-structure></div>
      </section>

      <div class="fix-registry__table-wrap fix-transcribe__table-wrap" tabindex="0"
           role="region" aria-label="Encoded ordered fields">
        <table class="fix-registry__table">
          <thead>
            <tr><th>#</th><th>Input</th><th>Registry field</th><th>Input value</th><th>Wire tag</th><th>Wire value</th><th>Datatype</th><th>Status</th></tr>
          </thead>
          <tbody data-encode-rows></tbody>
        </table>
      </div>

      <details class="fix-transcribe__issues" data-encode-issues-wrap hidden>
        <summary>Diagnostics and unparsed input <span data-encode-issue-count></span></summary>
        <div class="fix-registry__table-wrap" tabindex="0" role="region"
             aria-label="Encode issues">
          <table class="fix-registry__table">
            <thead><tr><th>#</th><th>Source</th><th>State</th><th>Reason</th></tr></thead>
            <tbody data-encode-issues></tbody>
          </table>
        </div>
      </details>

      <details class="fix-transcribe__debug">
        <summary>Full debug record</summary>
        <div class="fix-transcribe__output-head">
          <span>JSON</span>
          <button type="button" data-copy-encode-debug>Copy</button>
        </div>
        <pre tabindex="0"><code data-encode-debug></code></pre>
      </details>
    </section>
  </div>
</div>

<noscript>The transcription workspaces require JavaScript. Use the
<a href="../">FIX registry guide</a> for the equivalent Python APIs.</noscript>
