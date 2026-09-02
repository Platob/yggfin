# W0·3 — I2: delete three audited-dead definitions

**Runs:** immediately. **Merges first**, before W1 and W2·A rewrite these files.
**Owns:** `rekep/text/entries.py` → `pop_arrow` only; `rekep/entries.py` →
`Entry.pop_arrow` and `Entry.looks_structured_arrow` only.

Read `BRIEF.md` §I2 and `00-STANDING-RULES.md`. This is a 72-line deletion, but
the audit around it is the point — read the "do not delete these" list before
touching anything.

## Delete exactly these three

| definition | anchor | lines |
|---|---|---:|
| `pop_arrow` (module fn) | `text/entries.py:697-750` | 54 |
| `Entry.pop_arrow` (its only caller) | `entries.py:262-273` | 12 |
| `Entry.looks_structured_arrow` (wrapper) | `entries.py:221-226` | 6 |

A reachability sweep found these have **zero call sites**, no `__all__` entry, no
test, no doc reference, and no indirect dispatch. `pop_arrow` is a self-contained
two-layer pair: the classmethod's only caller is nothing, and the module
function's only caller is the classmethod.

Two traps:
- It is **not** `Rule.pop` (`fix/rules.py:180,229,237`), which is live and
  consumed by an independent `_popped_pairs` (`fix/transcribe.py:398,2295`).
- `Entry.looks_structured_arrow` is a wrapper — **delete only the classmethod**.
  The module-level `looks_structured_arrow` (`text/entries.py:105-117`) is live
  from `_payload_arrow` (`:149`).

Two things to know before deleting:
- `Entry` is published in `rekep.__all__`, so removing two of its public
  classmethods is a **nominal API break** even though nothing in this repo, its
  tests, benchmarks, tasks, or docs calls them. Note it in the report; do not let
  it stop the deletion.
- After removing `pop_arrow`, no import in `text/entries.py` is orphaned —
  `column_names` (`:287`), `build_list` (`:236,310`), `dense_counts` (`:238`) and
  `null_mask` (`:240,310`) all have other callers. **Re-check anyway.**

## Do NOT delete these — they were checked and are reachable

- `_timezone_transitions` / `_windows_utc_micros` / `_windows_local_micros` /
  `_datetime_micros` (`text/text_file.py:1532-1632`, ~95 lines) — live behind
  `os.name == "nt"` guards (`:978`, `:1523`), unreachable on POSIX. **Windows
  code, not dead code**, and untested here (the `windows`/`posix` fixtures at
  `tests/test_arrow_file_io.py:34,39` patch `arrow_file_io._WINDOWS`, not
  `os.name`) — risky to touch, not removable.
- `_windowed_batches` (`text/text_file.py:1280-1340`) — called from
  `into_arrow_batches` (`:569`) and `text_files.py:524`, tested.
- `_plugin_keys` (`text/entries.py:244-272`) and `_renamed_keys` (`:275-310`) —
  live from the `plugin_keys` branch of `normalized_arrow` (`:196-215`), a
  documented config surface.
- `unix_of` — two distinct functions, neither in `text/**`: `times.py:274-279`
  and `fix/fields.py:401-462` (`lru_cache(maxsize=8192)`, load-bearing).
- `Protocol.REFERENTIAL` **is** set, in `fix/rules.py:395` via `:233-238` and
  `text/message.py:321`. Proven by `tests/text/test_message.py:909`.
- `TextFile.read1` / `readinto1` (`text/text_file.py:912-920`) —
  `io.BufferedIOBase` overrides; the protocol is the caller.

**Six of the nine things that looked dead were reachable** through a guard, a
config branch, a protocol, or a getattr registry. Before deleting anything not in
the table above, check for those four mechanisms specifically.

## Check

```python
import subprocess
for name in DELETED:                      # the exact identifiers you removed
    hits = subprocess.run(["git", "grep", "-n", "--", name],
                          capture_output=True, text=True).stdout
    assert not hits, f"{name} still referenced:\n{hits}"
```

Plus: full test suite green. No bench run needed — this frees no measurable
memory and should claim none.

## Deliverable

One commit, `I2: delete three dead definitions`, reporting each deletion **with
its call-site evidence** and noting the nominal `rekep.__all__` API break.
