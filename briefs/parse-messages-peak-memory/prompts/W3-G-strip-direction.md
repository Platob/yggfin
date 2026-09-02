# W3·G — Item G: strip structurally, keep direction, drop repeated payloads (up to 48% of feed C)

**Runs last.** Requires W0·5's prototype numbers and W1 (H) merged. **Land it
behind default-off flags.**
**Owns:** `rekep/text/text_file.py` → the phase-1 strip; `rekep/text/message.py`
→ `vhash` identity and the direction column;
`tasks/parse_messages/parse_messages.yml` → the flags.

Read `BRIEF.md` §G in full, W0·5's prototype report, and `00-STANDING-RULES.md`.

## Why last, and what it needs that no other item needs

Everything else shrinks the **representation**. G removes **bytes and rows the
capture duplicates**. It is the biggest win available — and the only item that
**changes what is stored**, so it must not contaminate measurements against the
pinned baseline.

`[MISSING]` — the brief says G "deliberately violates hard constraints 2 and 3"
and **"needs the sign-off in its carve-out"**. The constraints and the carve-out
were not in the source screenshots. **Get them re-attached and signed off before
landing this.** If out of time overall: G alone on feed C beats A–F combined, but
only with that sign-off; the other items do not need it.

## The pattern

These feeds interleave a per-stage enrichment trace with the messages: the same
payload re-emitted after every stage, each behind different prose, microseconds
apart. Up to **fifteen lines, ~4.5 KB of payload each, one logical message.**
`HEADER_PATTERN` (`text/text_file.py:60-68`) captures `timestamp`, `threadname`,
`plugin`, `level`, then `(?P<body>.*)$` — **so all of that prose is inside
`body`.**

**Why the existing dedup cannot see them.** `text/message.py:530` is
`columns["vhash"] = hash_bytes_arrow(columns["body"])` — vhash over `body` alone,
which is right. Two problems stack:

1. The prose is *in* `body`, so two lines carrying an identical payload hash
   differently.
2. `hash` is `txhash.couple128_arrow(cls._clock_micros(...), vhash)`
   (`:531-533`) and `_clock_micros` (`market/event.py:395-405`) floors to whole
   **microseconds**. The trace lines are **7-14 µs apart**, so even with equal
   vhash they get distinct `hash` and `merge_by: true` cannot collapse them.

**So stripping is not primarily a byte saving — it is the enabler.** Strip, and
vhash becomes a true content identity.

## Implement the four rules W0·5 validated

Take `payload_start`, the suffix rule, `direction_of` and the consecutive-duplicate
rule **from the prototype report**, with its measured counts. Do not re-derive
them, and **never ship a prefix list** — the prose is not a closed set
(3,084-5,846 distinct prefixes per 200k slice) and any literal list is wrong on
arrival. The prototype's post-mortem is why: an earlier regex anchor missed
12,094 payloads on feed A, claimed 1,523 prose lines as payload, and **ate
payload** on 2,873 feed-C lines.

Carry the invariant into production tests: **after stripping, the head must
contain no token run** — `payload_start(head) < 0`, 0 failures over ~800k lines.

### Direction is correctness, not savings

**Direction-blind dedup silently collapses an inbound message with its outbound
copy** — measured consecutive pairs with identical payload but opposite direction:
**feed C 13,431 / 17,241, feed A 10,164, feed B 6,516.** Distinct business events.

Store direction as its own low-cardinality column (3 values — dictionary-encode
per item C, ~1 B/row) **and mix it into `vhash`**, so identity is
`(direction, payload)` rather than payload alone:

```python
# text/message.py:530, replacing hash_bytes_arrow(columns["body"])
identity = pyarrow.compute.binary_join_element_wise(
    direction_code, payload, pyarrow.scalar(b"", pyarrow.binary())
)
columns["vhash"] = hash_bytes_arrow(identity)
```

`couple128_arrow` (`txhash.py:88-101`) already composes an identity out of
`binary_join_element_wise` with an empty separator — **follow that precedent
rather than inventing a framing.** Use a **fixed-width code** so the join stays
unambiguous; a variable-length direction string would let `("in", "x")` and
`("i", "nx")` collide.

Keep the two keyword families **configurable**, leftmost-match-wins — the verb of
the log statement, not a noun inside it.

### Two hard "do nots"

- **Do not drop `body`.** Keep the raw line's body stored as it is today; the
  stripped payload and the direction are **derived** columns. The prose is the only
  evidence of which enrichment stage emitted a line, and constraint 1 treats `body`
  as contract.
- **Do not build a per-thread cache** for rule 4. One previous payload. Per-thread
  buys a few points on feed C and ~0.02 on A and B, against a dict holding one
  payload per live thread with **~21,625 distinct threadnames in 200k feed-A
  lines** (327,986 across a day) at ~4.5 KB each — an unbounded growth term of tens
  to hundreds of MB, **in a brief whose entire point is peak RSS**, and it
  contradicts item C's warning against threadname-keyed structures. If you want
  those points anyway, the only acceptable shape is a **bounded LRU (N = 64-256)
  keyed on the payload's 16-byte hash**, not the payload, so retained state is
  O(N × 16 B). Measure it; do not assume it wins.

## Flags

Rule 4 (the duplicate drop) goes behind an explicit flag, **default off**. These
are **the only runtime flags this whole brief sanctions**, and they exist solely
because G changes stored output. Everything else in the plan deletes what it
replaces.

With the flags off, the item must be a no-op: same rows, same `vhash`, same
`skipped`, byte-identical stored `body`.

## Target numbers

| feed / slice | prefix bytes | dup, direction-blind | **dup, direction-aware** | crossed pairs |
|---|---:|---:|---:|---:|
| C head 200k | 0.69% | 47.74% | **35.86%** | 13,431 |
| C mid 200k | 0.70% | 46.51% | **31.12%** | 17,241 |
| A head 200k | 0.90% | 12.81% | **3.93%** | 10,164 |
| B head 200k | 2.21% | 11.35% | **5.71%** | 6,516 |

**Implement against the direction-aware column.** The direction-blind column
exists only to show the ~9-15 points a naive strip would wrongly collapse.

## Checks

1. **Flags off = no observable change**, per file against `results/baseline/`
   (column-by-column `equals`, `skipped` unchanged).
2. **Flags on:** `skipped` rises by the direction-aware duplicate count and
   **nothing else moves** — every surviving row byte-identical, `body` untouched.
3. `payload_start(head) < 0` over the full slices, 0 failures.
4. No inbound/outbound pair merged — assert the crossed-pair counts above are
   preserved as **distinct** rows.

## Deliverable

Commits for the strip, the direction column + vhash identity, and the flagged
duplicate drop — separately. Report the direction-aware dedup achieved per feed,
and state the sign-off you obtained for the carve-out.
