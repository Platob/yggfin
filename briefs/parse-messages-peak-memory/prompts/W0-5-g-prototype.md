# W0·5 — G prototype: validate the content rules against real bytes (NO production code)

**Runs:** immediately, parallel with everything. **Unblocks W3·G at the end.**
**Owns:** `briefs/parse-messages-peak-memory/g-prototype/` only.
**Edits nothing under `python/src/`.** That restriction is the point of the task.

Read `BRIEF.md` §G in full and `00-STANDING-RULES.md`.

## Why this is a separate, early, code-free task

The brief's opening rule — *"Prototype before you commit... a rule that has not
been run over real bytes is a hypothesis"* — exists **because of item G**. An
earlier revision of the brief shipped a regex anchor,
`#[A-Z0-9_]+=|(?:^|[|\x01 ])\d{1,4}=`, and running it produced three separate
defects:

| defect | feed A | feed C | cause |
|---|---:|---:|---|
| missed a payload the scan finds | 12,094 | 8,028 | key charset excluded `.` and `-` |
| claimed payload in prose with no delimiter | 1,523 | 1,718 | bare `\d{1,4}=` matches prose |
| put the boundary *later* than the scan — **ate payload** | 66 | 2,873 | anchored on a later token |

It also over-stripped **+1.79% of body bytes** on one slice while *under*-stripping
on another, and a stricter regex form backtracks catastrophically on 4.5 KB
payloads — it did not finish a 200k-line slice in 10 minutes, where the linear
scan does it in **0.71 µs/line**.

So: measure first, ship later. G lands last (it changes stored output); this task
front-loads everything about it that can be validated without touching `src/`.

## Inputs

Sample slices at `/tmp/<feed>_head.txt` and `/tmp/<feed>_mid.txt` — 200k
header-matched lines each, ~200-450 MB, seconds to scan. Rebuild with
`head -n 200000` and `sed -n '4000000,4200000p'` over a decompressed feed if
gone. **Never hard-code a feed name** — take paths as arguments.

## Validate all four rules

### Rule 1 — structural payload anchor. Never ship a prefix list.

The prose is **not a closed set**: **3,084-5,846 distinct prefixes** per 200k
slice; collapsing digit runs only takes 3,419 → 2,743, and the top 12 cover
42-95% depending on feed. Shapes seen: `word :`, `-> word`, `word ->`,
`-> word :`, `&ident =`, a bare identifier with no separator, and composites that
embed payload fragments in the prose. **Any literal list is wrong on arrival.**

Anchor on the payload: it starts at the first **token run** — a
`key=value<delim>` pair followed by at least one more `=`. Linear scan, not a
regex:

```python
KEY_BYTES = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-")

def payload_start(body: bytes) -> int:
    """Offset of the payload's first token, or -1 when the line carries none."""
    for delimiter in (0x7C, 0x01):                    # '|' then SOH
        first = body.find(bytes([delimiter]))
        if first < 0:
            continue
        equals = body.rfind(b"=", 0, first)
        if equals < 0:
            continue
        start = equals
        while start > 0 and body[start - 1] in KEY_BYTES:
            start -= 1
        if start > 0 and body[start - 1] == 0x23:      # keep a leading '#'
            start -= 1
        if start == equals:                            # empty key, not a token
            continue
        if body.find(b"=", first + 1) < 0:             # need a second token
            continue
        return start
    return -1
```

Requiring a real delimiter matters: **27,920-48,930 lines per slice contain `=`
but no delimiter** and are prose, not payload.

**The invariant that catches all three regex defects at once: after stripping,
the head must contain no token run** — `payload_start(head) < 0`. Reproduce the
brief's result: **0 failures over ~800k lines across four slices.** Also
reproduce the **0.71 µs/line** throughput.

### Rule 2 — strip the trailing suffix

Content after the payload's last delimiter is a suffix, not a field:
**4.28-7.29% of payload lines** carry one, over **1-24 distinct families**,
dominated by a single `)`. Worth almost nothing in bytes
(**0.0015-0.0044% of body**) — take it for **dedup, not size**: some suffixes
carry variable data (an id, a name) that breaks byte-identity between two copies
of the same payload. It moves dedup **+0.16 to +1.23 points**. Payload is
`body[start : last_delimiter + 1]`.

### Rule 3 — direction. This one is correctness, not savings.

**Direction-blind dedup silently collapses an inbound message with its outbound
copy.** Consecutive pairs with identical payload but *opposite* direction, per
200k slice: **feed C 13,431 / 17,241, feed A 10,164, feed B 6,516.** Distinct
business events; they must not merge.

Two configurable keyword families, **leftmost match wins** — the verb of the log
statement, not a noun inside it. Composite prose matching both families is real
(327-948 lines per slice, over 3-434 distinct prefixes), which is exactly why
position decides:

```python
INBOUND = re.compile(rb"(?i)receiv|inbound|\bin\b|from|sroute|\bread\b")
OUTBOUND = re.compile(rb"(?i)send|outbound|\bout\b|push|writ|emit|publish")

def direction_of(head: bytes) -> str:
    into, out = INBOUND.search(head), OUTBOUND.search(head)
    if into and out:
        return "in" if into.start() < out.start() else "out"
    return "in" if into else "out" if out else "none"
```

### Rule 4 — consecutive-duplicate drop, and the cache that must not be built

Compare each row's `(direction, payload)` to the previous and skip when equal.
**One previous payload — do not build a per-thread cache.** Measured: per-thread
buys a few points on feed C and ~0.02 on A and B, against a dict holding one
payload per live thread with **~21,625 distinct threadnames in 200k feed-A lines**
(327,986 across a day). At ~4.5 KB per payload that is an unbounded growth term
of tens to hundreds of MB — in a brief whose entire point is peak RSS. If you
want the per-thread points anyway, the only acceptable shape is a bounded LRU
(N = 64-256) keyed on the payload's **16-byte hash**, not the payload, so
retained state is O(N × 16 B). Measure it; do not assume it wins.

## Reproduce this table

| feed / slice | prefix bytes | dup, direction-blind | **dup, direction-aware** | crossed pairs |
|---|---:|---:|---:|---:|
| C head 200k | 0.69% | 47.74% | **35.86%** | 13,431 |
| C mid 200k | 0.70% | 46.51% | **31.12%** | 17,241 |
| A head 200k | 0.90% | 12.81% | **3.93%** | 10,164 |
| B head 200k | 2.21% | 11.35% | **5.71%** | 6,516 |

Percentages are of total `body` bytes. **Direction-aware is the number W3·G
implements against**; the direction-blind column exists only to show the ~9-15
points a naive strip would wrongly collapse.

## Deliverable

A scratch script plus a measurement report under `g-prototype/`: the table above
reproduced on your slices, the `payload_start(head) < 0` failure count, the
µs/line throughput, and a go/no-go on each rule. **No `src/` edits, no flags, no
schema change** — W3·G does that, using your numbers.
