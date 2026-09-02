# Task prompts

Each file is **self-contained** — dispatch it to an agent as-is. `PLAN.md` (one
level up) has the dependency graph, the ownership matrix and the merge order;
`BRIEF.md` is the full reconstructed source brief.

| prompt | wave | items | may start |
|---|---|---|---|
| `W0-1-baseline-gate.md` | 0 | — | now |
| `W0-2-commit-path.md` | 0 | B5, F | now |
| `W0-3-dead-code.md` | 0 | I2 | now |
| `W0-4-row-primitive.md` | 0 | H dependency | now |
| `W0-5-g-prototype.md` | 0 | G dependency (no `src/` edits) | now |
| `W1-trunk-H-I1.md` | 1 | H, I1, deletes B3 + B4 | after W0·4, W0·3 merge |
| `W2-A-entries-types.md` | 2 | A | after W1 merges |
| `W2-B-column-widths.md` | 2 | B, C, D | after W1 merges |
| `W2-C-body-and-parse.md` | 2 | B2, E, E2 | after W1 merges (E after W2·A) |
| `W3-A2-comp-removal.md` | 3 | A2 | gated: A landed + explicit go |
| `W3-G-strip-direction.md` | 3 | G | after W0·5 + W1; needs carve-out sign-off |
| `MERGE-final.md` | — | integration, proof, report | after the above |

`00-STANDING-RULES.md` is inlined in every prompt; it is here as the single place
to edit if a rule changes.

**Five agents can start immediately** (all of wave 0). The trunk is W1 — nothing
else may touch `rekep/text/` while it is in flight, because H deletes B3 and B4
outright and anyone implementing them in parallel is writing code that gets
deleted on merge.
