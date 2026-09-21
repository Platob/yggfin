# Test data

`ulbridge.log` is the byte-exact, anonymized 144-line ULBridge capture copied
from the native core, so this repository's tests read the same bytes its own
acceptance example does. The digest below is what pins them. It
holds ordinary prose, numeric FIX, ULLINK, FIXML, bridge configuration rows,
printed separators, and raw control-byte separators, and it logs several
messages again at every hop they passed.

Its content SHA-256, as `sha256sum` prints it, is:

```text
4cc928ade1c8975701e4ab3908d5aa2fb53f8c919eb3ffc8faef10102cb6a94e
```

## What it answers

| reading | count | why |
| --- | ---: | --- |
| physical lines | 144 | one row of the text read each |
| stored lines | 141 | `logs.messages` is keyed on `curruuid`; native 0.1.8 gives 3 exact repeats the same legacy UUID, while the next native identity includes source and physical sequence |
| lines the row header does not date | 15 | they settle at the epoch pin in `currunix`, and each carries a content code of its own, so all 15 land |
| messages the codec answers | 79 | a line can carry two frames and a line carrying none answers nothing |
| duplicate frame arrivals | 28 | the same message is logged again at several hops |
| `fix.bronze` rows | 51 | one row per distinct parsed event, keyed on `curruuid` |
| `fix.silver` rows | 52 | the 51 source events walked, plus one expiry at `2026-08-14T16:25:00Z` |

`python/tests/test_fix.py` holds every chain's walked counts. Capture
`session:context` is retained as the `msgsectxid` identifier and never replaces
the business identifier that names a chain. The 144 lines and 79 frame
arrivals are the numbers `cargo run --example fix_capture` prints in a core
checkout.

The pipeline task pages under `docs/pipeline/tasks/` show the chain the walk
names `00026877711XOEA0`, including the partial fill and the fill that closed
the order, as each of the four tasks lands it. `tools/pipeline_samples.py`
renders those tables from a run over this file into
`docs/pipeline/tasks/samples/`, and `--check` renders them again
from a throwaway catalog and fails on any difference.

The workflow integration test reads it through `Message.text_options()`,
writes `logs.messages`, parses that table into `fix.bronze` and walks it into
`fix.silver`. The Airflow operator tests run the same fixture through the
scheduled route.
