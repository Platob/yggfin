# Test data

`ulbridge.log` is the byte-exact, anonymized 144-line ULBridge capture copied
from the native core, so this repository's tests read the same bytes its own
acceptance example does. The digest below is what pins them. It
holds ordinary prose, numeric FIX, ULLINK, FIXML, bridge configuration rows,
printed separators, and control-byte separators, and it logs several
messages again at every hop they passed.

Its content SHA-256, as `sha256sum` prints it, is:

```text
4cc928ade1c8975701e4ab3908d5aa2fb53f8c919eb3ffc8faef10102cb6a94e
```

## What it answers

| reading | count | why |
| --- | ---: | --- |
| physical lines | 144 | one row of the text read each |
| stored lines | 144 | `logs.messages` is keyed on `curruuid`, the UUIDv7 the native read packs from the line's instant and its content code -- which digests the object it was read from, its header's captures but the clock, its row number and its body -- so the 3 exact repeats answer 3 identities and no line is lost |
| lines the row header does not date | 0 | the shipped header reads every fraction this bridge writes, the fifteen grouped micros included |
| messages the codec answers | 79 | a line can carry two frames and a line carrying none answers nothing |
| duplicate frame arrivals | 30 | the same message is logged again at several hops |
| `fix.raw` rows | 49 | one row per distinct parsed event, keyed on `curruuid` |
| `fix.refined` rows | 19 | the 49 `fix.raw` rows walked, those of one event merged into one row; every line dated means every observation carries the session, context and sequence the fold merges on |

`python/tests/test_fix.py` holds every chain's walked counts. Capture
session and context form the byte-length-prefixed `msgsesseventid` identifier,
which never replaces
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
writes `logs.messages`, parses that table into `fix.raw` and walks it into
`fix.refined`. The Airflow operator tests run the same fixture through the
scheduled route.
