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
| stored lines | 141 | `logs.messages` is keyed on `currhashcode`, the code the read states over the whole line, and 3 lines repeat another byte for byte |
| messages the codec answers | 79 | a line can carry two frames and a line carrying none answers nothing |
| settled events | 53 | a message logged at several hops restates one event |
| `fix.bronze` rows | 53 | one row per event as parsed, keyed on `curruuid` |
| `fix.silver` rows | 53 | the same events walked: the walk restates them and adds none |

The widest chain is `e7254b12:9f0316669a`. The parse states it 37 times and
settles those statements on 23 rows; the walk moves two of them into
`e7254b12:9f03166699` and leaves 35 statements, 21 events and 6 as the last
step it numbered. A chain is named by the bridge's `msgsessionid:msgctxid`
where the row header states both, and by the first identifier the message
states where it does not -- one of this capture's eleven chains is named that
way. `python/tests/test_fix.py` holds every chain's walked counts, and the
144 lines and 79 messages are the numbers `cargo run --example fix_capture`
prints in a core checkout.

The pipeline task pages under `docs/pipeline/tasks/` show the chain the walk
names `e7254b12:9f03166699` -- rows 6 to 11, 15, 22, 35 and 36, a partial
fill and the fill that closed the order -- as each of the four tasks lands
it. `tools/pipeline_samples.py` renders those tables from a run over this
file into `docs/pipeline/tasks/samples/`, and `--check` renders them again
from a throwaway catalog and fails on any difference.

The workflow integration test reads it through `Message.text_options()`,
writes `logs.messages`, parses that table into `fix.bronze` and walks it into
`fix.silver`. The Airflow operator tests run the same fixture through the
scheduled route.
