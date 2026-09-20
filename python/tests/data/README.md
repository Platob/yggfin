# Test data

`ulbridge.log` is the byte-exact, anonymized 144-line ULBridge capture the
native core keeps at `rust/tests/fix/ulbridge.log`, copied here so this
repository's tests read the same bytes its own acceptance example does. It
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
| stored lines | 141 | `logs.messages` is keyed on `bodyhash`, the digest of the whole line, and 3 lines repeat another byte for byte |
| messages the codec answers | 79 | a line can carry two frames and a line carrying none answers nothing |
| settled events | 53 | a message logged at several hops restates one event |
| `fix.bronze` rows | 53 | one row per event as parsed, keyed on `curruuid` |
| `fix.silver` rows | 53 | the same events walked: the walk restates them and adds none |

Over the parse's own rows the widest chain is `e7254b12:9f0316669a`: 35
messages, 21 events, with 6 as the last step the walk numbered. A chain is
named by the bridge's `msgsessionid:msgctxid`. `python/tests/test_fix.py`
holds every chain's counts; they are the same numbers
`cargo run --example fix_capture` prints in a core checkout.

The workflow integration test reads it through `Message.text_options()`,
writes `logs.messages`, parses that table into `fix.bronze` and walks it into
`fix.silver`. The Airflow operator tests run the same fixture through the
scheduled route.
