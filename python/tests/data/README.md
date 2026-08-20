# Test data

`app_sample.txt` follows the layout the parser targets:

    <YYYY-MM-DD HH:MM:SS.mmm_uuu> [thread_name] [driver] (LEVEL) message

It is **synthetic**, written to exercise the shape rather than reproduce any
real capture: four thread ids, one row whose driver prints no level, and a
four-line stack trace that must fold into the `ERROR` row above it.

Tests derive their expectations from this file, so replacing it with a real
anonymized sample needs no test changes.

`fix_sample.txt` follows the same header layout, but its `message` is
pipe-delimited `key=value` pairs (`8=FIX.4.4|9=112|35=D|...`), for
`rekep.jobs.LogsToRecords`: one row opens with a plain `8=`, one with a
`#`-prefixed `#8=` (the strip case), one is not key/value shaped at all
(the "leave `fields` empty" case), and one carries a different protocol
version (`8=FIX.4.2`) so a batch is never all one protocol.
