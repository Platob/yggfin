# Test data

Two synthetic captures, both in the layout the parser targets:

    <YYYY-MM-DD HH:MM:SS.mmm[_uuu]> [thread_name] [driver] (LEVEL) message

They are **synthetic**, written to exercise the shapes rather than to reproduce
any real capture. Tests derive their expectations from the files, then pin the
derived counts, so replacing either with a real anonymised sample needs no test
changes.

`app_sample.txt` is the parser's own fixture: four thread ids, one row whose
driver prints no level, and a four-line stack trace that must fold into the
`ERROR` row above it.

`app_messages_sample.txt` is the **message layer's** fixture, and every line of
it is there for one thing the layer has to get right:

- a line of prose with `key=value` noise in it, which is not a message;
- millis-only and comma-separated stamps beside micros ones, so one batch
  mixes the widths the slicing path assumes;
- a wire message with a log prefix *and* a suffix, pipe-separated;
- the same wire shape spelled `^A`, with a truncated `8=FIX4` no version
  answers for;
- and spelled with the SOH byte itself -- a literal `\x01`, written as bytes;
- a UL bridge message: `#`-marked keys, a whole group entry nested in one
  token behind a second separator, and a field no dictionary has;
- a rejection message, prose again, from the same driver as the bridge one --
  so a rule that classified by driver alone would get it wrong;
- an `ERROR` with a three-line stack trace that must fold into it;
- and a row whose driver prints no level, on a millis-only stamp.
