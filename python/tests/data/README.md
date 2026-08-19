# Test data

`app_sample.txt` follows the layout the parser targets:

    <YYYY-MM-DD HH:MM:SS.mmm_uuu> [thread_name] [driver] (LEVEL) message

It is **synthetic**, written to exercise the shape rather than reproduce any
real capture: four thread ids, one row whose driver prints no level, and a
four-line stack trace that must fold into the `ERROR` row above it.

Tests derive their expectations from this file, so replacing it with a real
anonymized sample needs no test changes.
