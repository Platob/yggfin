# Test data

The synthetic captures use the parser's target layout:

```text
<YYYY-MM-DD HH:MM:SS.mmm[_uuu]> [thread_name] [driver] (LEVEL) message
```

`app_messages_sample.txt` covers millisecond and microsecond timestamps,
surrounding noise, delimiter-like payload text, a folded stack trace, and a row
without a level. Tests derive expectations from it and pin the resulting row
counts.
