# Test data

The synthetic captures use the parser's target layout:

```text
<YYYY-MM-DD HH:MM:SS.mmm[_uuu]> [thread_name] [driver] (LEVEL) message
```

Tests derive expectations from these files and then pin the resulting counts.
Replacing a fixture with an anonymized sample should therefore require no
hard-coded row edits.

- `app_sample.txt` covers thread identifiers, a missing level, and a four-line
  stack trace folded into its preceding `ERROR` row.
- `app_messages_sample.txt` covers millisecond and microsecond timestamps,
  surrounding noise, pipe, `^A`, and literal SOH separators, a truncated FIX
  version, indexed FIXML groups, unknown fields, prose from a bridge driver, a
  folded stack trace, and a row without a level.

The message fixture deliberately mixes shapes so tests exercise classification,
slicing, ordering, and headerless physical rows together.
