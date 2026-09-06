# Logs

Each task emits two INFO records through `rekep.logs.Stage`: start and finish.
The finish record and returned JSON share these keys:

```text
task read written skipped sources targets window elapsed_ms
```

Use `--log-level DEBUG` for per-source and per-file diagnostics. Batch-level
logging is deliberately absent; it would dominate large streams.

Command decoration and errors go to stderr. The machine-readable result alone
goes to stdout, so redirection remains safe:

```bash
rekep task run tasks/parse_messages/parse_messages.yml > result.json
```
