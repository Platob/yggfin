# Test data

`ulbridge.log` is the byte-exact, anonymized 111-line ULBridge capture from
Yggdryl pull request 91. It includes ordinary prose, numeric FIX, ULLINK,
FIXML, bridge configuration rows, printed separators, and raw control-byte
separators.

The workflow integration test reads it through `Message.text_options()`, writes
all 111 physical rows to `logs.messages`, reads that table through the native
FIX codec, and writes all 111 results to `fix.messages`. Its Git-blob SHA-256
is:

```text
6407fa90d4149be2f2e4047e22501bafedd71c5fe5fa3b34db5c77dbe9705074
```
