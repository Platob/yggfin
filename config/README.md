# Local configuration

`fix/` is the native Yggdryl FIX dictionary this checkout parses against: the
canonical `primitive/` and `nested/` JSON shards `FixRegistry.write_into`
emits, read back by `FixRegistry.from_handle`. It is the default `registry` of
`parse_fix`, and it is what types every FIX column in `fix.messages`.

It is configuration, not a schema contract: `schemas/` publishes the two table
shapes, and this directory publishes the dictionary one of them is generated
from. Point either task's `registry` parameter at another location - a
directory, or an `s3://` URI - to parse against a different dictionary.
