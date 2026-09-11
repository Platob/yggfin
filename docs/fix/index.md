# FIX

rekep ships one FIX system: a complete registry, a line codec, `FixMsg`, and a
fixed Arrow projection. It reads numeric FIX, ULLINK name/value rows, bridge
configuration JSON, FIXML, and already-split pairs through one builder.

| page | answers |
| --- | --- |
| [Registry](registry.md) | which tag, name, alias, branch, datatype, code set, and group does a key mean? |
| [Decode](decode.md) | how does a log line become `Message`, `FixMsg`, and `fix.messages`? |
| [Capture columns](capture.md) | what does a capture's own column do to the row it leads? |
| [Encode](encode.md) | how is the lossless arrival record emitted again? |
| [Quality](quality.md) | what survives malformed input, replay, and registry change? |
| [Registry browser](../tools/fix-registry.md) | how do I search and inspect all 6,883 loaded definitions? |

## Default registry

The package contains 6,241 specification fields, 181 messages, 747 components
and 580 repeating groups. Importing `rekep` loads them, adds 20 runtime fields
and 42 ULBridge fields, and installs the result as the process default.

```python
from rekep.fix import fix_registry, global_registry, registry_path

registry = fix_registry()

assert registry_path().is_dir()
assert len(registry) == 6303
assert global_registry() == registry
```

An application does not need a registry environment variable or an external
dictionary directory. An explicit registry URI remains available for testing
or a venue extension.

## One line, one typed message

```python
from rekep.fix import fix_codec

message, = fix_codec().parse_line(
    b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
)

assert message.field.name == "executionreport"
assert message.by_tag(35).as_py() == "8"
assert message.by_name("side").as_py() == "1"
assert message.by_name("lastqty").as_py() == 235.0
```

The registry resolved aliases and code names; the message still retains the
original spellings in `entries()`. Every parse door answers a lazy stream, so
`message, =` here is unpacking the one message this line held — a bulk bridge
configuration yields one per MBean.

## Stream contract

`FixCodec.parse_text_arrow_reader` exposes its output schema before reading its
first batch, decided from the source schema and the dictionary alone. Capture
columns lead the fixed projection unless a fixed field claims the same folded
name, in which case they fill it instead. One input row produces one output
row, including prose and rows with values that fail conversion.
