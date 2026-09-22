# FIX

rekep ships one FIX system: a complete registry, a line codec, `FixMsg`, and a
fixed Arrow projection. It reads numeric FIX, ULLINK name/value rows, bridge
configuration JSON, FIXML, and already-split pairs through one builder. The
capture pipeline is two stages over that one codec, parse into `fix.raw`
and lifecycle into `fix.refined`, behind a line door and a batch door each.

| page | answers |
| --- | --- |
| [Registry](registry.md) | which tag, name, alias, dialect, datatype, code set, and group does a key mean? |
| [Decode](decode.md) | how does a log line become `Message`, `FixMsg`, `fix.raw` and `fix.refined`? |
| [Encode](encode.md) | how is a canonical frame rebuilt from lifted fields and residual entries? |
| [Quality](quality.md) | what survives malformed input, replay, and registry change? |
| [Registry browser](../tools/fix-registry.md) | how do I search 7,781 definitions and 736 code sets? |

## Default registry

The package ships the dictionary as JSON shards. Importing `rekep` loads them
over the crate's own 32 definitions -- every registry holds those and two
standard definitions from construction, so a bare one holds 34 -- and installs the
result as the process default.

```python
from rekep.fix import fix_crate_fields, fix_registry, global_registry, registry_path

registry = fix_registry()

assert registry_path().is_dir()
assert len(registry) == 7781
assert len(fix_crate_fields()) == 32
assert global_registry() == registry
```

An application does not need a registry environment variable or an external
dictionary directory. An explicit registry URI remains available for testing
or a venue extension.

## One line, one typed message

```python
from rekep.fix import fix_codec, fix_registry

codec = fix_codec(fix_registry())
message = next(
    iter(
        codec.parse_line(
            b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
        )
    )
)

assert message.by_tag(35).as_py() == "8"
assert message.by_name("side").as_py() == "BUY"
assert message.by_name("lastqty").as_py() == 235.0
# The event's own ladder, exact: what it last traded is what it is about.
assert float(message.px.as_py()) == 72.28
assert float(message.qty.as_py()) == 235.0
```

`parse_line` answers a message per frame the line carried -- one here. The
registry resolved the dictionary's spellings and centrally stored
`FIX:codeset` names. Code-suffix columns use their native classes, including
`ISINCode`, `CFICode`, `CUSIPCode`, `SEDOLCode`, `BloombergCode`, `FIGICode`,
and `MICCode`.

## Stream contract

`FixCodec.parse_text_arrow_reader` exposes its output schema before reading its
first batch.
Source columns lead the fixed projection unless a fixed field claims the same
folded name. One input row produces one row per message it carried: a line
carrying two frames answers two, and a line carrying none -- log prose -- answers
none. A value that fails conversion stays null beside the pair that arrived.

Parsing and local enrichment are independent per event. `threads` defaults to
the available CPU count and zero becomes one; completed batches are yielded in
input order. Lifecycle enrichment remains ordered and stateful. `snapshot_ns`
defaults to zero, while a positive value emits owned views of live events on
that nanosecond grid.

The default null markers are empty text, `null`, `<null>`, `none`, `n/a`, and
`[n/a]`, with ASCII whitespace and case ignored. `crosscode` uses the first
available `OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID`, or
`MDReqID`. Message type, capture session, context and sequence instead form
`identifiers["msgsesseventid"]`, each text part byte-length-prefixed, when all
four exist.
