# FIX

`rekep.fix` parses wire or rendered FIX while preserving repeated fields and
unknown source data.

```python
from rekep.fix import FixMessage, FixRegistry

message = FixMessage.from_text("8=FIX.4.4|35=D|11=C1|55=IBM|10=001")
message.msg_type  # 'D'
message.get(55)  # 'IBM'
message.pairs  # ordered entries

registry = FixRegistry(offline=True)
registry.field("OrigClOrdID", "4.4")
```

The separator is detected from the message. SOH, pipe, caret forms, and
rendered `Name=Value` logs use the same ordered representation. Vectorized
Arrow helpers split whole columns and resolve distinct rendered names once.

## Registry

The reviewable `data/fix/` directory and `data/fix.zip` archive contain the
same versioned registry. It combines OnixS field definitions with QuickFIX
symbols and headers. Runtime parsing is offline by default.

The registry supplies:

- canonical name and numeric tag;
- FIX datatype and Arrow projection;
- description, valid values, and component/message usage;
- cross-version lookup using the latest known definition where needed.

Protocol-specific code should normalize values, not duplicate registry tables.

## Groups and components

Repeating groups remain ordered entries. Known components such as Parties are
extracted into typed lists; each Party retains an ordered string buffer for
fields absent from the current model. New registry components can use the same
generic extraction path.

## Parsed-log projection

Common fields are promoted once into `Log`. Residual `fix_tags`, `keyval`, and
`fix_miss_tags` keep everything not promoted. A later market conversion rebuilds
a `FixEvents` view from those typed columns and residual pairs instead of
tokenizing the raw message again.

## Benchmark

`python/benchmarks/bench_fix.py --quick` verifies and measures scalar versus
column parsing, tag projection, groups, and parsed-log reconstruction.
