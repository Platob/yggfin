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

## Version selection

Normal parsing has no default FIX version. Each message first inspects
`BeginString <8>` and, for FIXT, `ApplVerID <1128>` then
`DefaultApplVerID <1137>`. A known application version selects its prepared
registry index. If the evidence is absent, inconsistent, or unavailable, the
message remains valid raw FIX with ordered numeric tags; parsing never silently
chooses 4.2, 4.4, or the newest registry. Direct registry and codec callers may
still supply an explicit version.

## Registry

The reviewable `data/fix/` directory and `data/fix.zip` archive contain the
same versioned registry. It combines OnixS field definitions with QuickFIX
symbols and headers. Resource locations use the project's URL resolver and
`pyarrow.fs`; a remote resource is materialized only when a downstream parser
requires an OS-local path, then reused from the local cache. Resolution and
network work happen before the message loop.

The registry supplies:

- canonical name and numeric tag;
- FIX datatype and Arrow projection;
- description, valid values, and component/message usage;
- explicit-version and inferred-version lookup through cached indexes.

Protocol-specific code should normalize values, not duplicate registry tables.

### What the wheel carries

`data/fix.zip` is the whole dictionary and stays beside the repository. The
wheel ships `rekep/fix/registry.zip`, a projection of it holding the keys
`rekep.fix.publish.PROJECTED` names -- and every version's component
declarations, whole. A component says where a repeating group starts and ends,
so a projection that selected its members alongside the fields would end the
group somewhere else, and one that dropped them extracts no group at all.

`components()` answers `[]` twice over: for a version whose spec declares none
-- nothing before 4.3 has a component -- and for a store written before any
were kept. `components_available()` is what tells them apart, and the second
case warns rather than quietly extracting nothing.

Rebuild the projection after refreshing the dictionary:

```bash
cd python
uv run python -c "from rekep.fix.publish import publish_builtin; \
publish_builtin('../data/fix.zip', 'src/rekep/fix/registry.zip')"
```

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
