# Read bridge configurations

`Field.from_cfb` reads an Ullink Bridge CBlock configuration (`.cfb`) as the
fields it declares: one `Field` per placement of a tag in a message, typed
from the file's vocabulary, carrying the message type, the nesting and the
values its validity regexp enumerates. Nothing is folded on the way out. A
tag placed in three messages is three readings, and `Field.merge` is the one
merge there is:

```python
from rekep.fields import Field

read = Field.from_cfb("python/tests/fields/fixtures/cfb/FX_Quoting_SellSide.cfb")
side = [one for one in read if one.fix.tag == 54]

print([one.fix.msgtypes for one in side])
print([value.value for value in side[0].fix.enumerated])
print(side[0].merge(side[1]).merge(side[2]).fix.msgtypes)

report = read.report
print(report.bindings, report.constraints, report.groups, report.vocabulary_only, report.unresolved)
print(dict(report.enumerated), dict(report.skipped))
```

```text
[('D',), ('j',), ('j',)]
['1', '2']
('j', 'D')
3 23 2 2 3
{'class': 6, 'alternation': 3, 'repeat': 1} {'.*': 4, 'quantified format': 1, 'unanchored': 1}
```

The iterator is lazy and carries its own `report`, complete once it is
exhausted: what the read covered and, by name, what it passed over. Nothing
is skipped silently.

## Namespace

The namespace is the file's stem, normalised (`FX_Quoting_SellSide.cfb` reads
as `fx-quoting-sellside`), unless `namespace=` says otherwise. Every value a
regexp enumerates carries it, so a venue's meaning of `54=1` never
masquerades as the standard's when the readings reach a registry.

## What the vocabulary does not declare

A placed tag the file's `<vocabulary>` does not declare is resolved through
`standard=`, a callable from tag to `(name, datatype)` or `None`. Without
one, such tags are counted under `report.unresolved` and left out. A
vocabulary tag no grammar places is still yielded, after the walk and in
document order, with `fix.source == "vocabulary"`; for a `No*` counter that
is often the only evidence of its group.

## What a regexp enumerates

Three forms are read as enumerations: an alternation of literal tokens
(`^(1|2|G)$`), a character class (`^[1-9A-J]$`), and a space-separated
repeat of one class (`^[1-9]( [1-9])*$`, a MultipleValueString). Ranges
expand inside one alphabet only. Everything else is refused under a kind in
`report.skipped`: `.*`, an unanchored or quantified format, a negated class,
a shorthand escape such as `\d`, a range across alphabets, an inverted
range, or regex syntax inside a token. `report.regexps` is the total either
way.

## Groups and messages

A nested `<grammar>` is a repeating group: `list<Hop: struct<...>>`, named
for its counter by the registry's own naming rule, with the counter kept as
a field in its own right. Each `<grammar-binding>` is a message block named
by its type code; a direction word beside the code (`j Inbound`) is one type
read twice, and a binding with no members declares no message. Inside a
tree a member is the dictionary's member shape, name, type and tag, so a
bridge's `NoHops` compares with the standard's member for member.

## Into a registry

Fold readings before storing them. Two bindings of one message type are one
declaration, and a registry handed both sees two shapes of one name and
disputes them; handed the fold, it stores the union. `record_key` is the
key the store itself uses, the tag where there is one and the name where
there is none:

```python
from rekep.fix import FixRegistry
from rekep.fix.entries import record_key

read = Field.from_cfb("python/tests/fields/fixtures/cfb/FX_Quoting_SellSide.cfb")
folded = {}
for one in read:
    key = record_key(one)
    folded[key] = folded[key].merge(one) if key in folded else one
FixRegistry(cache_dir="bridges").add_fields(list(folded.values()), read.report.namespace)
```

The group record carries no tag of its own -- the tag stays on the counter,
a field in its own right -- so the two are stored side by side.

## Damaged files

A file that does not parse yields nothing, so a directory scan keeps the
other eighty. A root other than `<cplugin-configuration>`, or a missing
`fix-version`, raises: those are not damage but a different kind of file.

See [Fields](fields.md) for how readings fold in the registry, and
[Registry](registry.md) for `add_fields(..., namespace=)`.
