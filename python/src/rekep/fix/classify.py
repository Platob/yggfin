"""Every key name a capture spells, against every name the dictionary knows.

Roughly half the key occurrences in a bridge capture match nothing the registry
has. That is not one problem, and treating it as one is how a backlog stops
being actionable. It is three:

- a **standard** field the dictionary has and the shipped projection left out,
  which is a key list to widen and no new code at all;
- a **near miss** of a name the dictionary has -- a case, separator or spelling
  variant -- which is an alias to record against the field it means, with the
  capture and the count that earned it, and never a silent merge;
- a **namespaced** name FIX never numbered, which is an entry to declare.

So this counts names and says which of the three each is. Names and counts
only: a value never leaves the batch it was counted in, and nothing here reads
or reports one.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fields.arrays import groups_of
from rekep.fix.entries import Alias, fold
from rekep.fix.fields import namespaced_field
from rekep.fix.message import rendered_keys
from rekep.fix.registry import FixRegistry, _levenshtein
from rekep.fix.rules import Rules

#: What a counted name turned out to be. Ordered by how much work it implies,
#: which is also the order a backlog reads best in.
EXACT = "exact"
ALIASED = "aliased"
NEAR = "near"
NAMESPACE = "namespace"
KINDS: tuple[str, ...] = (NAMESPACE, NEAR, ALIASED, EXACT)

#: How far a name may be from a known one and still be called a near miss.
#: A third of its length, capped -- the same shape `FixRegistry.search` uses --
#: because a two-character edit means something quite different in `Side` and
#: in `TrdRegTimestampOrigin`.
NEAR_CEILING = 3

#: A key that is only ever a number is a FIX tag, not a rendered name.
_IS_TAG = re.compile(r"^\d+$", re.ASCII)


@dataclasses.dataclass(frozen=True)
class KeyCount(Convertible):
    """One distinct rendered key name, and how it was written."""

    name: str
    #: How many tokens carried it, marked with the bridge's `#` and without.
    marked: int = 0
    bare: int = 0
    #: Which capture sources it was counted in.
    sources: tuple[str, ...] = ()

    @property
    def total(self) -> int:
        """Every occurrence, however it was written."""
        return self.marked + self.bare

    def into_dict(self) -> dict[str, Any]:
        """The count as a report holds it."""
        return {
            "name": self.name,
            "marked": self.marked,
            "bare": self.bare,
            "total": self.total,
            "sources": list(self.sources),
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> KeyCount:
        """Read one counted name back."""
        return cls(
            name=str(mapping["name"]),
            marked=int(mapping.get("marked", 0)),
            bare=int(mapping.get("bare", 0)),
            sources=tuple(str(source) for source in mapping.get("sources", ())),
        )

    def merged(self, other: KeyCount) -> KeyCount:
        """The same name counted in two places, as one count."""
        return dataclasses.replace(
            self,
            marked=self.marked + other.marked,
            bare=self.bare + other.bare,
            sources=tuple(dict.fromkeys((*self.sources, *other.sources))),
        )


@dataclasses.dataclass(eq=False)
class KeyCounts(Convertible):
    """Every distinct key name a stream spelled, accumulated batch by batch.

    A stream, because a capture is millions of lines and the whole point is to
    never hold one: each batch is counted in kernels and folded into a mapping
    whose size is the number of distinct *names*, which is a thousand or so.
    """

    #: The rules that say which lines carry a message and how to read one.
    rules: Rules = dataclasses.field(default_factory=Rules)

    #: `{name: count}`, keyed by the spelling the capture used. Exactly as it
    #: used it, because a report saying `OrderQty` where the line said
    #: `ORDER_QTY` is a report nobody can act on: which spelling to record as
    #: an alias is the question, and folding them together answers it early.
    counts: dict[str, KeyCount] = dataclasses.field(default_factory=dict)

    #: How many lines were read and how many carried a message at all.
    lines: int = 0
    messages: int = 0

    def add_messages(self, messages: Any, plugins: Any = None, source: str = "") -> KeyCounts:
        """Count one batch of raw log lines, and keep only what it spelled."""
        compute = pyarrow.compute
        if isinstance(messages, pyarrow.ChunkedArray):
            messages = messages.combine_chunks()
        if isinstance(plugins, pyarrow.ChunkedArray):
            plugins = plugins.combine_chunks()
        self.lines += len(messages)
        if not len(messages):
            return self
        protocols = self.rules.into_arrow_protocol_array(messages, plugins)
        for category, where in groups_of(protocols):
            protocol = category.as_py()
            if self.rules.rule(protocol).named is None:
                continue
            selected = compute.take(messages, where)
            self.messages += len(selected)
            self.add_keys(*rendered_keys(selected, named=self.rules.rule(protocol).named), source)
        return self

    def add_keys(self, markers: Any, keys: Any, source: str = "") -> KeyCounts:
        """Count one batch of already-tokenised keys."""
        compute = pyarrow.compute
        if not len(keys):
            return self
        marked = compute.equal(markers, "#")
        for wanted, attribute in ((True, "marked"), (False, "bare")):
            counted = compute.value_counts(compute.filter(keys, compute.equal(marked, wanted)))
            names = counted.field("values").to_pylist()
            totals = counted.field("counts").to_pylist()
            for name, total in zip(names, totals, strict=True):
                self._add(name, source, **{attribute: total})
        return self

    def _add(self, name: str, source: str, marked: int = 0, bare: int = 0) -> None:
        fresh = KeyCount(name=name, marked=marked, bare=bare, sources=(source,) if source else ())
        held = self.counts.get(name)
        self.counts[name] = fresh if held is None else held.merged(fresh)

    def merged(self, other: KeyCounts) -> KeyCounts:
        """Two streams' counts as one, for a run split across sources."""
        counts = dict(self.counts)
        for key, count in other.counts.items():
            held = counts.get(key)
            counts[key] = count if held is None else held.merged(count)
        return KeyCounts(
            rules=self.rules,
            counts=counts,
            lines=self.lines + other.lines,
            messages=self.messages + other.messages,
        )

    def into_dict(self) -> dict[str, Any]:
        """The counts as a document, biggest first."""
        return {
            "lines": self.lines,
            "messages": self.messages,
            "keys": [count.into_dict() for count in self.ordered()],
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> KeyCounts:
        """Read counts back, so a run can be resumed or two of them merged."""
        counted = cls(lines=int(mapping.get("lines", 0)), messages=int(mapping.get("messages", 0)))
        for stored in mapping.get("keys", ()):
            count = KeyCount.from_dict(stored)
            counted.counts[count.name] = count
        return counted

    def ordered(self) -> list[KeyCount]:
        """Every counted name, most occurrences first and ties by name."""
        return sorted(self.counts.values(), key=lambda count: (-count.total, count.name))


@dataclasses.dataclass(frozen=True)
class Classified(Convertible):
    """One counted name, and what the dictionary makes of it."""

    count: KeyCount
    kind: str
    #: The field it resolves to, or the nearest one when it is a near miss.
    resolved: str = ""
    #: Edit distance to `resolved`; zero unless this is a near miss.
    distance: int = 0

    @property
    def name(self) -> str:
        """The name as the capture spelled it."""
        return self.count.name

    def into_dict(self) -> dict[str, Any]:
        """One row of the backlog."""
        found = {**self.count.into_dict(), "kind": self.kind}
        if self.resolved:
            found["resolved"] = self.resolved
        if self.distance:
            found["distance"] = self.distance
        return found

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Classified:
        """Read one backlog row back."""
        return cls(
            count=KeyCount.from_dict(mapping),
            kind=str(mapping["kind"]),
            resolved=str(mapping.get("resolved", "")),
            distance=int(mapping.get("distance", 0)),
        )

    def into_alias(self) -> Alias:
        """This near miss as the alias it is a candidate for."""
        return Alias(
            name=self.name,
            source=", ".join(self.count.sources),
            occurrences=self.count.total,
        )

    def into_entry(self, column: str = "") -> Field:
        """This namespaced name as the record declaring it would be.

        `column` names the parsed-log column the field is lifted into, for a
        caller that already knows it; the empty default leaves the record in
        the pairs, completable later through `FixRegistry.promote_field`.
        """
        return namespaced_field(self.name, "String", column=column)


@dataclasses.dataclass(frozen=True)
class KeyReport(Convertible):
    """A whole run: what was counted, what it was, and what to do about it."""

    rows: tuple[Classified, ...] = ()
    lines: int = 0
    messages: int = 0

    def of(self, kind: str) -> tuple[Classified, ...]:
        """Every row of one kind, most occurrences first."""
        return tuple(row for row in self.rows if row.kind == kind)

    def totals(self) -> dict[str, int]:
        """`{kind: occurrences}` -- which of the three problems is the big one."""
        found = dict.fromkeys(KINDS, 0)
        for row in self.rows:
            found[row.kind] += row.count.total
        return found

    def names(self) -> dict[str, int]:
        """`{kind: distinct names}`, which is the size of the work rather than
        the size of the traffic."""
        found = dict.fromkeys(KINDS, 0)
        for row in self.rows:
            found[row.kind] += 1
        return found

    def into_dict(self) -> dict[str, Any]:
        """The report as the document a backlog is reviewed from."""
        return {
            "lines": self.lines,
            "messages": self.messages,
            "totals": self.totals(),
            "names": self.names(),
            "keys": [row.into_dict() for row in self.rows],
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> KeyReport:
        """Read a report back, so a run and the change it drives are separable."""
        return cls(
            rows=tuple(Classified.from_dict(row) for row in mapping.get("keys", ())),
            lines=int(mapping.get("lines", 0)),
            messages=int(mapping.get("messages", 0)),
        )


def classify(counts: KeyCounts, registry: FixRegistry, ceiling: int = NEAR_CEILING) -> KeyReport:
    """Every counted name against the dictionary, most counted first.

    The walk is over distinct names -- a thousand or so -- and not over rows,
    which is why it is written as one.
    """
    known = _known_names(registry)
    containers = _known_containers(registry)
    rows = [
        _classified(count, registry, known, containers, ceiling)
        for count in counts.ordered()
        # A bare number is a FIX tag on the wire, not a rendered name, and
        # whether the registry has it is a different question.
        if not _IS_TAG.match(count.name.strip())
    ]
    return KeyReport(rows=tuple(rows), lines=counts.lines, messages=counts.messages)


def _classified(
    count: KeyCount,
    registry: FixRegistry,
    known: Mapping[str, str],
    containers: frozenset[str],
    ceiling: int,
) -> Classified:
    """What one counted name is: known, aliased, nearly known, or nobody's."""
    record = registry.resolve(count.name)
    if record is not None:
        aliased = fold(count.name) in {alias.folded for alias in record.fix.named_aliases}
        return Classified(count, ALIASED if aliased else EXACT, record.fix.canonical)
    wanted = _member_name(count.name, containers)
    if wanted != count.name:
        record = registry.resolve(wanted)
        if record is not None:
            return Classified(count, EXACT, record.fix.canonical)
    nearest, distance = _nearest(fold(wanted), known, ceiling)
    if nearest:
        return Classified(count, NEAR, nearest, distance)
    return Classified(count, NAMESPACE)


def _member_name(name: str, containers: frozenset[str]) -> str:
    """The field a dotted key names, when the dots are a path and not a namespace.

    `NoPartyIDs.partyid` is `PartyID` sitting inside a group the dictionary
    knows, so the tail is the field. `TECH.CLIENTID` is a vendor's own
    namespace, and reading its tail as `ClientID <109>` would file an enrichment
    enrichment field under a standard tag it has nothing to do with. What
    tells them apart is whether the segments in front name anything -- a
    component, a group, a field -- that this dictionary has.
    """
    head, _, tail = name.rpartition(".")
    if not head or not tail:
        return name
    return tail if all(fold(part) in containers for part in head.split(".") if part) else name


def _known_containers(registry: FixRegistry) -> frozenset[str]:
    """Every folded name a dotted key may sit *inside*: a component or a field.

    A field as well as a component, because a group is named by its count
    field -- `NoPartyIDs` -- and that is what a rendered path writes.
    """
    found = {fold(name) for name in registry.component_records()}
    for record in registry.field_records().values():
        found.update(fold(spelling) for spelling in record.fix.spellings())
    return frozenset(found)


def _known_names(registry: FixRegistry) -> Mapping[str, str]:
    """`{folded spelling: canonical name}` for every name the dictionary has."""
    found: dict[str, str] = {}
    for record in registry.field_records().values():
        for spelling in record.fix.spellings():
            found.setdefault(fold(spelling), record.fix.canonical)
    return found


def _nearest(wanted: str, known: Mapping[str, str], ceiling: int) -> tuple[str, int]:
    """The closest known name within a length-scaled ceiling, or nothing.

    Scaled, because two edits in `Side` is a different word and two edits in
    `TrdRegTimestampOrigin` is a typo.
    """
    allowed = min(ceiling, max(1, len(wanted) // 3))
    best, distance = "", allowed + 1
    for folded, name in known.items():
        found = _levenshtein(wanted, folded, allowed)
        if found is not None and (found < distance or (found == distance and name < best)):
            best, distance = name, found
    return (best, distance) if best else ("", 0)


# -- turning a report into changes -------------------------------------------


def apply_report(
    registry: FixRegistry,
    report: KeyReport,
    *,
    aliases: bool = False,
    namespace: bool = False,
    minimum: int = 0,
) -> list[str]:
    """Register what the report found, through the registry's own verbs.

    Nothing is applied by default, and a near miss is never applied without
    being asked for: a case variant of a known name is *evidence* that the two
    are one field, not proof, and the whole reason the classification separates
    them is so somebody decides. What this does is turn the decision into one
    command instead of a thousand file edits.
    """
    applied = []
    for row in report.of(NEAR) if aliases else ():
        if row.count.total < minimum or not row.resolved:
            continue
        registry.alias_field(row.resolved, row.into_alias())
        applied.append(f"alias {row.name} -> {row.resolved} ({row.count.total} occurrences)")
    for row in report.of(NAMESPACE) if namespace else ():
        if row.count.total < minimum:
            continue
        registry.add_field(row.into_entry())
        applied.append(f"field {row.name} ({row.count.total} occurrences)")
    return applied


# -- streaming a capture -----------------------------------------------------


def count_reader(
    reader: Any,
    counts: KeyCounts | None = None,
    *,
    source: str = "",
    plugins: str | None = None,
) -> KeyCounts:
    """Count every key name a `RecordBatchReader` of parsed lines spelled.

    One batch at a time, and only the counts are kept: the batch that carried
    a value is released before the next one is read.

    `plugins` is a regular expression the line's `plugincode` must match --
    `^UL` for a bridge's own traffic -- so a report can be about the plugins
    that matter rather than about a whole estate.
    """
    counted = counts if counts is not None else KeyCounts()
    pattern = re.compile(plugins, re.ASCII) if plugins else None
    for batch in _batches(reader):
        messages, named = _columns(batch)
        if pattern is not None and named is not None:
            keep = pyarrow.compute.fill_null(
                pyarrow.compute.match_substring_regex(named, pattern.pattern), False
            )
            messages = pyarrow.compute.filter(messages, keep)
            named = pyarrow.compute.filter(named, keep)
        counted.add_messages(messages, named, source)
    return counted


def _batches(reader: Any) -> Iterator[pyarrow.RecordBatch]:
    """Whatever was handed over, as batches: a reader, a table, or batches."""
    if isinstance(reader, pyarrow.RecordBatch):
        yield reader
        return
    if isinstance(reader, pyarrow.Table):
        yield from reader.to_batches()
        return
    if isinstance(reader, pyarrow.RecordBatchReader):
        yield from reader
        return
    if isinstance(reader, Iterable):
        for batch in reader:
            yield from _batches(batch)
        return
    raise TypeError(f"cannot read record batches from {type(reader).__name__}")


def _columns(batch: pyarrow.RecordBatch) -> tuple[Any, Any]:
    """The message column and the plugin column, by the names a `FixMsg` uses."""
    names = batch.schema.names
    if "message" not in names:
        raise ValueError(f"a batch of log lines needs a 'message' column; got {names}")
    return batch.column("message"), (batch.column("plugincode") if "plugincode" in names else None)


def count_files(
    source: str,
    counts: KeyCounts | None = None,
    *,
    pattern: str = "*",
    recursive: bool = True,
    plugins: str | None = None,
    batch_row_size: int = 65536,
    limit: int | None = None,
) -> KeyCounts:
    """Count every key name under one folder or file, one batch at a time."""
    from rekep.text.text_files import TextFiles

    files = TextFiles.from_folder(source, pattern=pattern, recursive=recursive)
    counted = counts if counts is not None else KeyCounts()
    for opened in files.into_files():
        with opened:
            for batch in opened.into_arrow_batches(batch_row_size=batch_row_size):
                counted = count_reader(
                    batch, counted, source=_source_of(opened.url), plugins=plugins
                )
                if limit is not None and counted.lines >= limit:
                    return counted
    return counted


def _source_of(url: str) -> str:
    """A capture's own name, which is what a backlog says a count came from."""
    return url.rstrip("/").rsplit("/", 1)[-1]
