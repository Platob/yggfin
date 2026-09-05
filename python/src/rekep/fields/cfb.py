"""Ullink Bridge CBlock configurations (`.cfb`) as the fields they declare.

A `.cfb` is a FIX adapter's own dictionary: what it calls each tag, what type
it stores it as, which messages it places it in, and -- through the validity
regexps on those placements -- which values it accepts. Every one of those is
a fact about a *field*, so this reads the file as `Field`s and nothing else:
no record kind of its own, no tuple stage, no assembly afterwards.

The grammar drives. A `<vocabulary>` names and types the tags; the
`<grammar-binding>`s say where each is used, in what tree, with what values.
The unit of production is therefore a `<tag-constraint>` -- a declaration
that this tag is used here -- resolved through the vocabulary, which is an
index the walk consults and not a source of records. That ordering is what
lets a nested `<grammar>` become a repeating group in the same traversal, and
what puts the message type, the nesting and the enumeration on a field at
the moment it is built.

The same tag placed in twenty messages is yielded twenty times. That is the
file's own statement -- twenty readings of one identity -- and folding them
is the caller's `Field.merge`, the one merge there is.
"""

from __future__ import annotations

import dataclasses
import re
import string
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from xml.etree import ElementTree

from rekep.fields.field import Field
from rekep.fields.metadata import FixFieldValue, normalized_namespace

#: The root a bridge configuration has, and the only root this reads.
ROOT = "cplugin-configuration"

#: The one attribute of a constraint that matters here. It is called `name`
#: and it holds the tag *number* -- a fact worth stating, because a reader
#: who assumes `name` names something will look for a `tag` that is not there.
TAG_ATTRIBUTE = "name"

#: How a value produced here says where it came from.
VOCABULARY_SOURCE = "vocabulary"

#: What resolves a tag the file's own vocabulary does not declare: the
#: standard's name and datatype for it, or None where the standard has none.
#: A callable rather than a registry, so a file can be read where no store
#: exists and the parser never learns what a registry is.
StandardResolver = Callable[[int], "tuple[str, str] | None"]

# -- what the validity regexps enumerate --------------------------------------

#: `^(A|B|C)$`: an alternation of literal tokens. A token may carry a simple
#: backslash escape of one character (`\?`), which denotes that character;
#: any other regex syntax inside a token disqualifies the whole expression.
_ALTERNATION = re.compile(r"\^\((?P<body>[^()]+)\)\$")
_TOKEN = re.compile(r"^(?:[^()|\[\]{}*+?.^$\\]|\\[^A-Za-z0-9])+$")

#: `^[...]$`: a character class, anchored and unquantified. The body is read
#: by `_expanded_class`, which is where ranges are settled.
_CLASS = re.compile(r"\^\[(?P<body>[^\]]+)\]\$")

#: `^[C]( [C])*$`: a space-separated repeat of one class -- a
#: MultipleValueString, whose enumeration is the class and whose shape is
#: independent evidence that the field is multi-valued.
_REPEAT = re.compile(r"\^\[(?P<body>[^\]]+)\]\( \[(?P<again>[^\]]+)\]\)\*\$")

#: The alphabets a range may be expanded within. `A-z` spans punctuation and
#: is nobody's enumeration; a range is a run inside exactly one of these.
_ALPHABETS = (string.digits, string.ascii_uppercase, string.ascii_lowercase)


@dataclasses.dataclass
class CfbReport:
    """What one read of a configuration covered, and what it passed over.

    Skipping silently is not acceptable: the next reader has to know the
    coverage rather than infer completeness from an absence of errors. So
    every regexp is counted under the kind that accepted or refused it, every
    fallback to the standard is counted, and every vocabulary tag no grammar
    placed is counted -- on the iterator, not in a side table keyed by tag.
    """

    namespace: str = ""
    version: str = ""
    bindings: int = 0
    constraints: int = 0
    groups: int = 0
    vocabulary: int = 0
    vocabulary_only: int = 0
    resolved_by_standard: int = 0
    unresolved: int = 0
    malformed: int = 0
    empty_bindings: int = 0
    enumerated: Counter[str] = dataclasses.field(default_factory=Counter)
    skipped: Counter[str] = dataclasses.field(default_factory=Counter)

    @property
    def regexps(self) -> int:
        """Every validity regexp seen, enumerated or not."""
        return sum(self.enumerated.values()) + sum(self.skipped.values())


@dataclasses.dataclass(frozen=True)
class _Vocabulary:
    """One file's `<vocabulary>`, indexed by tag: the walk resolves against it."""

    name: str
    datatype: str
    description: str


class CfbFields:
    """The lazy iterator `Field.from_cfb` hands back, carrying its own report.

    A generator cannot carry an attribute, and the coverage a read produced is
    worth more than the fields alone to a reader deciding whether to trust
    them. So this is the iterator, and `report` is what it counted -- complete
    once the iterator is exhausted, and honest at every point before that.
    """

    def __init__(
        self,
        content: bytes,
        *,
        namespace: str,
        standard: StandardResolver | None,
    ) -> None:
        self.report = CfbReport(namespace=namespace)
        self._content = content
        self._standard = standard
        self._fields: Iterator[Field] | None = None

    def __iter__(self) -> Iterator[Field]:
        if self._fields is None:
            self._fields = self._walk()
        return self._fields

    def __next__(self) -> Field:
        return next(iter(self))

    # -- the walk ----------------------------------------------------------

    def _walk(self) -> Iterator[Field]:
        try:
            root = ElementTree.fromstring(self._content)
        except ElementTree.ParseError:
            # The one tolerance, and it is about I/O: a damaged file in a
            # directory scan must not lose the other eighty.
            return
        if root.tag != ROOT:
            raise ValueError(f"not a bridge configuration: the root is <{root.tag}>, not <{ROOT}>")
        report = self.report
        report.version = str(root.get("fix-version") or "").strip()
        if not report.version:
            raise ValueError("a bridge configuration states no fix-version")
        vocabulary = self._vocabulary(root)
        report.vocabulary = len(vocabulary)
        placed: set[int] = set()
        for binding in root.iterfind("grammar-binding"):
            report.bindings += 1
            # A direction word beside the type -- "j Inbound", "j Outbound" --
            # is one message type read twice, not two.
            msgtype = str(binding.get("type") or "").split()[0] if binding.get("type") else ""
            body = binding.find("grammar")
            if body is None:
                continue
            members: list[Field] = []
            yield from self._members(body, vocabulary, msgtype, placed, members)
            if not members:
                # `struct([])` is the shape of an unexpanded *reference*, and
                # a message with no members would read back as one.
                report.empty_bindings += 1
                continue
            if msgtype:
                yield self._message(msgtype, members)
        # A vocabulary tag no grammar places is still a field the bridge
        # declares -- and, for a counter, the only evidence of its group
        # (step 7). Yielded after the walk and marked as the vocabulary's,
        # so a reader can tell a used field from a merely declared one.
        for tag in vocabulary:  # document order: the index keeps it
            if tag in placed:
                continue
            report.vocabulary_only += 1
            entry = vocabulary[tag]
            yield self._field(tag, entry.name, entry.datatype, entry.description, VOCABULARY_SOURCE)

    def _members(
        self,
        grammar: ElementTree.Element,
        vocabulary: dict[int, _Vocabulary],
        msgtype: str,
        placed: set[int],
        direct: list[Field],
    ) -> Iterator[Field]:
        """Yield every field one `<grammar>` declares, at every depth.

        `direct` collects this grammar's own members in wire order -- what a
        message or a group is built from -- while everything constructed
        underneath is yielded as it is built, so a group three levels down is
        a field a caller sees, not only an entry inside an entry.

        Every deeper `<grammar>` is a repeating group whose *first*
        constraint is the counter -- positionally, never by name or type: 144
        counters in the corpus are typed `string`, 37 are not spelled `No*`,
        and a gate on either loses all of them.
        """
        report = self.report
        for child in grammar:
            if child.tag == "tag-constraint":
                report.constraints += 1
                declared = self._constrained(child, vocabulary, msgtype)
                if declared is None:
                    continue
                placed.add(declared.fix.tag)
                direct.append(declared)
                yield declared
            elif child.tag == "grammar":
                report.groups += 1
                inner: list[Field] = []
                yield from self._members(child, vocabulary, msgtype, placed, inner)
                if not inner:
                    continue
                counter, *entries = inner
                group = self._group(counter, entries)
                direct.append(group)
                yield group

    def _constrained(
        self,
        constraint: ElementTree.Element,
        vocabulary: dict[int, _Vocabulary],
        msgtype: str,
    ) -> Field | None:
        """One `<tag-constraint>` as the field it declares, or None unresolved."""
        report = self.report
        raw = constraint.get(TAG_ATTRIBUTE)
        if raw is None or not str(raw).strip().isdigit():
            report.malformed += 1
            return None
        tag = int(raw)
        entry = vocabulary.get(tag)
        if entry is None:
            # A constraint names a tag and nothing else -- no alt, no datatype
            # -- so a tag the file's vocabulary lacks cannot be resolved here.
            # The standard's reading of it is the fallback, counted; a name
            # is never synthesised.
            found = self._standard(tag) if self._standard is not None else None
            if found is None:
                report.unresolved += 1
                return None
            report.resolved_by_standard += 1
            entry = _Vocabulary(name=found[0], datatype=found[1], description="")
        values = tuple(self._values(constraint))
        return self._field(
            tag,
            entry.name,
            entry.datatype,
            entry.description,
            f"binding:{msgtype}",
            msgtype,
            values,
        )

    # -- building fields ---------------------------------------------------

    def _field(
        self,
        tag: int,
        name: str,
        datatype: str,
        description: str,
        source: str,
        msgtype: str = "",
        values: Sequence[FixFieldValue] = (),
    ) -> Field:
        """One complete field: name, Arrow type and metadata, at the moment it is built.

        `fix_field` decides the type once, from the Ullink word through the
        same table every other FIX reader uses, and the description's own
        "expressed in UTC" fixes the zone. The word itself stays in `fix.type`
        descriptively; it decides nothing downstream.
        """
        from rekep.fix.fields import fix_field

        # Typed through the table's own spelling -- `utc-timestamp` is filed
        # as `utctimestamp` -- and the Ullink word kept as it was written,
        # because it is descriptive and decides nothing downstream.
        built = fix_field(name, tag, _table_spelling(datatype), description=description or None)
        fix = built.fix
        # Stated, not left to fall back on the Arrow name: `fix.name` is what
        # a merge compares canonical spellings by, and a reading that leaves
        # it empty is one whose own name a fold can never tell from an alias.
        fix.name = name
        if datatype:
            fix.type = datatype
        fix.versions = (self.report.version,)
        fix["namespace"] = self.report.namespace
        fix.source = source
        fix.sources = (source,)
        if msgtype:
            fix.msgtypes = (msgtype,)
        if values:
            fix.enumerated = values
        return built

    def _group(self, counter: Field, entries: Sequence[Field]) -> Field:
        """A nested `<grammar>` as the group it declares: `list<Item>`.

        The counter is the first constraint and stays a field of its own; the
        group is named for it and built by the one naming rule the registry
        already has, so `NoHops` repeats a `Hop` here exactly as it does in the
        published dictionary.
        """
        from rekep.fix.quickfix import group_member

        tag = counter.fix.tag
        assert tag is not None  # a counter reached here through a constraint
        embedded = group_member(counter.fix.canonical, tag, [_member_of(one) for one in entries])
        # The standalone record is the store's own group shape: a block that is
        # a list, keyed by name. Its tag stays on the counter, a field in its
        # own right; carried here it would pair the two by tag on the way in
        # and dispute the counter's type.
        built = dataclasses.replace(embedded, metadata={"fix:component": counter.fix.canonical})
        fix = built.fix
        fix.name = counter.fix.canonical
        fix.versions = (self.report.version,)
        fix["namespace"] = self.report.namespace
        fix.source = counter.fix.source
        fix.sources = (counter.fix.source,)
        fix.msgtypes = counter.fix.msgtypes
        fix["counter"] = str(tag)
        return built

    def _message(self, msgtype: str, members: Sequence[Field]) -> Field:
        """A `<grammar-binding>` body as the message it declares: a struct."""
        from rekep.fix.quickfix import block

        built = block(msgtype, [_member_of(one) for one in members], msg_type=msgtype)
        fix = built.fix
        fix.name = msgtype
        fix.versions = (self.report.version,)
        fix["namespace"] = self.report.namespace
        fix.source = f"binding:{msgtype}"
        fix.sources = (f"binding:{msgtype}",)
        return built

    # -- reading the file --------------------------------------------------

    def _vocabulary(self, root: ElementTree.Element) -> dict[int, _Vocabulary]:
        """The file's vocabulary as `{tag: entry}`, built once.

        An entry with no `alt` falls back to the standard's own name for the
        tag -- never to a synthesised `Tag29`, which would become a canonical
        name, a shard filename and an alias nobody declared. Where the
        standard has none either, the entry is dropped and counted.
        """
        found: dict[int, _Vocabulary] = {}
        for element in root.iterfind("vocabulary/vocabulary-tag"):
            raw = element.get(TAG_ATTRIBUTE)
            if raw is None or not str(raw).strip().isdigit():
                continue
            tag = int(raw)
            name = str(element.get("alt") or "").strip()
            datatype = str(element.get("type") or "").strip()
            if not name:
                found_standard = self._standard(tag) if self._standard is not None else None
                if found_standard is None:
                    self.report.unresolved += 1
                    continue
                self.report.resolved_by_standard += 1
                name = found_standard[0]
                datatype = datatype or found_standard[1]
            description = element.findtext("description") or ""
            found[tag] = _Vocabulary(name=name, datatype=datatype, description=description.strip())
        return found

    def _values(self, constraint: ElementTree.Element) -> Iterator[FixFieldValue]:
        """The closed set a constraint's validity regexps enumerate, if any.

        Only what is unambiguously closed: an anchored alternation of
        literals, or an anchored character class with its ranges expanded.
        Everything else is a format, skipped and counted under its kind.
        """
        namespace = self.report.namespace
        for validity in constraint:
            regexp = validity.get("regexp")
            if regexp is None:
                continue
            expanded, kind = enumerated_values(regexp)
            if expanded is None:
                self.report.skipped[kind] += 1
                continue
            self.report.enumerated[kind] += 1
            for value in expanded:
                yield FixFieldValue(value=value, namespaces=(namespace,))


def enumerated_values(regexp: str) -> tuple[tuple[str, ...] | None, str]:
    """What one validity regexp enumerates, and the kind that decided it.

    `(values, kind)` where `values` is None for a regexp that is not a closed
    set. The kind names the rule that accepted it -- `alternation`, `class`,
    `repeat` -- or the reason it was refused, so a reader of the counts sees
    coverage by mechanism rather than one undifferentiated "skipped".
    """
    text = regexp.strip()
    if (matched := _ALTERNATION.fullmatch(text)) is not None:
        tokens = matched.group("body").split("|")
        if all(_TOKEN.fullmatch(token) for token in tokens):
            return tuple(dict.fromkeys(_unescaped(token) for token in tokens)), "alternation"
        return None, "alternation with regex syntax in a token"
    if (matched := _REPEAT.fullmatch(text)) is not None:
        if matched.group("body") != matched.group("again"):
            return None, "repeat of two different classes"
        expanded, reason = _expanded_class(matched.group("body"))
        return (expanded, "repeat") if expanded is not None else (None, reason)
    if (matched := _CLASS.fullmatch(text)) is not None:
        return _expanded_class(matched.group("body"))
    if text == ".*":
        return None, ".*"
    if not text.startswith("^") or not text.endswith("$"):
        return None, "unanchored"
    if re.search(r"\{\d|[*+?]", text):
        return None, "quantified format"
    return None, "other format"


def _member_of(declared: Field) -> Field:
    """A yielded field as the member a container carries: name, type and tag.

    The full reading -- versions, message types, values, provenance -- is
    yielded in its own right; inside a group or a message the member is the
    shape every tree in the dictionary has, so a bridge's `NoHops` compares
    with the standard's member for member rather than stamp for stamp.
    """
    fix = declared.fix
    # A group is placed in its parent by its counter's tag, as in every tree
    # the dictionary holds; the block marker belongs to the standalone record.
    tag = fix.tag if fix.tag is not None else fix.get("counter")
    metadata = {"fix:tag": str(tag)} if tag else {}
    return Field(name=declared.name, dtype=declared.dtype, nullable=True, metadata=metadata)


def _table_spelling(datatype: str) -> str:
    """An Ullink datatype word as `FIX_SCALARS` keys it: `utc-date` -> `utcdate`."""
    return datatype.strip().replace("-", "").lower()


def _unescaped(token: str) -> str:
    """A literal token with its simple backslash escapes resolved."""
    return re.sub(r"\\(.)", r"\1", token)


def _expanded_class(body: str) -> tuple[tuple[str, ...] | None, str]:
    """Every member of a character class, ranges expanded; None if refused.

    A range is expanded only within one alphabet -- the digits, the upper
    case, the lower case -- so `[A-z]` is refused rather than spelled out
    across the punctuation between them. An inverted range is refused. A
    trailing `-` is a literal member, not a range that lost its end.
    """
    if body.startswith("^"):
        # `[^0-9]` is everything *but* the digits: an open set, not a closed one.
        return None, "negated class"
    members: list[str] = []
    i = 0
    while i < len(body):
        char = body[i]
        if char == "\\":
            if i + 1 >= len(body) or body[i + 1].isalnum():
                # `\d`, `\w`, `\s`: a shorthand class, not one character.
                return None, "shorthand escape in class"
            members.append(body[i + 1])
            i += 2
            continue
        if i + 2 < len(body) and body[i + 1] == "-":
            low, high = char, body[i + 2]
            alphabet = next((one for one in _ALPHABETS if low in one and high in one), None)
            if alphabet is None:
                return None, "range across alphabets"
            if alphabet.index(low) > alphabet.index(high):
                return None, "inverted range"
            members.extend(alphabet[alphabet.index(low) : alphabet.index(high) + 1])
            i += 3
            continue
        members.append(char)
        i += 1
    expanded = tuple(dict.fromkeys(members))
    return (expanded, "class") if expanded else (None, "empty class")


def fields_of(
    content: bytes, *, namespace: str, standard: StandardResolver | None = None
) -> CfbFields:
    """The fields one configuration's bytes declare, lazily, under `namespace`."""
    return CfbFields(content, namespace=normalized_namespace(namespace), standard=standard)


__all__ = ["CfbFields", "CfbReport", "StandardResolver", "enumerated_values", "fields_of"]
