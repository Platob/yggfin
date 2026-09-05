"""`Field.from_cfb`: an Ullink bridge configuration as the fields it declares.

Every number here is derived from `fixtures/cfb/`, whose README says what each
file is for, and pinned -- so a parser that quietly lost a construct fails
against the fixture rather than against a figure typed in beside it.

Field-level throughout: no store on disk and no registry constructed, except
in the two tests that are *about* ingestion.
"""

from __future__ import annotations

import collections
import re
from pathlib import Path

import pyarrow
import pytest

import rekep
from rekep.fields import Field
from rekep.fields.cfb import CfbFields, enumerated_values

FIXTURES = Path(__file__).parent / "fixtures" / "cfb"
SELL_SIDE = FIXTURES / "FX_Quoting_SellSide.cfb"
BUY_SIDE = FIXTURES / "Rates_BuySide.cfb"

#: What the standard says about the two tags the sell-side file cannot name
#: itself: 29 has no `alt`, and 9999 is constrained without being declared.
STANDARD = {29: ("NoLinesOfText", "integer"), 9999: ("FakeVendorFlag", "string")}


def _fields(path: Path = SELL_SIDE, **kwargs: object) -> tuple[list[Field], CfbFields]:
    parsed = Field.from_cfb(path, **kwargs)
    return list(parsed), parsed


def _scalars(fields: list[Field]) -> dict[int, list[Field]]:
    """Every scalar reading by tag: the groups and messages left aside."""
    found: dict[int, list[Field]] = collections.defaultdict(list)
    for one in fields:
        if one.fix.tag is not None and not pyarrow.types.is_list(one.dtype):
            found[one.fix.tag].append(one)
    return found


# -- field-level, no registry --------------------------------------------------


def test_every_yielded_object_is_a_complete_field_with_its_arrow_type_set() -> None:
    """The whole point of the entry point living on `Field`.

    Nothing is a tuple, a record kind or an assembly step: each thing the
    iterator hands over is a `Field` with its Arrow type decided, and no
    `FixRegistry` was built to get it.
    """
    fields, parsed = _fields(standard=STANDARD.get)

    assert all(isinstance(one, Field) for one in fields)
    assert all(one.dtype is not None for one in fields)
    assert all(one.nullable for one in fields), "never nullable=False, whatever a binding required"
    assert parsed.report.namespace == "fx-quoting-sellside", "the stem, normalised"
    assert {one.fix.get("namespace") for one in fields} == {"fx-quoting-sellside"}
    assert {one.fix.versions for one in fields} == {("4.4",)}, "plural, from the root"
    assert all(one.fix.source for one in fields), "every field says where it was read"


def test_the_grammar_drives_and_the_vocabulary_is_the_index_it_resolves_through() -> None:
    """One field per `<tag-constraint>`; the counts are the file's own."""
    fields, parsed = _fields(standard=STANDARD.get)
    report = parsed.report

    assert (report.bindings, report.constraints, report.groups) == (3, 23, 2)
    assert report.vocabulary == 20
    shapes = collections.Counter(
        "list"
        if pyarrow.types.is_list(f.dtype)
        else "struct"
        if pyarrow.types.is_struct(f.dtype)
        else "scalar"
        for f in fields
    )
    assert shapes == {"scalar": 25, "list": 2, "struct": 3}, (
        "23 constraints resolved, 2 vocabulary-only tags, 2 groups, 3 bindings' messages"
    )
    by_tag = _scalars(fields)
    assert len(by_tag[54]) == 3, "used in three bindings, declared three times"
    assert [one.fix.msgtypes for one in by_tag[11]] == [("D",), ("j",), ("j",)], (
        "'j Inbound' and 'j Outbound' are one message type read twice"
    )


def test_a_tag_no_grammar_places_is_still_a_field_and_says_so() -> None:
    fields, parsed = _fields(standard=STANDARD.get)
    by_tag = _scalars(fields)

    assert parsed.report.vocabulary_only == 2
    assert {by_tag[7777][0].fix.source, by_tag[7778][0].fix.source} == {"vocabulary"}
    assert by_tag[7777][0].dtype == pyarrow.bool_()
    assert by_tag[7778][0].fix.type == "utc-time-only"
    assert by_tag[40][0].fix.source == "binding:D", "and a placed one names its binding"


def test_a_tag_the_file_cannot_name_takes_the_standards_reading_and_is_counted() -> None:
    """Never a synthesised `Tag29`: it would become a canonical name and a filename."""
    fields, parsed = _fields(standard=STANDARD.get)
    by_tag = _scalars(fields)

    assert parsed.report.resolved_by_standard == 2
    assert parsed.report.unresolved == 0
    assert (by_tag[29][0].fix.canonical, by_tag[29][0].dtype) == ("NoLinesOfText", pyarrow.int32())
    assert by_tag[9999][0].fix.canonical == "FakeVendorFlag"

    fields, parsed = _fields()
    assert parsed.report.unresolved == 3, (
        "without the standard: the entry, its constraint, and 9999"
    )
    assert 29 not in _scalars(fields) and 9999 not in _scalars(fields)
    assert not any(re.fullmatch(r"Tag\d+", one.fix.canonical) for one in fields)


def test_datatypes_go_through_the_one_table_and_the_ullink_word_stays_descriptive() -> None:
    fields, _ = _fields(standard=STANDARD.get)
    by_tag = _scalars(fields)

    assert by_tag[60][0].dtype == pyarrow.timestamp("us", tz="UTC"), (
        "'expressed in UTC' fixes the zone"
    )
    assert by_tag[60][0].fix.type == "utc-timestamp", "the word as written, deciding nothing"
    assert by_tag[64][0].dtype == pyarrow.timestamp("us")
    assert by_tag[38][0].dtype == pyarrow.float64()
    assert by_tag[9690][0].dtype == pyarrow.int32()
    assert by_tag[54][0].dtype == pyarrow.string()
    assert {one.fix.type for one in fields if one.fix.type} == {
        "string",
        "integer",
        "float",
        "char",
        "utc-date",
        "boolean",
        "utc-timestamp",
        "utc-time-only",
    }, "the eight spellings the corpus is complete at, and no ninth"


# -- enumerations ---------------------------------------------------------------


def test_enumerations_come_from_the_constraints_and_carry_their_namespace() -> None:
    fields, parsed = _fields(standard=STANDARD.get)
    by_tag = _scalars(fields)

    def values(tag: int) -> list[str]:
        return sorted({v.value for f in by_tag[tag] for v in f.fix.enumerated})

    assert len(values(40)) == 19, "^[1-9A-J]$: nine digits and ten letters"
    assert values(22) == ["?", "BIC", "ISO", "ZZ"], "a simple escape is the character"
    assert values(9692) == list("1234689DEGIJKLMP"), "^[1-4689DEGI-MP]$, two levels down"
    assert values(18) == list("123456789A"), "a space-separated repeat: the class is the set"
    assert values(167) == ["CS", "FUT", "NONE", "OPT"]
    assert values(11) == [] and values(64) == [], "formats enumerate nothing"
    assert all(
        v.namespaces == ("fx-quoting-sellside",) for f in fields for v in f.fix.enumerated
    ), "every value says who declared it"
    assert dict(parsed.report.enumerated) == {"class": 6, "alternation": 4, "repeat": 1}
    assert dict(parsed.report.skipped) == {".*": 4, "quantified format": 1, "unanchored": 1}
    assert (
        parsed.report.regexps
        == 17
        == sum(parsed.report.enumerated.values()) + sum(parsed.report.skipped.values())
    ), "every regexp counted once, enumerated or skipped, so coverage is known rather than inferred"


@pytest.mark.parametrize(
    ("regexp", "count", "kind"),
    [
        ("^[1-9A-J]$", 19, "class"),
        ("^[0-9LS]$", 12, "class"),
        ("^[1-4689DEGI-MP]$", 16, "class"),
        ("^[1-59BCEFHILM]$", 14, "class"),
        ("^[AB-]$", 3, "class"),
        ("^(CS|FUT|OPT|NONE)$", 4, "alternation"),
        (r"^(TREASURY|PROVINCE|MUNICIPAL|\?)$", 4, "alternation"),
        ("^(AB)$", 1, "alternation"),
        ("^[1-9A]( [1-9A])*$", 10, "repeat"),
        ("^[0-9A-SU-Za-e]( [0-9A-SU-Za-e])*$", 40, "repeat"),
    ],
)
def test_a_closed_set_is_enumerated_with_its_ranges_expanded(
    regexp: str, count: int, kind: str
) -> None:
    values, decided = enumerated_values(regexp)
    assert values is not None and len(values) == count and decided == kind
    assert len(set(values)) == len(values), "and nothing twice"


@pytest.mark.parametrize(
    ("regexp", "kind"),
    [
        (".*", ".*"),
        (".-.", "unanchored"),
        ("[NY]", "unanchored"),
        ("^...$", "other format"),
        ("^[0-9]{4}((0[1-9])|10|11|12)$", "quantified format"),
        (r"^\d{4}(0[1-9]|1[0-2])(0[1-9]|[1-2]\d|3[01]|w[1-5])?$", "quantified format"),
        ("^(A|B)+$", "quantified format"),
        ("^[A-z]$", "range across alphabets"),
        ("^[a-Z]$", "range across alphabets"),
        ("^[9-1]$", "inverted range"),
        ("^[1-9A]( [1-9B])*$", "repeat of two different classes"),
        ("^(A|B[0-9])$", "alternation with regex syntax in a token"),
    ],
)
def test_a_format_is_refused_and_counted_under_its_kind(regexp: str, kind: str) -> None:
    """`[A-z]` spans punctuation and is nobody's enumeration; a quantified class is a format."""
    values, decided = enumerated_values(regexp)
    assert values is None and decided == kind


# -- constraint scope ------------------------------------------------------------


def test_one_tag_in_several_bindings_is_several_readings_whose_fold_is_the_union() -> None:
    """The parser reports what each binding says; `Field.merge` does the folding."""
    fields, _ = _fields(standard=STANDARD.get)
    readings = _scalars(fields)[54]

    assert [sorted(v.value for v in one.fix.enumerated) for one in readings] == [
        ["1", "2"],
        ["1", "2", "7", "8", "9"],
        ["1", "2"],
    ], "three bindings, three value sets, none intersected and none last-wins"
    folded = readings[0]
    for one in readings[1:]:
        folded = folded.merge(one)
    assert sorted(v.value for v in folded.fix.enumerated) == ["1", "2", "7", "8", "9"]
    assert set(folded.fix.msgtypes) == {"D", "j"}
    assert folded.nullable, "a <condition-expression> in one binding is not a nullable=False field"


def test_folding_the_file_by_tag_loses_nothing_and_raises_nothing() -> None:
    fields, _ = _fields(standard=STANDARD.get)
    scalars = _scalars(fields)
    folded = {tag: readings[0] for tag, readings in scalars.items()}
    for tag, readings in scalars.items():
        for one in readings[1:]:
            folded[tag] = folded[tag].merge(one)
    declared = {
        (tag, v.value)
        for tag, readings in scalars.items()
        for one in readings
        for v in one.fix.enumerated
    }
    kept = {(tag, v.value) for tag, one in folded.items() for v in one.fix.enumerated}
    assert kept == declared, "0 readings lost"


# -- the tree ---------------------------------------------------------------------


def test_a_nested_grammar_is_a_group_with_its_members_inside_its_own_type() -> None:
    """The counter is the first constraint, positionally; the group is named by it."""
    fields, _ = _fields(standard=STANDARD.get)
    groups = {one.fix.canonical: one for one in fields if pyarrow.types.is_list(one.dtype)}

    assert set(groups) == {"NoHops", "NoDealers"}, "one at each depth, both yielded"
    hops = groups["NoHops"]
    assert hops.fix.tag is None and hops.fix.get("counter") == "627", "the tag stays on the counter"
    assert hops.fix.component == "NoHops", "the record is a block that is a list, keyed by name"
    item = hops.dtype.value_type
    assert pyarrow.types.is_struct(item) and hops.dtype.value_field.name == "Hop"
    assert [item.field(i).name for i in range(item.num_fields)] == [
        "HopCompID",
        "HopSendingTime",
        "NoDealers",
    ], "members in wire order, the nested group among them"
    dealers = item.field(2).type
    assert pyarrow.types.is_list(dealers) and dealers.value_field.name == "Dealer"
    assert item.field(2).metadata == {b"fix:tag": b"9690"}, (
        "placed in its parent by its counter's tag"
    )
    assert hops.fix.msgtypes == ("D",)


def test_a_binding_is_a_message_carrying_its_type_and_its_members() -> None:
    fields, _ = _fields(standard=STANDARD.get)
    messages = [one for one in fields if pyarrow.types.is_struct(one.dtype) and one.fix.msgtype]

    assert [(one.fix.canonical, one.fix.msgtype, one.dtype.num_fields) for one in messages] == [
        ("D", "D", 12),
        ("j", "j", 4),
        ("j", "j", 2),
    ], "eleven scalars and one group in D; one message type read twice"
    assert all(one.fix.get("component") for one in messages), "a message is a block"


# -- what it tolerates, and what it refuses ---------------------------------------


def test_a_damaged_file_yields_nothing_and_a_wrong_root_is_refused() -> None:
    assert list(Field.from_cfb(FIXTURES / "Damaged_Capture.cfb")) == [], "one bad file in a scan"
    with pytest.raises(ValueError, match="cplugin-configuration"):
        list(Field.from_cfb(FIXTURES / "Not_A_Capture.cfb"))


def test_the_iterator_is_lazy_and_in_document_order() -> None:
    parsed = Field.from_cfb(SELL_SIDE, standard=STANDARD.get)
    first = next(iter(parsed))
    assert first.fix.tag == 11 and first.fix.msgtypes == ("D",), (
        "the first constraint of the first binding"
    )
    assert parsed.report.constraints == 1, "and nothing further has been walked yet"


def test_the_registry_knows_nothing_of_the_format() -> None:
    source = Path(rekep.__file__).parent
    found = {
        f"{path.relative_to(source)}:{number}"
        for path in sorted(source.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if "fields_from_cfb" in line
    }
    assert found == set()


# -- ingestion, the generic path ---------------------------------------------------


def test_ingesting_and_reopening_round_trips_every_record_with_its_type(tmp_path: Path) -> None:
    from rekep.fix import FixRegistry
    from rekep.fix.entries import record_key

    registry = FixRegistry(cache_dir=tmp_path / "fix")
    parsed = Field.from_cfb(SELL_SIDE, standard=STANDARD.get)
    # Readings fold before they are stored: two bindings of one message
    # are one declaration, where the registry would see two shapes of one
    # name and dispute them. The parser yields; the caller's one merge folds.
    folded: dict[str, Field] = {}
    for one in parsed:
        key = record_key(one)
        folded[key] = folded[key].merge(one) if key in folded else one
    registry.add_fields(list(folded.values()), parsed.report.namespace)

    reopened = FixRegistry(cache_dir=tmp_path / "fix")
    stored = reopened.field_records("fx-quoting-sellside")
    assert reopened.field(627, namespace="fx-quoting-sellside").dtype == pyarrow.int32(), (
        "the counter keeps its type beside its group"
    )
    assert set(reopened.repeating_group_records("fx-quoting-sellside")) == {"NoHops", "NoDealers"}
    j = reopened.component_records("fx-quoting-sellside")["j"].into_record()
    assert pyarrow.types.is_struct(j.dtype) and j.dtype.num_fields == 4, (
        "Inbound and Outbound, one message: the union of their members"
    )
    assert reopened.field(60, namespace="fx-quoting-sellside").dtype == pyarrow.timestamp(
        "us", tz="UTC"
    )
    assert len(reopened.field(40, namespace="fx-quoting-sellside").fix.enumerated) == 19
    assert sorted(
        v.value for v in reopened.field(54, namespace="fx-quoting-sellside").fix.enumerated
    ) == ["1", "2", "7", "8", "9"], "the three bindings' readings folded on the way in"
    for record in stored.values():
        assert Field.from_dict(record.into_dict()).into_dict() == record.into_dict(), (
            record.fix.canonical
        )


def test_two_bridges_are_two_namespaces_and_one_unified_view(tmp_path: Path) -> None:
    """One tag, two names: an identity with both spellings. Different tags: two records."""
    from rekep.fix import FixRegistry

    registry = FixRegistry(cache_dir=tmp_path / "fix")
    for path in (SELL_SIDE, BUY_SIDE):
        parsed = Field.from_cfb(path, standard=STANDARD.get)
        registry.add_fields(list(parsed), parsed.report.namespace)
    reopened = FixRegistry(cache_dir=tmp_path / "fix")

    eleven = reopened.field(11)
    assert eleven.fix.canonical == "ClOrdID", "the first declarer's spelling"
    assert [(a.name, a.source) for a in eleven.fix.named_aliases] == [("SACHA", "rates-buyside")]

    assert reopened.field(5001, namespace="fx-quoting-sellside").fix.canonical == "GLMXTradeType"
    assert reopened.field(5001, namespace="rates-buyside").fix.canonical == "MaturityDate"
    unified = reopened.field(5001)
    assert (unified.fix.canonical, [a.name for a in unified.fix.named_aliases]) == (
        "GLMXTradeType",
        ["MaturityDate"],
    ), "two stored records, both reachable, and one view that names both"


# -- what the design review caught ------------------------------------------------


@pytest.mark.parametrize(
    ("regexp", "kind"),
    [
        ("^(A.|B+)$", "alternation with regex syntax in a token"),
        (r"^(A|\d)$", "alternation with regex syntax in a token"),
        ("^[^0-9]$", "negated class"),
        (r"^[\d]$", "shorthand escape in class"),
        ("^[A-z]$", "range across alphabets"),
        ("^[9-1]$", "inverted range"),
    ],
)
def test_an_open_set_dressed_as_a_class_or_a_token_is_refused_by_name(
    regexp: str, kind: str
) -> None:
    """`.` and `+` inside a token are syntax, `[^0-9]` is everything else, `\d` is a class.

    Each is an open set a naive reader enumerates as literals -- `A.` and
    `B+` as two values, the caret as a member -- and each is refused under a
    kind that says why, so the coverage report names what it passed over.
    """
    values, decided = enumerated_values(regexp)
    assert values is None and decided == kind


def test_a_simple_escape_is_the_character_and_nothing_else_is_syntax() -> None:
    assert enumerated_values(r"^(A\.B|C\-D|\?)$")[0] == ("A.B", "C-D", "?")
    assert enumerated_values(r"^[\-A\.]$")[0] == ("-", "A", ".")


def test_vocabulary_only_tags_come_out_in_document_order() -> None:
    fields, _ = _fields(standard=STANDARD.get)
    unplaced = [one.fix.tag for one in fields if one.fix.source == "vocabulary"]
    assert unplaced == [7777, 7778], "as the file lists them, not sorted"


def test_a_member_inside_a_container_is_the_dictionarys_member_shape() -> None:
    """The full reading is yielded on its own; inside the tree a member is name, type, tag."""
    fields, _ = _fields(standard=STANDARD.get)
    message = next(one for one in fields if one.fix.msgtype == "D")
    member = message.dtype.field(0)
    assert member.name == "ClOrdID"
    assert set(member.metadata or {}) == {b"fix:tag"}, (
        "no versions, values or provenance in the tree"
    )
    members = {member.name: member for member in message.dtype}
    group = members["NoHops"]
    assert set(group.metadata or {}) == {b"fix:tag"}, (
        "a group member is a tag, as in the dictionary"
    )
    assert str(group.type).startswith("list<Hop: struct<"), (
        "repeating the entry the naming rule gives"
    )


def test_an_empty_binding_declares_no_message(tmp_path: Path) -> None:
    """`struct([])` is what an unexpanded reference looks like; a message must not."""
    empty = FIXTURES / "FX_Quoting_SellSide.cfb"
    text = empty.read_text(encoding="utf-8").replace(
        '<grammar-binding type="j Outbound">',
        '<grammar-binding type="Q"><grammar checkordering="false"/></grammar-binding>'
        '<grammar-binding type="j Outbound">',
    )
    (tmp_path / "With_Empty.cfb").write_text(text, encoding="utf-8")
    fields, parsed = _fields(tmp_path / "With_Empty.cfb", standard=STANDARD.get)
    assert parsed.report.bindings == 4 and parsed.report.empty_bindings == 1
    assert not any(one.fix.msgtype == "Q" for one in fields)
    assert parsed.report.namespace == "with-empty"
