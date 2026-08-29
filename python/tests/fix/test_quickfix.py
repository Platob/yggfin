"""The QuickFIX source read as fields, components, and the session layer.

It supplies machine-readable value symbols, session fields, and declaration
trees where higher-priority dictionaries omit them. It supplies no prose, so
these tests cover only the parts it answers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rekep.fields import Field
from rekep.fix.quickfix import (
    SPEC_VERSIONS,
    block,
    entry_name,
    entry_of,
    field_member,
    group_member,
    is_group,
    is_reference,
    members_of,
    parse_declarations,
    parse_session,
    parse_spec,
    reference_member,
    spec_name,
)

SPEC = (Path(__file__).parent / "fixtures" / "FIX44.xml").read_text()

#: Derived from the fixture, then pinned: its fields, values, and the blocks
#: it declares -- two components and the one message.
EXPECTED_FIELDS = 11
EXPECTED_VALUES = 6
EXPECTED_DECLARATIONS = 3


def test_the_fixture_is_the_shape_the_tests_assume() -> None:
    parsed = parse_spec(SPEC)
    assert len(parsed) == EXPECTED_FIELDS
    assert sum(len(field.values) for field in parsed.values()) == EXPECTED_VALUES
    assert len(parse_declarations(SPEC)) == EXPECTED_DECLARATIONS


# -- which file is which version ----------------------------------------------


@pytest.mark.parametrize(
    "version,name",
    [
        ("4.0", "FIX40.xml"),
        ("4.4", "FIX44.xml"),
        ("5.0", "FIX50.xml"),
        ("5.0.SP2", "FIX50SP2.xml"),
        ("FIXT1.1", "FIXT11.xml"),
        ("fixt1.1", "FIXT11.xml"),
        ("FIX.4.2", "FIX42.xml"),
    ],
)
def test_a_version_names_its_spec_file(version: str, name: str) -> None:
    """The punctuation is decoration on both sides; `FIXT` is a protocol."""
    assert spec_name(version) == name


def test_every_version_this_package_knows_has_a_spelling() -> None:
    """A version in the list with no file name would 404 at the first fetch."""
    assert len(SPEC_VERSIONS) == 9
    assert all(spec_name(version).endswith(".xml") for version in SPEC_VERSIONS)
    assert len({spec_name(version) for version in SPEC_VERSIONS}) == len(SPEC_VERSIONS)


# -- what a field is ----------------------------------------------------------


def test_a_field_comes_back_with_its_tag_name_and_datatype() -> None:
    parsed = parse_spec(SPEC)
    assert parsed[54].name == "Side"
    assert parsed[54].datatype == "CHAR"
    assert parsed[103].name == "OrdRejReason"
    assert parsed[103].datatype == "INT"


def test_an_enumerated_value_comes_back_as_its_symbol() -> None:
    """`description=` in the spec is the value's *name*, which is the point of it."""
    parsed = parse_spec(SPEC)
    assert parsed[54].values == {"1": "BUY", "2": "SELL", "7": "UNDISCLOSED"}
    assert parsed[43].values == {"Y": "POSSIBLE_DUPLICATE", "N": "ORIGINAL_TRANSMISSION"}


def test_a_field_with_no_enumeration_has_no_values() -> None:
    assert parse_spec(SPEC)[103].values == {}


def test_a_document_that_is_not_a_spec_reads_as_nothing() -> None:
    """Malformed XML contributes no machine-readable declaration."""
    assert parse_spec("") == {}
    assert parse_spec("<fix><fields>") == {}
    assert parse_spec("not xml at all") == {}
    assert parse_declarations("<broken") == {}
    assert parse_session("<broken") == ()


def test_a_field_missing_its_number_or_name_is_skipped_rather_than_guessed() -> None:
    document = (
        "<fix><fields>"
        "<field number='1' name='Account' type='STRING'/>"
        "<field name='NoNumber' type='STRING'/>"
        "<field number='x' name='NotATag' type='STRING'/>"
        "<field number='2'/>"
        "</fields></fix>"
    )
    assert set(parse_spec(document)) == {1}


# -- reusable components -----------------------------------------------------


def test_a_component_preserves_its_group_tree_and_wire_order() -> None:
    parties = parse_declarations(SPEC)["Parties"]
    assert [member.name for member in members_of(parties)] == ["NoPartyIDs"]
    group = members_of(parties)[0]
    assert is_group(group)
    assert group.fix.tag == 453
    rows = members_of(entry_of(group))
    assert [member.name for member in rows] == [
        "PartyID",
        "PartyIDSource",
        "PartyRole",
        "PtysSubGrp",
    ]
    assert [member.fix.tag for member in rows[:3]] == [448, 447, 452]
    assert is_reference(rows[-1])


def test_field_and_component_references_match_by_the_column_fold() -> None:
    document = """
    <fix>
      <fields><field number='11' name='ClOrdID' type='STRING'/></fields>
      <components>
        <component name='OrderBlock'><field name='Cl_Ord_ID' required='Y'/></component>
      </components>
      <messages>
        <message name='Order' msgtype='D'>
          <component name='ORDER_BLOCK' required='Y'/>
        </message>
      </messages>
    </fix>
    """
    declared = parse_declarations(document)
    field = members_of(declared["OrderBlock"])[0]
    reference = members_of(declared["Order"])[0]

    assert (field.name, field.fix.tag) == ("Cl_Ord_ID", 11)
    assert (reference.name, reference.fix.component) == ("ORDER_BLOCK", "ORDER_BLOCK")


@pytest.mark.parametrize(
    "document,owner",
    [
        (
            "<fix><fields><field number='1' name='A_B'/>"
            "<field number='2' name='AB'/></fields></fix>",
            "field",
        ),
        (
            "<fix><fields/><components><component name='A_B'/>"
            "<component name='AB'/></components></fix>",
            "component",
        ),
    ],
)
def test_quickfix_declarations_refuse_names_that_collide_by_fold(document: str, owner: str) -> None:
    with pytest.raises(ValueError, match=rf"FIX {owner} .* collides by fold"):
        parse_declarations(document)


def test_a_nested_group_is_its_own_component_declaration() -> None:
    subgroup = parse_declarations(SPEC)["PtysSubGrp"]
    group = members_of(subgroup)[0]
    assert is_group(group)
    assert (group.name, group.fix.tag) == ("NoPartySubIDs", 802)
    assert [(member.name, member.fix.tag) for member in members_of(entry_of(group))] == [
        ("PartySubID", 523),
        ("PartySubIDType", 803),
    ]


def test_a_component_declaration_round_trips_as_a_typed_document() -> None:
    """The document is the declaration's own: a struct of structs and lists,
    so what comes back is the same tree and not a resemblance of it."""
    parties = parse_declarations(SPEC)["Parties"]
    rebuilt = Field.from_dict(parties.into_dict())
    assert rebuilt == parties
    group = members_of(rebuilt)[0]
    assert is_group(group)
    assert is_reference(members_of(entry_of(group))[-1])


def test_a_round_trip_keeps_which_members_the_standard_requires() -> None:
    """Requiredness is nullability now, and a dumped declaration that lost it
    would read as a standard where nothing is mandatory."""
    declared = block(
        "Parties",
        [
            group_member(
                "NoPartyIDs",
                453,
                [
                    field_member("PartyID", 448),
                    field_member("PartyIDSource", 447, required=True),
                ],
                required=True,
            )
        ],
    )
    rebuilt = Field.from_dict(declared.into_dict())
    assert rebuilt == declared
    group = members_of(rebuilt)[0]
    assert group.nullable is False
    assert [member.nullable for member in members_of(entry_of(group))] == [True, False]


def test_each_member_kind_is_told_apart_by_its_shape() -> None:
    """No member stores its kind any more: a group is a list, a reference is a
    struct with no members yet, and a plain field is neither."""
    plain = field_member("PartyID", 448)
    group = group_member("NoPartyIDs", 453, [plain])
    reference = reference_member("PtysSubGrp")
    assert (is_group(plain), is_reference(plain)) == (False, False)
    assert (is_group(group), is_reference(group)) == (True, False)
    assert (is_group(reference), is_reference(reference)) == (False, True)


@pytest.mark.parametrize(
    ("group", "entry"),
    [
        # The count is dropped and the stem singularized -- 269 of the 507
        # groups the dictionary declares land on a real field name this way.
        ("NoPartyIDs", "PartyID"),
        ("NoLegs", "Leg"),
        ("NoMDEntries", "MDEntry"),
        ("NoCapacities", "Capacity"),
        # A stem that hisses takes `es`, one that does not takes `s`.
        ("NoSecondaryAssetClasses", "SecondaryAssetClass"),
        ("NoOfSecSizes", "OfSecSize"),
        # The one plural in the whole dictionary no rule reaches...
        ("NoContractualMatrices", "ContractualMatrix"),
        # ...and the ones that were never plural to begin with.
        ("NoRelatedSym", "RelatedSym"),
        ("NoSecurityAltID", "SecurityAltID"),
        ("NoPosAmt", "PosAmt"),
        # A regular plural that merely looks Latin.
        ("NoReturnRatePrices", "ReturnRatePrice"),
    ],
)
def test_a_group_says_what_one_row_of_it_is(group: str, entry: str) -> None:
    assert entry_name(group) == entry


def test_the_entry_a_group_repeats_is_the_declaration_it_carries() -> None:
    """Arrow would call it `item`; a FIX group repeats something named."""
    group = group_member("NoPartyIDs", 453, [field_member("PartyID", 448)])

    assert group.dtype.field(0).name == "PartyID"
    assert entry_of(group).name == "PartyID"
    assert [member.name for member in members_of(entry_of(group))] == ["PartyID"]


def test_a_component_refusing_an_unknown_field_names_the_path() -> None:
    document = """
    <fix><components><component name='Parties'>
      <field name='Missing' required='N'/>
    </component></components><fields/></fix>
    """
    with pytest.raises(ValueError, match="Parties.*Missing"):
        parse_declarations(document)


def test_recursive_components_are_refused_by_their_chain() -> None:
    document = """
    <fix><components>
      <component name='A'><component name='B' required='N'/></component>
      <component name='B'><component name='A' required='N'/></component>
    </components><fields/></fix>
    """
    with pytest.raises(ValueError, match=r"A -> B -> A"):
        parse_declarations(document)


def test_an_unknown_component_reference_names_its_owner() -> None:
    document = """
    <fix><components>
      <component name='Parties'><component name='Missing' required='N'/></component>
    </components><fields/></fix>
    """
    with pytest.raises(ValueError, match="Parties.*Missing"):
        parse_declarations(document)


# -- what every message carries -----------------------------------------------


def test_the_session_layer_is_the_header_then_the_trailer() -> None:
    session = parse_session(SPEC)
    assert [name for name, _ in session] == [
        "BeginString",
        "MsgSeqNum",
        "PossDupFlag",
        "CheckSum",
        "Signature",
    ]


def test_the_session_layer_says_which_fields_a_message_must_carry() -> None:
    session = dict(parse_session(SPEC))
    assert session["BeginString"] is True
    assert session["MsgSeqNum"] is True
    assert session["CheckSum"] is True
    assert session["PossDupFlag"] is False
    assert session["Signature"] is False


def test_a_group_inside_the_header_is_not_a_session_field() -> None:
    """`HopGrp` is a repeating group, and one row of it is not one value."""
    assert "HopGrp" not in dict(parse_session(SPEC))
