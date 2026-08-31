"""Offline Orchestra parsing through the registry's existing Arrow shapes."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix.orchestra import SourceProvenance, parse_orchestra, parse_quickfix
from rekep.fix.quickfix import entry_of, is_group, is_reference, members_of

FIXTURES = Path(__file__).parent / "fixtures"
ORCHESTRA = (FIXTURES / "orchestra.xml").read_bytes()
QUICKFIX = (FIXTURES / "FIX44.xml").read_bytes()


def fixture_registry():
    """The one complete offline artifact used by the parser assertions."""
    source = SourceProvenance.for_bytes(
        ORCHESTRA,
        source_id="fixture",
        namespace="fixtrading-udf",
        version="fixture",
        url="file:///orchestra.xml",
        format="orchestra",
        license_url="https://example.test/terms",
    )
    return parse_orchestra(ORCHESTRA, source)


def test_repository_metadata_and_every_definition_family_are_retained() -> None:
    parsed = fixture_registry()

    assert parsed.repository_name == "FIX.Test"
    assert parsed.repository_version == "FIX.Test_EP7"
    assert parsed.source.version == "fixture"
    assert parsed.declaration_version == parsed.source.protocol_version == "FIX.Test"
    assert dict(parsed.metadata) == {
        "title": "Offline Orchestra fixture",
        "publisher": "FIX Trading Community",
        "rights": "Fixture terms",
    }
    assert (len(parsed.datatypes), len(parsed.code_sets), len(parsed.fields)) == (8, 1, 12)
    assert (len(parsed.messages), len(parsed.components), len(parsed.groups)) == (1, 1, 1)


def test_pedigree_covers_datatypes_codes_blocks_and_membership() -> None:
    parsed = fixture_registry()
    datatype = next(datatype for datatype in parsed.datatypes if datatype.name == "VendorMapped")
    code_set = parsed.code_sets[0]
    group = parsed.groups[0]

    assert datatype.pedigree.into_dict() == {"added": "FIX.Test", "added_ep": "3"}
    assert code_set.pedigree.into_dict() == {"added": "FIX.2.7"}
    assert code_set.values[0].pedigree.into_dict() == {"added": "FIX.2.7"}
    assert code_set.values[1].pedigree.into_dict() == {
        "deprecated": "FIX.Test",
        "deprecated_ep": "7",
    }
    assert parsed.components[0].pedigree.into_dict() == {"added": "FIX.4.3"}
    assert parsed.messages[0].pedigree.into_dict() == {"added": "FIX.2.7"}
    assert group.pedigree.into_dict() == {"added": "FIX.4.3"}
    assert group.members[0].pedigree.into_dict() == {
        "deprecated": "FIX.Test",
        "deprecated_ep": "7",
    }


def test_codesets_resolve_to_their_base_type_without_closing_the_enum() -> None:
    side = fixture_registry().field(54)

    assert side is not None
    assert side.original_datatype == "SideCodeSet"
    assert side.datatype == "char" and side.arrow_type == pyarrow.string()
    assert [(value.value, value.name) for value in side.values] == [("1", "Buy"), ("2", "Sell")]
    assert side.pedigree.into_dict() == {
        "added": "FIX.2.7",
        "updated": "FIX.Test",
        "updated_ep": "6",
        "deprecated": "FIX.Test",
        "deprecated_ep": "7",
        "replaced": "FIX.Test",
        "replaced_ep": "7",
        "replaced_by_field": 1054,
        "issue": "TEST-54",
    }


def test_unknown_and_disputed_types_are_strings_with_the_source_type_kept() -> None:
    parsed = fixture_registry()
    mapped = parsed.field(7000)
    unknown = parsed.field(7001)
    disputed = parsed.field(7002)

    assert mapped is not None and mapped.datatype == "int" and mapped.arrow_type == pyarrow.int32()
    assert unknown is not None and unknown.arrow_type == pyarrow.string()
    assert unknown.original_datatype == "FutureIdentifier"
    assert unknown.fallback == "unknown source datatype"
    assert disputed is not None and disputed.arrow_type == pyarrow.string()
    assert disputed.type_readings == ("String", "int")
    assert disputed.aliases == ("DisputedValueNumber",)
    assert len(parsed.conflicts) == 1
    assert (parsed.conflicts[0].key, parsed.conflicts[0].part) == ("7002", "datatype")


def test_registered_udf_examples_keep_standardization_and_prose_values() -> None:
    parsed = fixture_registry()
    max_show = parsed.field(9001)
    cross = parsed.field(9002)
    support = parsed.field(9003)

    assert max_show is not None
    assert (max_show.name, max_show.aliases) == ("MaxShow", ("MaxShow1",))
    assert max_show.replacement is not None
    assert (max_show.replacement.tag, max_show.replacement.name) == (210, "MaxShow")
    assert cross is not None and cross.datatype == "string"
    assert cross.fallback == "prose permits alphanumeric values"
    assert support is not None
    assert [(value.value, value.description) for value in support.values] == [
        ("1", "Supports UDFs in the message"),
        ("2", "Supports UDFs in repeating groups"),
    ]


def test_source_field_projects_provenance_and_original_type_to_arrow_metadata() -> None:
    declared = fixture_registry().field(9001).into_field()  # type: ignore[union-attr]

    assert declared.dtype == pyarrow.float64()
    assert declared.metadata["fix:namespace"] == "fixtrading-udf"
    assert declared.metadata["fix:source"] == "fixture"
    assert declared.metadata["fix:original-type"] == "Qty"
    assert declared.metadata["fix:replacement-tag"] == "210"
    assert declared.metadata["fix:source-name"] == "MaxShow1"
    assert declared.metadata["fix:protocol-version"] == "FIX.Test"
    assert declared.fix.version == "FIX.Test"
    assert declared.fix.named_aliases[0].name == "MaxShow1"


def test_source_field_keeps_the_utc_zone_its_description_declares() -> None:
    document = b"""<repository name='FIX.Test' version='FIX.Test_EP7'>
      <fields>
        <field id='42' name='OrigTime' type='UTCTimestamp'>
          <annotation><documentation>
            Original time of message transmission, expressed in UTC.
          </documentation></annotation>
        </field>
      </fields>
    </repository>"""

    field = parse_orchestra(document).field(42)

    assert field is not None and field.arrow_type == pyarrow.timestamp("us")
    assert field.into_field().dtype == pyarrow.timestamp("us", tz="UTC")


def test_folded_name_collisions_keep_every_tag_and_record_source_spelling() -> None:
    document = b"""<repository name='FIX.UDF' version='1.0'>
      <fields>
        <field id='5187' name='Reserved1' type='String'/>
        <field id='5829' name='RESERVED1' type='String'/>
        <field id='9100' name='ContraBroker1' type='String'>
          <annotation><documentation>
            ADDED TO FIX 4.4 TAG: 375 (ContraBroker)
          </documentation></annotation>
        </field>
        <field id='9421' name='ContraBroker2' type='String'>
          <annotation><documentation>
            ADDED TO FIX 4.4 TAG: 375 (ContraBroker)
          </documentation></annotation>
        </field>
      </fields>
    </repository>"""

    parsed = parse_orchestra(document)

    assert [field.name for field in parsed.fields] == [
        "Reserved1",
        "RESERVED15829",
        "ContraBroker1",
        "ContraBroker2",
    ]
    assert len({field.into_field().name for field in parsed.fields}) == 4
    disambiguated = parsed.field(5829)
    assert disambiguated is not None and not disambiguated.aliases
    assert disambiguated.into_field().metadata["fix:source-name"] == "RESERVED1"
    assert parsed.field(9100).replacement.tag == 375  # type: ignore[union-attr]


def test_messages_components_groups_and_nested_references_share_the_existing_shape() -> None:
    parsed = fixture_registry()
    declared = parsed.declarations()
    message = declared["NewOrderSingle"]

    assert message.fix.msgtype == "D"
    members = members_of(message)
    assert [member.name for member in members] == ["Instrument", "Side", "NoPartyIDs"]
    assert is_reference(members[0]) and members[0].nullable is False
    assert members[1].fix.tag == 54 and members[1].nullable is False
    assert is_group(members[2]) and members[2].nullable is False
    group = entry_of(parsed.group_declarations()["Parties"])
    assert [member.name for member in members_of(group)] == [
        "PartyID",
        "PartyIDSource",
        "PartyRole",
        "Instrument",
    ]


@pytest.mark.parametrize(
    "document,message",
    [
        (b"", "empty"),
        (b"<fixr:repository", "malformed"),
        (b"<notRepository/>", "repository root"),
        (b"<!DOCTYPE repository [<!ENTITY x 'boom'>]><repository/>", "XML entities"),
        (
            b" " * 5000 + b"<!DOCTYPE repository [<!ENTITY x 'boom'>]><repository>&x;</repository>",
            "XML entities",
        ),
    ],
)
def test_malformed_or_unsafe_orchestra_documents_are_refused(document: bytes, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_orchestra(document)


def test_an_unknown_nested_reference_is_reported_when_the_tree_is_built() -> None:
    document = ORCHESTRA.replace(b'<fixr:componentRef id="100"/>', b'<fixr:componentRef id="999"/>')
    parsed = parse_orchestra(document)

    with pytest.raises(ValueError, match="unknown component 999"):
        parsed.group_declarations()


def test_a_recursive_group_reference_is_refused_before_tree_construction() -> None:
    document = b"""<repository name='FIX.Test' version='FIX.Test'>
      <fields><field id='453' name='NoPartyIDs' type='NumInGroup'/></fields>
      <groups><group id='101' name='Parties'>
        <numInGroup id='453'/><groupRef id='101'/>
      </group></groups>
    </repository>"""
    parsed = parse_orchestra(document)

    with pytest.raises(ValueError, match="recursive source group"):
        parsed.group_declarations()


def test_quickfix_uses_the_same_source_registry_and_declaration_model() -> None:
    parsed = parse_quickfix(QUICKFIX)

    assert parsed.repository_version == parsed.source.version == "4.4"
    assert parsed.declaration_version == "4.4"
    assert len(parsed.fields) == 11
    assert parsed.field(54).values[0].name == "BUY"  # type: ignore[union-attr]
    assert set(parsed.declarations()) == {"Parties", "PtysSubGrp", "TradeCaptureReport"}
    assert parsed.declarations()["TradeCaptureReport"].fix.version == "4.4"
    assert {group.name for group in parsed.groups} == {"NoPartyIDs", "NoPartySubIDs"}


def test_fixt_version_spelling_is_not_doubled() -> None:
    document = b"""<fix type='FIXT' major='1' minor='1' servicepack='0'>
      <header/><trailer/><messages/><components/>
      <fields><field number='8' name='BeginString' type='STRING'/></fields>
    </fix>"""

    assert parse_quickfix(document).source.version == "FIXT.1.1"


def test_source_revision_is_not_used_as_the_negotiated_quickfix_version() -> None:
    source = SourceProvenance.for_bytes(
        QUICKFIX,
        source_id="quickfix",
        version="FIX.4.4_EP280",
        format="quickfix",
    )

    parsed = parse_quickfix(QUICKFIX, source)
    field = parsed.field(54)

    assert parsed.source.version == "FIX.4.4_EP280"
    assert parsed.declaration_version == "4.4"
    assert field is not None and field.into_field().fix.version == "4.4"
    assert parsed.source.into_dict()["version"] == "FIX.4.4_EP280"


def test_the_same_complete_file_parses_deterministically() -> None:
    assert fixture_registry() == fixture_registry()
