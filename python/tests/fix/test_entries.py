"""One entry per identity: what a file holds, and what it answers about it.

The unit half of the layout. `test_registry.py` reads a whole store and
`test_data.py` reads the published one; these hold the entry itself to what a
variant may state, what a rename does to it, and how the merged reading of a
field is assembled from its versions.

Every identity here is synthetic (`FAKE-*`, `Fake*`), because none of this is
about which fields the standard has.
"""

from __future__ import annotations

import json

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    STANDARD,
    Alias,
    ComponentEntry,
    FieldEntry,
    fold,
    merged_field,
    slug_of,
    variant_of,
)
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import SpecComponent, SpecFieldRef, SpecGroup


def _entry(**changed: object) -> FieldEntry:
    """A numbered field declared for two versions, unless a test says otherwise."""
    declared: dict[str, object] = {
        "name": "FakeRole",
        "tag": 90001,
        "variants": {
            "4.4": {"type": "int", "description": "A role that no standard has."},
            "4.2": {"type": "char", "name": "FakeRoleCode"},
        },
    }
    return FieldEntry(**{**declared, **changed})


# -- what a file is named ----------------------------------------------------


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        ("PartyRole", "party_role"),
        ("NoPartyIDs", "no_party_i_ds"),
        ("FAKE.VENDOR.CODE", "fake_vendor_code"),
        ("fake-vendor-code", "fake_vendor_code"),
        ("Fake Vendor Code", "fake_vendor_code"),
    ],
)
def test_a_name_slugs_to_the_file_it_is_stored_in(name: str, slug: str) -> None:
    assert slug_of(name) == slug


def test_a_name_that_slugs_to_nothing_is_refused() -> None:
    """The slug lands in a file name, so it can never be empty or a path."""
    for hostile in ("", "   ", "...", "///"):
        with pytest.raises(ValueError, match="FIX registry entry"):
            slug_of(hostile)


@pytest.mark.parametrize(
    ("spelled", "folded"),
    [("PartyRole", "partyrole"), ("party_role", "partyrole"), ("Party Role", "partyrole")],
)
def test_a_name_is_matched_folded(spelled: str, folded: str) -> None:
    """The same fold a rendered key gets, so a name resolves here as it does there."""
    assert fold(spelled) == folded


# -- what an entry refuses ---------------------------------------------------


def test_an_entry_no_lookup_could_answer_for_is_refused() -> None:
    with pytest.raises(ValueError, match="has no name"):
        _entry(name="")
    with pytest.raises(ValueError, match="declared for no version"):
        _entry(variants={})
    with pytest.raises(ValueError, match="has no tag"):
        _entry(tag=None)
    with pytest.raises(ValueError, match="unknown FIX field kind"):
        _entry(kind="invented")


def test_a_vendor_field_must_not_claim_a_tag() -> None:
    """The two kinds are what a field *is*, and a tagged vendor field is neither."""
    with pytest.raises(ValueError, match="must not claim tag"):
        _entry(kind=NAMESPACE)
    assert _entry(name="FAKE.CODE", tag=None, kind=NAMESPACE).kind == NAMESPACE


def test_a_variant_may_only_state_what_a_version_can_differ_on() -> None:
    """A key the schema does not have is a fact nothing downstream would read."""
    with pytest.raises(ValueError, match="declares unknown"):
        _entry(variants={"4.4": {"invented": "yes"}})


# -- what an entry answers ---------------------------------------------------


def test_an_entry_reports_each_version_its_own_name_and_tag() -> None:
    entry = _entry()
    assert entry.names() == {"4.4": "FakeRole", "4.2": "FakeRoleCode"}
    assert entry.tags() == {"4.4": 90001, "4.2": 90001}
    assert entry.versions == ("4.4", "4.2")


def test_an_entry_answers_to_its_names_in_resolution_order() -> None:
    """Canonical first, then a name some version spells, then a declared alias."""
    entry = _entry(aliases=(Alias(name="FakeRolle", source="brk", occurrences=12),))
    assert entry.spellings() == ("FakeRole", "FakeRoleCode", "FakeRolle")


def test_an_alias_that_folds_to_a_name_the_entry_has_adds_nothing() -> None:
    """`FAKEROLE` is how a bridge shouts `FakeRole`, and matching folds case."""
    entry = _entry(aliases=(Alias(name="FAKEROLE"), Alias(name="fake_role")))
    assert entry.spellings() == ("FakeRole", "FakeRoleCode")


def test_a_version_the_entry_never_saw_has_no_declaration() -> None:
    """Absent is not "present and untyped": a caller has to be able to tell."""
    assert _entry().into_field("4.0") is None
    assert _entry().into_field("4.2").name == "FakeRoleCode"


def test_a_wildcard_variant_answers_for_every_version() -> None:
    """What a field outside the standard has: one reading, whatever was negotiated."""
    entry = _entry(name="FAKE.CODE", tag=None, kind=NAMESPACE, variants={ANY_VERSION: {}})
    assert entry.into_field("4.0") is not None
    assert entry.into_field("5.0.SP2").fix["version"] == "5.0.SP2"
    assert "tag" not in entry.into_field("4.4").fix, "no tag, rather than tag zero"
    assert entry.into_field("4.4").fix["kind"] == NAMESPACE


def test_a_declared_column_travels_with_the_field() -> None:
    entry = _entry(
        name="FAKE.CODE", tag=None, kind=NAMESPACE, variants={ANY_VERSION: {}}, column="fake"
    )
    assert entry.into_field("4.4").fix["column"] == "fake"


# -- reading a version's declaration back ------------------------------------


def test_a_variant_states_only_what_it_does_not_share() -> None:
    member = fix_field("FakeRole", 90001, "int", version="4.4")
    assert variant_of(member, "FakeRole", 90001) == {"type": "int"}
    assert variant_of(member, "FakeRoleCode", 90001) == {"name": "FakeRole", "type": "int"}
    assert variant_of(member, "FakeRole", 90002)["tag"] == 90001


def test_an_entry_round_trips_through_the_document_it_is_stored_as() -> None:
    entry = _entry(aliases=(Alias(name="FAKEROLE", source="pco", occurrences=3),))
    assert FieldEntry.from_dict(entry.into_dict()) == entry


def test_a_stored_alias_may_be_a_bare_name_or_carry_its_provenance() -> None:
    """Both spellings read; only one is written, and only when there is anything to say."""
    assert Alias.from_dict("FAKEROLE") == Alias(name="FAKEROLE")
    assert Alias.from_dict("FAKEROLE").into_dict() == {"name": "FAKEROLE"}
    carried = Alias(name="FAKEROLE", source="brk", occurrences=41)
    assert Alias.from_dict(carried.into_dict()) == carried
    with pytest.raises(ValueError, match="has no name"):
        Alias(name=" ")


def test_an_empty_part_is_not_written_into_the_document() -> None:
    """A file per identity is a file people read; empties are noise in a diff."""
    stored = _entry().into_dict()
    assert set(stored) == {"name", "tag", "versions"}
    assert stored["versions"]["4.4"] == {
        "type": "int",
        "description": "A role that no standard has.",
    }


# -- the merged reading ------------------------------------------------------


def test_a_merged_field_keeps_the_newest_identity_and_every_versions_knowledge() -> None:
    """One declaration carrying each version's disagreement, not resolving it away."""
    entry = _entry(
        variants={
            "4.4": {"type": "int", "description": "A role.", "values": {"1": "One"}},
            "4.2": {"type": "char", "name": "FakeRoleCode", "values": {"2": "Two"}},
        }
    )
    merged = merged_field(entry.into_fields(("4.4", "4.2")))
    assert merged.name == "FakeRole"
    assert merged.arrow_type == pyarrow.int64(), "the newest version that types it"
    assert json.loads(merged.fix["versions"]) == ["4.4", "4.2"]
    assert json.loads(merged.fix["types"]) == {"4.4": "int", "4.2": "char"}
    assert json.loads(merged.fix["names"]) == {"4.4": "FakeRole", "4.2": "FakeRoleCode"}
    assert json.loads(merged.fix["values"]) == {"1": "One", "2": "Two"}
    assert "tags" not in merged.fix, "the tag never moved, so nothing to say"


def test_a_merged_field_takes_the_newest_correction_of_one_code() -> None:
    """Oldest first, so a newer reading wins without losing a value it dropped."""
    entry = _entry(
        variants={
            "4.4": {"type": "int", "values": {"1": "Corrected"}},
            "4.2": {"type": "int", "values": {"1": "Original", "2": "Dropped"}},
        }
    )
    merged = merged_field(entry.into_fields(("4.4", "4.2")))
    assert json.loads(merged.fix["values"]) == {"1": "Corrected", "2": "Dropped"}


def test_a_merged_field_says_when_a_tag_moved() -> None:
    entry = _entry(variants={"4.4": {"type": "int"}, "4.2": {"type": "int", "tag": 90009}})
    merged = merged_field(entry.into_fields(("4.4", "4.2")))
    assert json.loads(merged.fix["tags"]) == {"4.4": "90001", "4.2": "90009"}


# -- components --------------------------------------------------------------


def _group() -> SpecComponent:
    return SpecComponent(
        "FakeParties",
        (
            SpecGroup(
                "NoFakeParties",
                False,
                90010,
                (
                    SpecFieldRef("FakePartyID", False, 90011),
                    SpecFieldRef("FakePartyRole", False, 90012),
                ),
            ),
        ),
    )


def _narrower() -> SpecComponent:
    return SpecComponent(
        "FakeParties",
        (SpecGroup("NoFakeParties", False, 90010, (SpecFieldRef("FakePartyID", False, 90011),)),),
    )


def test_a_component_entry_reports_each_versions_paths_and_delimiters() -> None:
    """The derived half of a tree, so two readers cannot derive it differently."""
    entry = ComponentEntry.from_components([_group(), _narrower()], ["4.4", "4.2"], [7, 3])
    assert entry.paths("4.4") == {
        "NoFakeParties": (),
        "FakePartyID": ("NoFakeParties",),
        "FakePartyRole": ("NoFakeParties",),
    }
    assert entry.delimiters("4.4") == {("NoFakeParties",): "FakePartyID"}
    assert entry.order("4.4") == 7 and entry.order("4.2") == 3


def test_a_component_entry_diffs_its_versions_against_the_newest() -> None:
    """What one file makes answerable that nine per-version blobs did not."""
    entry = ComponentEntry.from_components([_narrower(), _group()], ["4.4", "4.2"])
    assert entry.diff() == {"4.2": ("FakePartyRole",)}, "4.2 declares one the newest does not"


def test_a_component_entry_round_trips_through_its_document() -> None:
    entry = ComponentEntry.from_components([_group(), _narrower()], ["4.4", "4.2"], [7, 3])
    assert ComponentEntry.from_dict(entry.into_dict()) == entry
    assert entry.into_component("4.4") == _group()
    assert entry.into_component("4.0") is None


def test_a_component_declared_for_no_version_is_refused() -> None:
    with pytest.raises(ValueError, match="declared for no version"):
        ComponentEntry(name="FakeParties")
    with pytest.raises(ValueError, match="has no name"):
        ComponentEntry(name="", variants={"4.4": {"members": []}})


# -- building an entry out of per-version readings ---------------------------


def test_an_entry_is_built_from_the_same_field_read_from_several_versions() -> None:
    members = [
        fix_field("FakeRole", 90001, "int", version="4.4"),
        fix_field("FakeRoleCode", 90001, "char", version="4.2"),
    ]
    entry = FieldEntry.from_fields(members, ["4.4", "4.2"])
    assert entry.name == "FakeRole" and entry.tag == 90001 and entry.kind == STANDARD
    assert entry.into_fields(("4.4", "4.2")) == members, "and reads back as what it was built from"


def test_an_entry_needs_at_least_one_reading() -> None:
    with pytest.raises(ValueError, match="at least one declaration"):
        FieldEntry.from_fields([], [])
    with pytest.raises(ValueError, match="at least one declaration"):
        ComponentEntry.from_components([], [])


def test_a_field_with_no_tag_becomes_a_vendor_entry() -> None:
    member = Field(name="FAKE.CODE", arrow_type=pyarrow.string(), metadata={"fix:type": "String"})
    entry = FieldEntry.from_fields([member], [ANY_VERSION])
    assert entry.kind == NAMESPACE and entry.tag is None
