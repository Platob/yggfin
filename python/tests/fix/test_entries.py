"""One record per identity: what a shard holds, and what it answers about it.

The unit half of the layout. `test_store.py` reads a whole store and
`test_data.py` reads the published one; these hold the record itself to what it
may state, how a version disagreement collapses into it, and how its
encodings are spelled.

Every identity here is synthetic (`FAKE-*`, `Fake*`), because none of this is
about which fields the standard has.
"""

from __future__ import annotations

import json

import pyarrow
import pytest

from rekep.enums import EventType, State
from rekep.fields import Field, encoded_key, encodings_of
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    STANDARD,
    Alias,
    ComponentEntry,
    FixFieldValue,
    canonical_versions,
    collapsed_record,
    fold,
    merged_record,
    name_of,
    newest_of,
    record_document,
    record_for,
    record_kind,
    record_of,
    slug_of,
    values_of,
)
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import block, field_member, group_member, members_of


def _entry(**changed: object) -> Field:
    """A numbered field declared for two versions, unless a test says otherwise."""
    declared: dict[str, object] = {
        "name": "FakeRole",
        "tag": 90001,
        "versions": ("4.2", "4.4"),
        "type": "int",
        "description": "A role that no standard has.",
    }
    return record_of({**declared, **changed})


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
def test_a_component_name_slugs_to_the_file_it_is_stored_in(name: str, slug: str) -> None:
    assert slug_of(name) == slug


def test_a_name_that_slugs_to_nothing_is_refused() -> None:
    """The slug lands in a file name, so it can never be empty or a path."""
    for hostile in ("", "   ", "...", "///"):
        with pytest.raises(ValueError, match="FIX registry entry"):
            slug_of(hostile)


@pytest.mark.parametrize(
    ("label", "name"),
    [
        ("Execution Report <8>", "ExecutionReport"),
        ("Order Cancel/Replace Request (legacy)", "OrderCancelReplaceRequest"),
        ("LastShares prior to FIX 4.3)", "LastShares"),
        ("ListNoOrds)", "ListNoOrds"),
    ],
)
def test_a_registry_label_has_one_annotation_free_identifier(label: str, name: str) -> None:
    assert name_of(label) == name


@pytest.mark.parametrize(
    ("spelled", "folded"),
    [("PartyRole", "partyrole"), ("PARTYROLE", "partyrole"), (" PartyRole ", "partyrole")],
)
def test_a_name_is_matched_folded(spelled: str, folded: str) -> None:
    """The same fold a rendered key gets, so a name resolves here as it does there."""
    assert fold(spelled) == folded


@pytest.mark.parametrize("spelled", ["party_role", "Party Role", "party-role"])
def test_a_separator_is_part_of_a_name_and_not_folded_away(spelled: str) -> None:
    """Dropping them merged identities a store holds apart; a real spelling is an alias."""
    assert fold(spelled) != fold("PartyRole")


# -- which version owns a reading --------------------------------------------


def test_versions_are_stored_oldest_first_with_the_transport_last() -> None:
    """`versions.json`'s declared order, so one sort answers for every record."""
    assert canonical_versions(("FIXT1.1", "5.0.SP2", "4.0", "4.10", "5.0")) == (
        "4.0",
        "4.10",
        "5.0",
        "5.0.SP2",
        "FIXT1.1",
    )


def test_the_transport_never_owns_an_application_fields_reading() -> None:
    """FIXT1.1 carries session fields; it does not redefine what they mean."""
    assert newest_of(("4.4", "FIXT1.1", "5.0")) == "5.0"
    assert newest_of(("FIXT1.1",)) == "FIXT1.1", "unless nothing else declares it"
    assert _entry(versions=("4.4", "FIXT1.1")).fix.newest == "4.4"


# -- what a record refuses ---------------------------------------------------


def test_a_record_no_lookup_could_answer_for_is_refused() -> None:
    with pytest.raises(ValueError, match="has no name"):
        _entry(name="")
    with pytest.raises(ValueError, match="declared for no version"):
        _entry(versions=())
    with pytest.raises(ValueError, match="has no tag"):
        _entry(tag=None)
    with pytest.raises(ValueError, match="unknown FIX field kind"):
        _entry(kind="invented")


def test_a_vendor_field_must_not_claim_a_tag() -> None:
    """The two kinds are what a field *is*, and a tagged vendor field is neither."""
    with pytest.raises(ValueError, match="must not claim tag"):
        _entry(kind=NAMESPACE)
    assert record_kind(_entry(name="FAKE.CODE", tag=None, kind=NAMESPACE)) == NAMESPACE


def test_a_stored_record_may_only_state_what_the_schema_has() -> None:
    """A key the schema does not have is a fact nothing downstream would read."""
    with pytest.raises(ValueError, match="declares unknown"):
        record_of({"name": "FakeRole", "tag": 1, "versions": ["4.4"], "invented": "y"})


# -- what a record answers ---------------------------------------------------


def test_a_record_is_keyed_by_its_tag_and_a_namespaced_one_by_its_name() -> None:
    assert _entry().fix.key == 90001
    assert _entry(name="FAKE.CODE", tag=None, kind=NAMESPACE).fix.key == "fake.code"


def test_a_record_answers_to_its_name_then_to_its_aliases() -> None:
    """Two tiers, and the second is where a spelling only 4.2 used now lives."""
    entry = _entry(
        aliases=[
            Alias(name="FakeRoleCode", source="4.2").into_dict(),
            Alias(name="FakeRolle").into_dict(),
        ]
    )
    assert entry.fix.spellings() == ("FakeRole", "FakeRoleCode", "FakeRolle")


def test_an_alias_that_folds_to_a_name_the_record_has_adds_nothing() -> None:
    """`FAKEROLE` is how a bridge shouts `FakeRole`, and matching folds case."""
    assert _entry(aliases=[Alias(name="FAKEROLE").into_dict()]).fix.spellings() == ("FakeRole",)


def test_an_alias_spelled_with_separators_is_a_spelling_of_its_own() -> None:
    """It is not folded onto the name, so it has to be recorded to be matched."""
    entry = _entry(aliases=[Alias(name="fake_role").into_dict()])
    assert entry.fix.spellings() == ("FakeRole", "fake_role")


def test_a_version_the_record_never_saw_has_no_declaration() -> None:
    """Absent is not "present and untyped": a caller has to be able to tell."""
    assert record_for(_entry(), "4.0") is None
    assert record_for(_entry(), "4.2").name == "FakeRole", "one reading, for every version"
    assert record_for(_entry(), "4.2").fix["version"] == "4.2", "and it says which was asked"


def test_message_usage_is_published_as_fix_msgtypes_metadata() -> None:
    member = record_for(_entry(used_in=("ExecutionReport",)), "4.2")

    assert member is not None
    assert json.loads(member.fix["msgtypes"]) == ["ExecutionReport"]
    assert "used_in" not in member.fix
    assert collapsed_record([member], ["4.2"]).fix.msgtypes == ("ExecutionReport",)


def test_a_wildcard_record_answers_for_every_version() -> None:
    """What a field outside the standard has: one reading, whatever was negotiated."""
    entry = _entry(name="FAKE.CODE", tag=None, kind=NAMESPACE, versions=(ANY_VERSION,))
    assert record_for(entry, "4.0") is not None
    assert record_for(entry, "5.0.SP2").fix["version"] == "5.0.SP2"
    assert "tag" not in record_for(entry, "4.4").fix, "no tag, rather than tag zero"
    assert record_for(entry, "4.4").fix["kind"] == NAMESPACE


def test_a_declared_column_travels_with_the_field() -> None:
    entry = _entry(
        name="FAKE.CODE", tag=None, kind=NAMESPACE, versions=(ANY_VERSION,), column="fake"
    )
    assert record_for(entry, "4.4").fix["column"] == "fake"


# -- codecs ------------------------------------------------------------------


@pytest.mark.parametrize(
    "spelled",
    [
        "ORDER_SUBMISSION_TIME",
        "Order Submission Time",
        "order-submission-time",
        "OrderSubmissionTime",
    ],
)
def test_every_spelling_of_one_value_normalizes_to_one_key(spelled: str) -> None:
    """Casefold alone leaves four keys; dropping what is not a letter or digit leaves one."""
    assert encoded_key(spelled) == "ordersubmissiontime"


def test_a_value_resolves_from_its_prose_its_symbol_or_itself() -> None:
    """One lookup path, not two: `Side=Buy`, `Side=BUY` and `Side=1` all reach `1`."""
    entry = _entry(
        values=[
            FixFieldValue(value="1", meaning="Buy", aliases=("BUY",)),
            FixFieldValue(value="2", meaning="Sell", aliases=("SELL_SHORT",)),
        ],
    )
    assert entry.fix.encode("Buy") == "1"
    assert entry.fix.encode("BUY") == "1"
    assert entry.fix.encode("sell short") == "2", "the symbol, spaced as a person would write it"
    assert entry.fix.encode("1") == "1", "and a raw value maps to itself"
    assert entry.fix.encode("nothing here") == "nothing here", "or falls through untouched"
    assert entry.fix.meaning("1") == "Buy", "and the value itself carries what it means"
    assert entry.fix.meaning("3") is None, "an unknown wire value means nothing here"
    assert not hasattr(entry.fix, "decode"), "there is no reverse: the wire value is the fact"


def test_a_spelling_two_values_share_is_emitted_for_neither() -> None:
    """An ambiguous translation that picks one silently is worse than none."""
    found, collisions = encodings_of(values_of({"1": "Cross", "2": "cross!"}))
    assert "cross" not in found
    assert collisions == {"cross": ("1", "2")}
    assert _entry(values={"1": "Cross", "2": "cross!"}).fix.encode("Cross") == "Cross"


def test_a_key_present_in_one_map_and_absent_from_the_other_is_tolerated() -> None:
    """Tag 770 lists keys 8 to 34 with a symbol and no prose."""
    found, _ = encodings_of(
        (
            FixFieldValue(value="1", meaning="Execution Time", aliases=("EXECUTION_TIME",)),
            FixFieldValue(value="10", aliases=("SUBMITTED",)),
        )
    )
    assert found["executiontime"] == "1"
    assert found["submitted"] == "10"
    assert found["10"] == "10", "and the raw value keys itself even with no prose"


def test_a_recorded_spelling_reaches_its_value_and_survives_a_rebuild() -> None:
    """An estate's own spelling is an alias of the value, beside the dictionary's."""
    entry = _entry(values=[FixFieldValue(value="1", meaning="Buy", aliases=("achat",))])
    assert entry.fix.encode("achat") == "1"
    assert entry.fix.encode("Buy") == "1", "and the dictionary's own are still there"
    restored = record_of(record_document(entry))
    assert restored.fix.encode("achat") == "1"
    assert restored.fix.value_of("1").aliases == ("achat",), "the spelling survives on the value"


def test_msg_type_event_kinds_round_trip_through_the_record_and_field() -> None:
    entry = _entry(
        name="MsgType",
        tag=35,
        values={"0": "Heartbeat", "D": "NewOrderSingle"},
        event_types={"D": EventType.ORDER},
    )

    restored = record_of(record_document(entry))
    assert restored.fix.event_types == {"D": EventType.ORDER}
    assert json.loads(merged_record(restored).fix["event_types"]) == {"D": int(EventType.ORDER)}
    assert restored.fix.event_type("D") is EventType.ORDER
    assert restored.fix.event_type("0") is EventType.MISC, "known FIX traffic, but not market data"
    assert restored.fix.event_type("U1") is EventType.UNKNOWN, "not declared by this registry"


def test_market_configuration_round_trips_through_field_metadata() -> None:
    entry = _entry(
        name="MsgType",
        tag=35,
        values={"D": "NewOrderSingle"},
        event_types={"D": "ORDER"},
        states={"D": State.PENDING_NEW},
    )

    restored = record_of(record_document(entry))
    metadata = merged_record(restored).fix

    assert restored.fix.event_types == {"D": EventType.ORDER}
    assert restored.fix.states == {"D": State.PENDING_NEW}
    assert restored.fix.encode("new_order_single") == "D"
    assert restored.fix.encode("NewOrderSingle") == "D", "however the caller spells it"
    assert "handlers" not in record_document(restored)
    assert record_document(restored)["event_types"] == {
        "D": {"name": "ORDER", "id": int(EventType.ORDER)}
    }
    assert record_document(restored)["states"] == {
        "D": {"name": "PENDING_NEW", "id": int(State.PENDING_NEW)}
    }
    assert json.loads(metadata["states"]) == {"D": int(State.PENDING_NEW)}


def test_an_id_no_event_kind_has_ever_stored_is_refused() -> None:
    """A dead id must not load as a silently degraded member."""
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": 999})
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": {"name": "ORDER", "id": 999}})


def test_event_kinds_only_belong_to_msg_type_and_must_name_a_stable_code() -> None:
    with pytest.raises(ValueError, match="belong to MsgType"):
        _entry(event_types={"D": EventType.ORDER})
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": "invented"})


@pytest.mark.parametrize(
    "value",
    (
        {"name": "ORDER", "id": 110.9},
        {"name": "ORDER", "id": "110"},
        {"name": "", "id": 110},
    ),
)
def test_enum_documents_require_an_exact_string_name_and_integer_id(value: object) -> None:
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": value})


# -- reading a record back ---------------------------------------------------


def test_a_record_round_trips_through_the_document_it_is_stored_as() -> None:
    entry = _entry(
        aliases=[Alias(name="FAKEROLE", source="pco", occurrences=3).into_dict()],
        values=[FixFieldValue(value="1", meaning="One", aliases=("ONE",))],
        used_in=("Execution Report",),
        components=("FakeParties",),
        note="no longer used",
    )
    assert record_of(record_document(entry)) == entry


def test_a_stored_alias_may_be_a_bare_name_or_carry_its_provenance() -> None:
    """Both spellings read; only one is written, and only when there is anything to say."""
    assert Alias.from_dict("FAKEROLE") == Alias(name="FAKEROLE")
    assert Alias.from_dict("FAKEROLE").into_dict() == {"name": "FAKEROLE"}
    carried = Alias(name="FAKEROLE", source="brk", occurrences=41)
    assert Alias.from_dict(carried.into_dict()) == carried
    with pytest.raises(ValueError, match="has no name"):
        Alias(name=" ")


def test_an_empty_part_is_not_written_into_the_document() -> None:
    """A shard is a file people read; empties are noise in a diff."""
    stored = record_document(_entry())
    assert stored == {
        "name": "FakeRole",
        "tag": 90001,
        "type": "int",
        "description": "A role that no standard has.",
        "versions": ["4.2", "4.4"],
    }


# -- the merged reading ------------------------------------------------------


def test_a_merged_declaration_is_the_record_and_the_versions_that_declare_it() -> None:
    entry = _entry(
        values={"1": "One", "2": "Two"}, aliases=[Alias(name="FakeRoleCode").into_dict()]
    )
    merged = merged_record(entry, ("4.4", "4.2", "4.0"))
    assert merged.name == "FakeRole"
    assert merged.dtype == pyarrow.int32()
    assert json.loads(merged.fix["versions"]) == ["4.4", "4.2"], "newest first, and only those"
    assert merged.fix["version"] == "4.4", "the version the reading was taken from"
    assert merged.fix.enumerated == values_of({"1": "One", "2": "Two"})
    assert json.loads(merged.fix["aliases"])[0]["name"] == "FakeRoleCode"


def test_a_merged_declaration_falls_back_to_the_records_own_order() -> None:
    """A caller with no version list still gets the record, not an exception."""
    assert merged_record(_entry()).fix["version"] == "4.2"


# -- components --------------------------------------------------------------


def _group() -> Field:
    """A component holding one repeating group of two fields, as 4.4 declares it."""
    return block(
        "FakeParties",
        [
            group_member(
                "NoFakeParties",
                90010,
                [
                    field_member("FakePartyID", 90011),
                    field_member("FakePartyRole", 90012),
                ],
            )
        ],
    )


def _narrower() -> Field:
    """The same component one member short, as an older version declared it."""
    return block(
        "FakeParties",
        [group_member("NoFakeParties", 90010, [field_member("FakePartyID", 90011)])],
    )


def test_a_component_record_keeps_the_newest_tree_and_reports_its_paths() -> None:
    """The derived half of a tree, so two readers cannot derive it differently."""
    entry = ComponentEntry.from_components([_narrower(), _group()], ["4.2", "4.4"])
    assert entry.versions == ("4.2", "4.4") and entry.newest == "4.4"
    assert entry.paths() == {
        "NoFakeParties": (),
        "FakePartyID": ("NoFakeParties",),
        "FakePartyRole": ("NoFakeParties",),
    }
    assert entry.delimiters() == {("NoFakeParties",): "FakePartyID"}


def test_a_component_record_round_trips_through_its_document() -> None:
    entry = ComponentEntry.from_components([_narrower(), _group()], ["4.2", "4.4"])
    assert ComponentEntry.from_dict(entry.into_dict()) == entry
    assert entry.into_component("4.4") == _group()
    assert entry.into_component("4.0") is None


def test_a_message_definition_carries_its_msg_type_and_nothing_else_does() -> None:
    """The type rides the declaration's own metadata, absent rather than null."""
    declared = block("FakeOrder", members_of(_group()), "D")
    entry = ComponentEntry.from_components([declared], ["4.4"])
    assert entry.msg_type == "D"
    assert entry.into_dict()["declaration"]["fix"]["msgtype"] == "D"
    reusable = ComponentEntry.from_components([_group()], ["4.4"])
    assert reusable.msg_type == ""
    assert "msgtype" not in reusable.into_dict()["declaration"]["fix"]


def test_a_component_declared_for_no_version_is_refused() -> None:
    with pytest.raises(ValueError, match="declared for no version"):
        ComponentEntry(name="FakeParties")
    with pytest.raises(ValueError, match="has no name"):
        ComponentEntry(name="", versions=("4.4",))


# -- building a record out of per-version readings ---------------------------


def test_a_record_is_built_from_the_same_field_read_from_several_versions() -> None:
    """Oldest first, so the newest reading is simply the last one written."""
    members = [
        fix_field("FakeRoleCode", 90001, "char", version="4.2"),
        fix_field("FakeRole", 90001, "int", version="4.4"),
    ]
    entry = collapsed_record(members, ["4.2", "4.4"])
    assert entry.fix.canonical == "FakeRole"
    assert entry.fix.tag == 90001 and record_kind(entry) == STANDARD
    assert entry.fix.type == "int" and entry.fix.versions == ("4.2", "4.4")


def test_a_records_values_are_the_union_with_the_newest_winning_per_key() -> None:
    """A value that only ever existed in 4.2 still parses, and a correction still wins."""
    older = fix_field("FakeRole", 90001, "int", version="4.2", values={"1": "First", "2": "Gone"})
    newer = fix_field("FakeRole", 90001, "int", version="4.4", values={"1": "Corrected"})
    entry = collapsed_record([older, newer], ["4.2", "4.4"])
    assert entry.fix.enumerated == values_of({"1": "Corrected", "2": "Gone"})


def test_a_record_needs_at_least_one_reading() -> None:
    with pytest.raises(ValueError, match="at least one declaration"):
        collapsed_record([], [])
    with pytest.raises(ValueError, match="at least one declaration"):
        ComponentEntry.from_components([], [])


def test_a_field_with_no_tag_becomes_a_vendor_record() -> None:
    member = Field(name="FAKE.CODE", dtype=pyarrow.string(), metadata={"fix:type": "String"})
    entry = collapsed_record([member], [ANY_VERSION])
    assert record_kind(entry) == NAMESPACE and entry.fix.tag is None
