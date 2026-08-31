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
from collections.abc import Sequence

import pyarrow
import pytest

from rekep.enums import EventType, State
from rekep.fields import Field, encoded_key, encodings_of
from rekep.fields.metadata import values_of
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    STANDARD,
    Alias,
    ComponentRecord,
    FixFieldValue,
    canonical_versions,
    collapsed_record,
    fold,
    merged_record,
    name_of,
    newest_of,
    record_for,
    record_kind,
    refuse_record,
    slug_of,
)
from rekep.fix.fields import fix_field, namespaced_field
from rekep.fix.quickfix import block, field_member, group_member, members_of


def _entry(
    name: str = "FakeRole",
    tag: int | None = 90001,
    versions: Sequence[str] = ("4.2", "4.4"),
    datatype: str = "int",
    description: str = "A role that no standard has.",
    **fixed: object,
) -> Field:
    """A numbered field declared for two versions, unless a test says otherwise.

    Refused on the way out, because a record is only a record if a lookup
    could answer for it -- which is what a store does to a document it reads.
    """
    record = (
        fix_field(name, tag, datatype, description=description)
        if tag is not None
        else namespaced_field(name, datatype, description=description)
    )
    record.fix.versions = versions
    for key, value in fixed.items():
        setattr(record.fix, key, value)
    return refuse_record(record)


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
    [
        ("PartyRole", "partyrole"),
        ("PARTYROLE", "partyrole"),
        (" PartyRole ", "partyrole"),
        ("party_role", "partyrole"),
        ("Party Role", "partyrole"),
        ("party-role", "partyrole"),
    ],
)
def test_a_name_is_matched_folded(spelled: str, folded: str) -> None:
    """The same fold a rendered key gets, so a name resolves here as it does there."""
    assert fold(spelled) == folded


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


def test_a_record_refuses_unreadable_or_unattributed_source_metadata() -> None:
    record = _entry()
    record.fix["sources"] = "not-json"
    with pytest.raises(ValueError, match="invalid source metadata"):
        refuse_record(record)

    with pytest.raises(ValueError, match="primary source first"):
        _entry(source="nanoconda", sources=("onixs",))
    with pytest.raises(ValueError, match="distinct source names"):
        _entry(source="nanoconda", sources=("nanoconda", "nanoconda"))
    with pytest.raises(ValueError, match="unknown source origin"):
        _entry(
            source="nanoconda",
            sources=("nanoconda",),
            origins={"type": "onixs"},
        )


def test_which_kind_a_record_is_is_the_tag_and_nothing_beside_it() -> None:
    """A record used to state its kind as well, and the two could disagree.

    Nothing stores the claim now: having a tag *is* being standard, so the
    two readings are one and a document has one fewer way to be wrong.
    """
    assert record_kind(_entry()) == STANDARD
    assert record_kind(_entry(name="FAKE.CODE", tag=None)) == NAMESPACE
    assert "kind" not in _entry(name="FAKE.CODE", tag=None).fix


# -- what a record answers ---------------------------------------------------


def test_a_record_is_keyed_by_its_tag_and_a_namespaced_one_by_its_name() -> None:
    assert _entry().fix.key == 90001
    assert _entry(name="FAKE.CODE", tag=None).fix.key == "fakecode"


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


def test_an_alias_that_only_restores_separators_adds_nothing() -> None:
    entry = _entry(aliases=[Alias(name="fake_role").into_dict()])
    assert entry.fix.spellings() == ("FakeRole",)


def test_a_version_the_record_never_saw_has_no_declaration() -> None:
    """Absent is not "present and untyped": a caller has to be able to tell."""
    assert record_for(_entry(), "4.0") is None
    assert record_for(_entry(), "4.2").name == "FakeRole", "one reading, for every version"
    assert record_for(_entry(), "4.2").fix["version"] == "4.2", "and it says which was asked"


def test_message_usage_is_published_as_fix_msgtypes_metadata() -> None:
    member = record_for(_entry(msgtypes=("ExecutionReport",)), "4.2")

    assert member is not None
    assert json.loads(member.fix["msgtypes"]) == ["ExecutionReport"]
    assert "used_in" not in member.fix
    assert collapsed_record([member], ["4.2"]).fix.msgtypes == ("ExecutionReport",)


def test_a_wildcard_record_answers_for_every_version() -> None:
    """What a field outside the standard has: one reading, whatever was negotiated."""
    entry = _entry(name="FAKE.CODE", tag=None, versions=(ANY_VERSION,))
    assert record_for(entry, "4.0") is not None
    assert record_for(entry, "5.0.SP2").fix["version"] == "5.0.SP2"
    assert "tag" not in record_for(entry, "4.4").fix, "no tag, rather than tag zero"
    assert record_kind(record_for(entry, "4.4")) == NAMESPACE


def test_a_declared_column_travels_with_the_field() -> None:
    entry = _entry(name="FAKE.CODE", tag=None, versions=(ANY_VERSION,), column="fake")
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
        enumerated=[
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


def test_a_value_resolves_the_standard_s_parenthesized_abbreviation() -> None:
    entry = _entry(
        enumerated={
            "6": "Good Till Date (GTD)",
            "B": "Broken date; SettlDate (64) is required",
            "S": "Swap Value Factor (SVP) through a central counterparty (CCP)",
        }
    )

    assert entry.fix.encode("gtd") == "6"
    assert entry.fix.encode("64") == "64", "a referenced tag is not a value spelling"
    assert entry.fix.encode("svp") == "S"
    assert entry.fix.encode("ccp") == "ccp", "context is not the value's abbreviation"


def test_a_spelling_two_values_share_is_emitted_for_neither() -> None:
    """An ambiguous translation that picks one silently is worse than none."""
    found, collisions = encodings_of(values_of({"1": "Cross", "2": "cross!"}))
    assert "cross" not in found
    assert collisions == {"cross": ("1", "2")}
    assert _entry(enumerated={"1": "Cross", "2": "cross!"}).fix.encode("Cross") == "Cross"


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
    entry = _entry(enumerated=[FixFieldValue(value="1", meaning="Buy", aliases=("achat",))])
    assert entry.fix.encode("achat") == "1"
    assert entry.fix.encode("Buy") == "1", "and the dictionary's own are still there"
    restored = Field.from_dict(entry.into_dict())
    assert restored.fix.encode("achat") == "1"
    assert restored.fix.value_of("1").aliases == ("achat",), "the spelling survives on the value"


def test_msg_type_event_kinds_round_trip_through_the_record_and_field() -> None:
    entry = _entry(
        name="MsgType",
        tag=35,
        enumerated={"0": "Heartbeat", "D": "NewOrderSingle"},
        event_types={"D": EventType.ORDER},
    )

    restored = Field.from_dict(entry.into_dict())
    assert restored.fix.event_types == {"D": EventType.ORDER}
    assert json.loads(merged_record(restored).fix["event_types"]) == {"D": "ORDER"}
    assert restored.fix.event_type("D") is EventType.ORDER
    assert restored.fix.event_type("0") is EventType.MISC, "known FIX traffic, but not market data"
    assert restored.fix.event_type("U1") is EventType.UNKNOWN, "not declared by this registry"


def test_market_configuration_round_trips_through_field_metadata() -> None:
    entry = _entry(
        name="MsgType",
        tag=35,
        enumerated={"D": "NewOrderSingle"},
        event_types={"D": "ORDER"},
        states={"D": State.PENDING_NEW},
    )

    restored = Field.from_dict(entry.into_dict())
    metadata = merged_record(restored).fix

    assert restored.fix.event_types == {"D": EventType.ORDER}
    assert restored.fix.states == {"D": State.PENDING_NEW}
    assert restored.fix.encode("new_order_single") == "D"
    assert restored.fix.encode("NewOrderSingle") == "D", "however the caller spells it"
    assert "handlers" not in restored.into_dict()
    assert json.loads(restored.into_dict()["fix"]["event_types"]) == {"D": "ORDER"}
    assert json.loads(restored.into_dict()["fix"]["states"]) == {"D": "PENDING_NEW"}
    assert json.loads(metadata["states"]) == {"D": "PENDING_NEW"}


def test_an_id_no_event_kind_has_ever_stored_is_refused() -> None:
    """A dead code must not load as a silently degraded member."""
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": 999})


def test_event_kinds_only_belong_to_msg_type_and_must_name_a_stable_code() -> None:
    with pytest.raises(ValueError, match="belong to MsgType"):
        _entry(event_types={"D": EventType.ORDER})
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": "invented"})


@pytest.mark.parametrize(
    "value",
    ({"name": "ORDER", "id": 5715705941605744640}, ["ORDER"], 110.9, "", None),
)
def test_a_stored_enum_is_a_name_or_a_code_and_never_a_document(value: object) -> None:
    """A member used to be stored as `{name, id}` and is now the name alone.

    The pair was two spellings of one fact that a hand edit could put out of
    step, and neither half is what the metadata holds: `{"D":"ORDER"}` is the
    document, so anything shaped like the old one is refused rather than
    half-read.
    """
    with pytest.raises(ValueError, match="unknown EventType"):
        _entry(name="MsgType", tag=35, event_types={"D": value})


# -- reading a record back ---------------------------------------------------


def test_a_record_round_trips_through_the_document_it_is_stored_as() -> None:
    entry = _entry(
        aliases=[Alias(name="FAKEROLE", source="pco", occurrences=3).into_dict()],
        enumerated=[FixFieldValue(value="1", meaning="One", aliases=("ONE",))],
        msgtypes=("Execution Report",),
        components=("FakeParties",),
        note="no longer used",
    )
    assert Field.from_dict(entry.into_dict()) == entry


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
    stored = _entry().into_dict()
    assert stored == {
        "name": "FakeRole",
        # The Arrow reading, stored rather than derived from the FIX datatype
        # on every read -- which is what every other declaration in this
        # repository does, and what makes this the same document as they are.
        "type": "int32",
        "nullable": True,
        "description": "A role that no standard has.",
        "fix": {"tag": "90001", "type": "int", "versions": '["4.2","4.4"]'},
    }


# -- the merged reading ------------------------------------------------------


def test_a_merged_declaration_is_the_record_and_the_versions_that_declare_it() -> None:
    entry = _entry(
        enumerated={"1": "One", "2": "Two"}, aliases=[Alias(name="FakeRoleCode").into_dict()]
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
    entry = ComponentRecord.from_components([_narrower(), _group()], ["4.2", "4.4"])
    assert entry.versions == ("4.2", "4.4") and entry.newest == "4.4"
    assert entry.paths() == {
        "NoFakeParties": (),
        "FakePartyID": ("NoFakeParties",),
        "FakePartyRole": ("NoFakeParties",),
    }
    assert entry.delimiters() == {("NoFakeParties",): "FakePartyID"}


def test_a_component_record_round_trips_through_its_document() -> None:
    entry = ComponentRecord.from_components([_narrower(), _group()], ["4.2", "4.4"])
    assert ComponentRecord.from_dict(entry.into_dict()) == entry
    assert entry.into_component("4.4") == _group()
    assert entry.into_component("4.0") is None


def test_a_message_definition_carries_its_msg_type_and_nothing_else_does() -> None:
    """The type rides the declaration's own metadata, absent rather than null."""
    declared = block("FakeOrder", members_of(_group()), "D")
    entry = ComponentRecord.from_components([declared], ["4.4"])
    assert entry.msg_type == "D"
    assert entry.into_dict()["declaration"]["fix"]["msgtype"] == "D"
    reusable = ComponentRecord.from_components([_group()], ["4.4"])
    assert reusable.msg_type == ""
    assert "msgtype" not in reusable.into_dict()["declaration"]["fix"]


def test_a_component_declared_for_no_version_is_refused() -> None:
    with pytest.raises(ValueError, match="declared for no version"):
        ComponentRecord(name="FakeParties")
    with pytest.raises(ValueError, match="has no name"):
        ComponentRecord(name="", versions=("4.4",))


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


def test_a_record_keeps_the_newest_stated_added_version() -> None:
    older = fix_field("FakeRole", 90001, "int", version="4.2")
    older.fix.added = "2.7"
    older.fix.source = "nanoconda"
    older.fix.sources = ("nanoconda",)
    older.fix.origins = {"added": "nanoconda"}
    newer = fix_field("FakeRole", 90001, "int", version="4.4")
    newer.fix.source = "quickfix"
    newer.fix.sources = ("quickfix",)

    entry = collapsed_record([older, newer], ["4.2", "4.4"])

    assert entry.fix.added == "2.7"
    assert entry.fix.source_of("added") == "nanoconda"


def test_a_records_values_are_the_union_with_the_newest_winning_per_key() -> None:
    """A value that only ever existed in 4.2 still parses, and a correction still wins."""
    older = fix_field("FakeRole", 90001, "int", version="4.2", values={"1": "First", "2": "Gone"})
    newer = fix_field("FakeRole", 90001, "int", version="4.4", values={"1": "Corrected"})
    entry = collapsed_record([older, newer], ["4.2", "4.4"])
    assert entry.fix.enumerated == values_of({"1": "Corrected", "2": "Gone"})


def test_a_records_value_parts_keep_the_source_that_supplied_each_half() -> None:
    older = fix_field(
        "FakeRole",
        90001,
        "int",
        version="4.2",
        values=(FixFieldValue("1", "Older meaning", ("OlderName",)),),
    )
    older.fix.source = "onixs"
    older.fix.sources = ("onixs",)
    older.fix.origins = {
        "values": {"1": "onixs"},
        "aliases": {"1": "onixs"},
    }
    newer = fix_field(
        "FakeRole",
        90001,
        "int",
        version="4.4",
        values=(
            FixFieldValue("1", aliases=("NewerName",)),
            FixFieldValue("2", "Newer meaning", ("SecondName",)),
        ),
    )
    newer.fix.source = "nanoconda"
    newer.fix.sources = ("nanoconda",)
    newer.fix.origins = {
        "values": {"2": "nanoconda"},
        "aliases": {"1": "nanoconda", "2": "nanoconda"},
    }

    entry = collapsed_record([older, newer], ["4.2", "4.4"])

    assert entry.fix.origins == {
        "values": {"1": "onixs", "2": "nanoconda"},
        "aliases": {"1": "nanoconda", "2": "nanoconda"},
    }
    assert entry.fix.source_of("values", "1") == "onixs"
    assert entry.fix.source_of("aliases", "1") == "nanoconda"
    assert entry.fix.source_of("values") == "nanoconda"


def test_a_record_needs_at_least_one_reading() -> None:
    with pytest.raises(ValueError, match="at least one declaration"):
        collapsed_record([], [])
    with pytest.raises(ValueError, match="at least one declaration"):
        ComponentRecord.from_components([], [])


def test_a_field_with_no_tag_becomes_a_vendor_record() -> None:
    member = Field(name="FAKE.CODE", dtype=pyarrow.string(), metadata={"fix:type": "String"})
    entry = collapsed_record([member], [ANY_VERSION])
    assert record_kind(entry) == NAMESPACE and entry.fix.tag is None
