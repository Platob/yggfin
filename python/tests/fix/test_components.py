"""Structured extraction of FIX repeating components."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fix.columns import KWARGS
from rekep.fix.components import (
    PARTIES,
    TRD_REG_TIMESTAMPS,
    Parties,
    Party,
    TrdRegTimestamp,
    TrdRegTimestamps,
)
from rekep.fix.quickfix import SpecComponent, SpecFieldRef, SpecGroup


def _tags(*rows: object) -> pyarrow.Array:
    """Rows of `(tag, value)` as the `kwargs` column an extractor is handed."""
    return pyarrow.array(
        [
            None
            if row is None
            else [
                {
                    "tag": tag,
                    "key": str(tag),
                    "value": value,
                    "namespace": None,
                }
                for tag, value in row
            ]
            for row in rows
        ],
        type=KWARGS,
    )


def _pairs(cell: object) -> list[tuple[object, object]] | None:
    if cell is None:
        return None
    return [(entry["tag"], entry["value"]) for entry in cell]


#: The Parties declaration a registry hands the extractor: FIX's own tree, with
#: the `PtysSubGrp` component the newest versions reference inlined where the
#: extractor would resolve it. Every test below is handed this, because the
#: extraction is declaration-driven and there is no other source for one.
PARTIES_SPEC = SpecComponent(
    "Parties",
    (
        SpecGroup(
            "NoPartyIDs",
            False,
            453,
            (
                SpecFieldRef("PartyID", False, 448),
                SpecFieldRef("PartyIDSource", False, 447),
                SpecFieldRef("PartyRole", False, 452),
                SpecGroup(
                    "NoPartySubIDs",
                    False,
                    802,
                    (
                        SpecFieldRef("PartySubID", False, 523),
                        SpecFieldRef("PartySubIDType", False, 803),
                    ),
                ),
            ),
        ),
    ),
)


def _parties(**declared: object) -> Parties:
    """The Parties extractor over FIX's own declaration of it."""
    return Parties(components=[PARTIES_SPEC], **declared)


def _stamps(**declared: object) -> TrdRegTimestamps:
    """The TrdRegTimestamps extractor over FIX's own declaration of it."""
    return TrdRegTimestamps(components=[TRD_REG_SPEC], **declared)


def test_party_is_the_exact_fix_named_shape() -> None:
    assert Party.into_field().names == ["party_id", "party_id_source", "party_role", "buffer"]
    assert Party.into_field().field("party_id").metadata["fix:tag"] == "448"
    assert Party.into_field().field("party_id_source").metadata["fix:tag"] == "447"
    assert Party.into_field().field("party_role").metadata["fix:tag"] == "452"
    assert Party.into_field().field("party_role").arrow_type == pyarrow.int64()
    assert Party.into_field().field("buffer").arrow_type == pyarrow.map_(
        pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
    )
    assert PARTIES.value_field.nullable is False


def test_counted_parties_are_lifted_and_every_other_tag_keeps_its_order() -> None:
    source = _tags(
        [
            (8, "FIX.4.4"),
            (453, "2"),
            (448, "BUYSIDE"),
            (447, "D"),
            (452, "1"),
            (448, "XPAR"),
            (447, "G"),
            (452, "17"),
            (55, "TTF"),
        ]
    )

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [
        [
            {
                "party_id": "BUYSIDE",
                "party_id_source": "D",
                "party_role": 1,
                "buffer": None,
            },
            {
                "party_id": "XPAR",
                "party_id_source": "G",
                "party_role": 17,
                "buffer": None,
            },
        ]
    ]
    assert _pairs(residual.to_pylist()[0]) == [(8, "FIX.4.4"), (55, "TTF")]


def test_a_resolved_rendered_sequence_needs_no_count_field() -> None:
    # A rendered `NoPartyIDs[0]=PartyID=...` group reaches this adapter after
    # name resolution as the same ordered tags, but renderers often omit 453.
    source = _tags(
        [
            (448, "BUYSIDE"),
            (447, "D"),
            (452, "1"),
            (448, "XPAR"),
            (447, "G"),
            (452, "17"),
            (55, "TTF"),
        ]
    )

    parties, residual = _parties().into_arrow_arrays(source)

    assert [party["party_id"] for party in parties.to_pylist()[0]] == ["BUYSIDE", "XPAR"]
    assert [party["party_role"] for party in parties.to_pylist()[0]] == [1, 17]
    assert _pairs(residual.to_pylist()[0]) == [(55, "TTF")]


def test_null_absent_and_explicitly_empty_are_distinct() -> None:
    source = _tags(None, [], [(453, "0"), (55, "TTF")], [(55, "NG")])

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [None, None, [], None]
    assert [_pairs(row) for row in residual.to_pylist()] == [
        None,
        [],
        [(55, "TTF")],
        [(55, "NG")],
    ]


def test_a_zero_count_leaves_stray_party_members_residual() -> None:
    source = _tags([(453, "0"), (447, "D"), (55, "TTF")])

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [[]]
    assert _pairs(residual.to_pylist()[0]) == [(447, "D"), (55, "TTF")]


def test_nested_party_sub_ids_stay_lossless_in_unique_buffer_keys() -> None:
    source = _tags(
        [
            (453, "1"),
            (448, "BUYSIDE"),
            (447, "D"),
            (452, "1"),
            (802, "2"),
            (523, "SUB-A"),
            (803, "1"),
            (523, "SUB-B"),
            (803, "2"),
            (55, "TTF"),
        ]
    )

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"] == [
        ("NoPartySubIDs", "2"),
        ("NoPartySubIDs[0].PartySubID", "SUB-A"),
        ("NoPartySubIDs[0].PartySubIDType", "1"),
        ("NoPartySubIDs[1].PartySubID", "SUB-B"),
        ("NoPartySubIDs[1].PartySubIDType", "2"),
    ]
    assert _pairs(residual.to_pylist()[0]) == [(55, "TTF")]


def test_nested_member_indices_follow_the_group_delimiter() -> None:
    source = _tags(
        [
            (453, "1"),
            (448, "BUYSIDE"),
            (802, "2"),
            (523, "SUB-A"),
            (523, "SUB-B"),
            (803, "2"),
        ]
    )

    parties, _ = _parties().into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"] == [
        ("NoPartySubIDs", "2"),
        ("NoPartySubIDs[0].PartySubID", "SUB-A"),
        ("NoPartySubIDs[1].PartySubID", "SUB-B"),
        ("NoPartySubIDs[1].PartySubIDType", "2"),
    ]


def test_duplicate_nested_members_are_occurrence_qualified() -> None:
    source = _tags(
        [
            (453, "1"),
            (448, "BUYSIDE"),
            (802, "1"),
            (523, "SUB"),
            (803, "1"),
            (803, "2"),
        ]
    )

    parties, _ = _parties().into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"][-2:] == [
        ("NoPartySubIDs[0].PartySubIDType", "1"),
        ("NoPartySubIDs[0].PartySubIDType[1]", "2"),
    ]


def test_nested_members_before_their_delimiter_stay_residual() -> None:
    source = _tags([(453, "1"), (448, "BUYSIDE"), (802, "2"), (803, "2"), (803, "3")])

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"] == [("NoPartySubIDs", "2")]
    assert _pairs(residual.to_pylist()[0]) == [(803, "2"), (803, "3")]


def test_declarations_and_registry_names_extend_party_members() -> None:
    component = SpecComponent(
        "Parties",
        (
            SpecGroup(
                "NoPartyIDs",
                False,
                453,
                (
                    SpecFieldRef("PartyID", False, 448),
                    SpecFieldRef("PartyIDSource", False, 447),
                    SpecFieldRef("PartyRole", False, 452),
                    SpecFieldRef("DeskCode", False, 9001),
                ),
            ),
        ),
    )
    source = _tags([(453, "1"), (448, "BUYSIDE"), (9002, "LDN"), (55, "TTF")])

    parties, residual = Parties(components=[component], names={"DeskCode": 9002}).into_arrow_arrays(
        source
    )

    assert parties.to_pylist()[0][0]["buffer"] == [("DeskCode", "LDN")]
    assert _pairs(residual.to_pylist()[0]) == [(55, "TTF")]


@pytest.mark.parametrize("declared", ["2", "not-a-count"])
def test_a_bad_count_refuses_partial_extraction(declared: str) -> None:
    source = _tags(
        [
            (8, "FIX.4.4"),
            (453, declared),
            (448, "BUYSIDE"),
            (447, "D"),
            (452, "1"),
            (55, "TTF"),
        ]
    )

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_multiple_parties_blocks_stay_residual_without_owner_context() -> None:
    source = _tags(
        [
            (453, "1"),
            (448, "BUY"),
            (452, "1"),
            (453, "1"),
            (448, "SELL"),
            (452, "2"),
        ]
    )

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_an_invalid_role_is_retained_instead_of_becoming_null_data() -> None:
    source = _tags([(453, "1"), (448, "BUYSIDE"), (452, "dealer")])

    parties, residual = _parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [
        [
            {
                "party_id": "BUYSIDE",
                "party_id_source": None,
                "party_role": None,
                "buffer": [("PartyRole", "dealer")],
            }
        ]
    ]
    assert residual.to_pylist() == [[]]


def test_a_leading_plus_in_party_role_is_read_as_an_integer() -> None:
    parties, residual = _parties().into_arrow_arrays(
        _tags([(453, "1"), (448, "BUYSIDE"), (452, "+1")])
    )

    assert parties.to_pylist()[0][0]["party_role"] == 1
    assert residual.to_pylist() == [[]]


def test_an_explicit_43_declaration_keeps_partysubid_flat() -> None:
    component = SpecComponent(
        "Parties",
        (
            SpecGroup(
                "NoPartyIDs",
                False,
                453,
                (
                    SpecFieldRef("PartyID", False, 448),
                    SpecFieldRef("PartyIDSource", False, 447),
                    SpecFieldRef("PartyRole", False, 452),
                    SpecFieldRef("PartySubID", False, 523),
                ),
            ),
        ),
    )
    source = _tags([(453, "1"), (448, "BUYSIDE"), (523, "SUB"), (802, "1"), (803, "2")])

    parties, residual = Parties(components=[component]).into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"] == [("PartySubID", "SUB")]
    assert _pairs(residual.to_pylist()[0]) == [(802, "1"), (803, "2")]


def test_unrelated_transport_components_do_not_disable_parties() -> None:
    unrelated = SpecComponent("StandardHeader", (SpecFieldRef("BeginString", True, 8),))
    parties, residual = Parties(components=[unrelated, PARTIES_SPEC]).into_arrow_arrays(
        _tags([(453, "1"), (448, "BUYSIDE"), (452, "1")])
    )

    assert parties.to_pylist()[0][0]["party_id"] == "BUYSIDE"
    assert residual.to_pylist() == [[]]


def test_chunk_boundaries_do_not_change_the_answer() -> None:
    first = _tags([(453, "1"), (448, "A")])
    second = _tags(None, [(453, "0")])
    whole = pyarrow.concat_arrays([first, second])
    chunked = pyarrow.chunked_array([first, second], type=KWARGS)

    expected = _parties().into_arrow_arrays(whole)
    actual = _parties().into_arrow_arrays(chunked)

    assert actual[0].combine_chunks().to_pylist() == expected[0].to_pylist()
    assert actual[1].combine_chunks().to_pylist() == expected[1].to_pylist()


def test_a_slice_keeps_group_state_inside_each_message() -> None:
    source = _tags(
        [(453, "1"), (448, "outside")],
        [(453, "1"), (448, "A"), (452, "1"), (55, "X")],
        [(448, "B"), (452, "2"), (55, "Y")],
        [(55, "outside")],
    ).slice(1, 2)

    parties, residual = _parties().into_arrow_arrays(source)

    assert [[party["party_id"] for party in row] for row in parties.to_pylist()] == [
        ["A"],
        ["B"],
    ]
    assert [_pairs(row) for row in residual.to_pylist()] == [[(55, "X")], [(55, "Y")]]


# -- and whether any of this is about parties --------------------------------
#
# The extraction was written for Parties and reads like it. What it actually
# needs is a component declaration: which tag counts the entries, which tag
# opens one, which tags may belong to one and which group each sits inside.
# All four come out of the tree, so naming the component, its group and the
# members that earn a column is the whole of what a second extractor is.

TRD_REG_SPEC = SpecComponent(
    "TrdRegTimestamps",
    (
        SpecGroup(
            "NoTrdRegTimestamps",
            False,
            768,
            (
                SpecFieldRef("TrdRegTimestamp", False, 769),
                SpecFieldRef("TrdRegTimestampType", False, 770),
                SpecFieldRef("TrdRegTimestampOrigin", False, 771),
            ),
        ),
    ),
)


def test_the_declaration_a_group_needs_comes_out_of_its_own_tree() -> None:
    """Nothing here is named `party`: the tags are the component's, not this file's."""
    extractor = _stamps()
    counts, members, paths, delimiters = extractor._declaration
    assert sorted(counts) == [768], "the tag that counts the entries"
    assert members == {
        769: "TrdRegTimestamp",
        770: "TrdRegTimestampType",
        771: "TrdRegTimestampOrigin",
    }
    assert paths == dict.fromkeys(members, ())
    assert delimiters == {(): {769}}, "the group's first member opens an entry"


def test_another_group_splits_exactly_as_parties_does() -> None:
    """The state machine is the declaration's, so a second group needs no second one."""
    source = _tags(
        [
            (8, "FIX.4.4"),
            (768, "2"),
            (769, "20260101-00:00:00"),
            (770, "1"),
            (771, "FAKE-ORIGIN"),
            (769, "20260101-00:00:01"),
            (770, "2"),
            (55, "SYM-TEST"),
        ]
    )
    extractor = _stamps()
    found, rest = extractor.into_arrow_arrays(source)
    entries = found.to_pylist()[0]
    assert len(entries) == 2, "the count said two, and two delimiters opened"
    # Its own projection: the delimiter lands in `trd_reg_timestamp` and the
    # two members it declares in their own columns, with nothing left over.
    assert [entry["trd_reg_timestamp"].isoformat()[:19] for entry in entries] == [
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:01",
    ]
    assert [entry["trd_reg_timestamp_type"] for entry in entries] == [1, 2]
    assert [entry["trd_reg_timestamp_origin"] for entry in entries] == ["FAKE-ORIGIN", None]
    assert [entry["buffer"] for entry in entries] == [None, None]
    assert _pairs(rest.to_pylist()[0]) == [
        (8, "FIX.4.4"),
        (55, "SYM-TEST"),
    ], "and nothing else moved"


def test_a_group_whose_entries_open_with_something_else_splits_there() -> None:
    """The delimiter is read off the declaration, not named `PartyID` in code."""
    reordered = SpecComponent(
        "TrdRegTimestamps",
        (
            SpecGroup(
                "NoTrdRegTimestamps",
                False,
                768,
                (
                    SpecFieldRef("TrdRegTimestampType", False, 770),
                    SpecFieldRef("TrdRegTimestamp", False, 769),
                ),
            ),
        ),
    )
    extractor = TrdRegTimestamps(components=[reordered])
    assert extractor._declaration[3] == {(): {770}}
    source = _tags(
        [(768, "2"), (770, "1"), (769, "20260101-00:00:00"), (770, "2"), (769, "20260101-00:00:01")]
    )
    entries = extractor.into_arrow_arrays(source)[0].to_pylist()[0]
    # Two entries, split at 770 -- and the delimiter's own value is `1`/`2`,
    # which the stamp column cannot hold, so that column stays null while the
    # member declared for 770 takes it.
    assert len(entries) == 2
    assert [entry["trd_reg_timestamp"] for entry in entries] == [None, None], "770 is no stamp"
    assert [entry["trd_reg_timestamp_type"] for entry in entries] == [1, 2]


def test_naming_a_component_the_registry_does_not_have_extracts_nothing() -> None:
    extractor = Parties(components=[TRD_REG_SPEC], component="NoSuchGroup", group="NoSuch")
    assert extractor._declaration == (set(), {}, {}, {})
    assert extractor.into_arrow_arrays(_tags([(768, "1"), (769, "x")]))[0].to_pylist() == [None]


# -- another component, extracted by the same machine -------------------------


def test_a_regulatory_stamp_is_the_exact_fix_named_shape() -> None:
    names = TrdRegTimestamp.into_field().names
    assert names == [
        "trd_reg_timestamp",
        "trd_reg_timestamp_type",
        "trd_reg_timestamp_origin",
        "buffer",
    ]
    field = TrdRegTimestamp.into_field()
    assert field.field("trd_reg_timestamp").metadata["fix:tag"] == "769"
    assert field.field("trd_reg_timestamp_type").metadata["fix:tag"] == "770"
    assert field.field("trd_reg_timestamp_origin").metadata["fix:tag"] == "771"


def test_counted_regulatory_stamps_are_lifted_like_parties() -> None:
    """The same state machine, told which component and which group to read."""
    source = _tags(
        [
            (35, "8"),
            (768, "2"),
            (769, "20260814-09:30:00.123"),
            (770, "1"),
            (771, "venue"),
            (769, "20260814-09:30:01.000"),
            (770, "2"),
            (10, "000"),
        ]
    )
    stamps, residual = _stamps().into_arrow_arrays(source)
    assert stamps.type == TRD_REG_TIMESTAMPS
    entries = stamps.to_pylist()[0]
    assert [entry["trd_reg_timestamp_type"] for entry in entries] == [1, 2]
    assert [entry["trd_reg_timestamp_origin"] for entry in entries] == ["venue", None]
    assert entries[0]["trd_reg_timestamp"].isoformat().startswith("2026-08-14T09:30:00.123")
    assert _pairs(residual.to_pylist()[0]) == [(35, "8"), (10, "000")]


def test_a_malformed_stamp_is_kept_as_text_rather_than_becoming_null() -> None:
    """A null nobody can explain is worse than the value that actually arrived."""
    source = _tags([(768, "1"), (769, "not-a-stamp"), (770, "1")])
    stamps, residual = _stamps().into_arrow_arrays(source)
    (entry,) = stamps.to_pylist()[0]
    assert entry["trd_reg_timestamp"] is None
    assert entry["trd_reg_timestamp_type"] == 1
    assert dict(entry["buffer"]) == {"TrdRegTimestamp": "not-a-stamp"}
    assert _pairs(residual.to_pylist()[0]) == []


def test_each_extractor_only_takes_its_own_component() -> None:
    """So the two run in sequence over one message without taking each other's."""
    source = _tags(
        [
            (453, "1"),
            (448, "A"),
            (447, "D"),
            (452, "1"),
            (768, "1"),
            (769, "20260814-09:30:00.123"),
            (770, "1"),
        ]
    )
    parties, rest = _parties().into_arrow_arrays(source)
    stamps, rest = _stamps().into_arrow_arrays(rest)
    assert parties.type == PARTIES and stamps.type == TRD_REG_TIMESTAMPS
    assert len(parties.to_pylist()[0]) == 1
    assert len(stamps.to_pylist()[0]) == 1
    assert _pairs(rest.to_pylist()[0]) == []


def test_a_group_that_is_not_there_is_null_rather_than_empty() -> None:
    source = _tags([(35, "8"), (55, "AAPL")])
    stamps, residual = _stamps().into_arrow_arrays(source)
    assert stamps.to_pylist() == [None]
    assert _pairs(residual.to_pylist()[0]) == [(35, "8"), (55, "AAPL")]
