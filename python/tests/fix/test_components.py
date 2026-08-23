"""Structured extraction of FIX repeating components."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fix.components import PARTIES, Parties, Party
from rekep.fix.quickfix import SpecComponent, SpecFieldRef, SpecGroup

FIX_TAGS = pyarrow.list_(
    pyarrow.field(
        "item",
        pyarrow.struct(
            [
                pyarrow.field("key", pyarrow.int32(), nullable=False),
                pyarrow.field("value", pyarrow.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
)


def _tags(*rows: object) -> pyarrow.Array:
    return pyarrow.array(rows, type=FIX_TAGS)


def _pairs(cell: object) -> list[tuple[object, object]] | None:
    if cell is None:
        return None
    return [(entry["key"], entry["value"]) for entry in cell]


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

    parties, residual = Parties().into_arrow_arrays(source)

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

    parties, residual = Parties().into_arrow_arrays(source)

    assert [party["party_id"] for party in parties.to_pylist()[0]] == ["BUYSIDE", "XPAR"]
    assert [party["party_role"] for party in parties.to_pylist()[0]] == [1, 17]
    assert _pairs(residual.to_pylist()[0]) == [(55, "TTF")]


def test_null_absent_and_explicitly_empty_are_distinct() -> None:
    source = _tags(None, [], [(453, "0"), (55, "TTF")], [(55, "NG")])

    parties, residual = Parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [None, None, [], None]
    assert [_pairs(row) for row in residual.to_pylist()] == [
        None,
        [],
        [(55, "TTF")],
        [(55, "NG")],
    ]


def test_a_zero_count_leaves_stray_party_members_residual() -> None:
    source = _tags([(453, "0"), (447, "D"), (55, "TTF")])

    parties, residual = Parties().into_arrow_arrays(source)

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

    parties, residual = Parties().into_arrow_arrays(source)

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

    parties, _ = Parties().into_arrow_arrays(source)

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

    parties, _ = Parties().into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"][-2:] == [
        ("NoPartySubIDs[0].PartySubIDType", "1"),
        ("NoPartySubIDs[0].PartySubIDType[1]", "2"),
    ]


def test_nested_members_before_their_delimiter_stay_residual() -> None:
    source = _tags([(453, "1"), (448, "BUYSIDE"), (802, "2"), (803, "2"), (803, "3")])

    parties, residual = Parties().into_arrow_arrays(source)

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

    parties, residual = Parties().into_arrow_arrays(source)

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

    parties, residual = Parties().into_arrow_arrays(source)

    assert parties.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_an_invalid_role_is_retained_instead_of_becoming_null_data() -> None:
    source = _tags([(453, "1"), (448, "BUYSIDE"), (452, "dealer")])

    parties, residual = Parties().into_arrow_arrays(source)

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
    parties, residual = Parties().into_arrow_arrays(
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
    parties, residual = Parties(components=[unrelated]).into_arrow_arrays(
        _tags([(453, "1"), (448, "BUYSIDE"), (452, "1")])
    )

    assert parties.to_pylist()[0][0]["party_id"] == "BUYSIDE"
    assert residual.to_pylist() == [[]]


def test_chunk_boundaries_do_not_change_the_answer() -> None:
    first = _tags([(453, "1"), (448, "A")])
    second = _tags(None, [(453, "0")])
    whole = pyarrow.concat_arrays([first, second])
    chunked = pyarrow.chunked_array([first, second], type=FIX_TAGS)

    expected = Parties().into_arrow_arrays(whole)
    actual = Parties().into_arrow_arrays(chunked)

    assert actual[0].combine_chunks().to_pylist() == expected[0].to_pylist()
    assert actual[1].combine_chunks().to_pylist() == expected[1].to_pylist()


def test_a_slice_keeps_group_state_inside_each_message() -> None:
    source = _tags(
        [(453, "1"), (448, "outside")],
        [(453, "1"), (448, "A"), (452, "1"), (55, "X")],
        [(448, "B"), (452, "2"), (55, "Y")],
        [(55, "outside")],
    ).slice(1, 2)

    parties, residual = Parties().into_arrow_arrays(source)

    assert [[party["party_id"] for party in row] for row in parties.to_pylist()] == [
        ["A"],
        ["B"],
    ]
    assert [_pairs(row) for row in residual.to_pylist()] == [[(55, "X")], [(55, "Y")]]
