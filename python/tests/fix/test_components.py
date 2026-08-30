"""Structured extraction of FIX repeating components."""

from __future__ import annotations

import datetime

import pyarrow
import pytest

from rekep.fields import DISPLAY
from rekep.fix.columns import ENTRIES
from rekep.fix.components import (
    LEGS,
    PARTIES,
    SECURITY_ALT_IDS,
    TRD_REG_TIMESTAMPS,
    Leg,
    Legs,
    Parties,
    Party,
    SecurityAltID,
    SecurityAltIDs,
    TrdRegTimestamp,
    TrdRegTimestamps,
)
from rekep.fix.quickfix import block, field_member, group_member, reference_member


def _tags(*rows: object) -> pyarrow.Array:
    """Rows of `(tag, value)` as the `entries` column an extractor is handed."""
    return pyarrow.array(
        [
            None
            if row is None
            else [
                {
                    "tag": tag,
                    "key": str(tag),
                    "value": value,
                }
                for tag, value in row
            ]
            for row in rows
        ],
        type=ENTRIES,
    )


def _pairs(cell: object) -> list[tuple[object, object]] | None:
    if cell is None:
        return None
    return [(entry["tag"], entry["value"]) for entry in cell]


#: The Parties declaration a registry hands the extractor: FIX's own tree, with
#: the `PtysSubGrp` component the newest versions reference inlined where the
#: extractor would resolve it. Every test below is handed this, because the
#: extraction is declaration-driven and there is no other source for one.
PARTIES_SPEC = block(
    "Parties",
    [
        group_member(
            "NoPartyIDs",
            453,
            [
                field_member("PartyID", 448),
                field_member("PartyIDSource", 447),
                field_member("PartyRole", 452),
                group_member(
                    "NoPartySubIDs",
                    802,
                    [
                        field_member("PartySubID", 523),
                        field_member("PartySubIDType", 803),
                    ],
                ),
            ],
        ),
    ],
)


def _parties(**declared: object) -> Parties:
    """The Parties extractor over FIX's own declaration of it."""
    return Parties(components=[PARTIES_SPEC], **declared)


def _stamps(**declared: object) -> TrdRegTimestamps:
    """The TrdRegTimestamps extractor over FIX's own declaration of it."""
    return TrdRegTimestamps(components=[TRD_REG_SPEC], **declared)


def test_party_is_the_exact_fix_named_shape() -> None:
    assert Party.into_field().names == ["partyid", "partyidsource", "partyrole", "buffer"]
    assert Party.into_field().field("partyid").metadata["fix:tag"] == "448"
    assert Party.into_field().field("partyidsource").metadata["fix:tag"] == "447"
    assert Party.into_field().field("partyrole").metadata["fix:tag"] == "452"
    assert Party.into_field().field("partyrole").dtype == pyarrow.int32()
    assert Party.into_field().field("buffer").dtype == pyarrow.map_(
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
                "partyid": "BUYSIDE",
                "partyidsource": "D",
                "partyrole": 1,
                "buffer": None,
            },
            {
                "partyid": "XPAR",
                "partyidsource": "G",
                "partyrole": 17,
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

    assert [party["partyid"] for party in parties.to_pylist()[0]] == ["BUYSIDE", "XPAR"]
    assert [party["partyrole"] for party in parties.to_pylist()[0]] == [1, 17]
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
    component = block(
        "Parties",
        [
            group_member(
                "NoPartyIDs",
                453,
                [
                    field_member("PartyID", 448),
                    field_member("PartyIDSource", 447),
                    field_member("PartyRole", 452),
                    field_member("DeskCode", 9001),
                ],
            ),
        ],
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
                "partyid": "BUYSIDE",
                "partyidsource": None,
                "partyrole": None,
                "buffer": [("PartyRole", "dealer")],
            }
        ]
    ]
    assert residual.to_pylist() == [[]]


def test_a_leading_plus_in_party_role_is_read_as_an_integer() -> None:
    parties, residual = _parties().into_arrow_arrays(
        _tags([(453, "1"), (448, "BUYSIDE"), (452, "+1")])
    )

    assert parties.to_pylist()[0][0]["partyrole"] == 1
    assert residual.to_pylist() == [[]]


def test_an_explicit_43_declaration_keeps_partysubid_flat() -> None:
    component = block(
        "Parties",
        [
            group_member(
                "NoPartyIDs",
                453,
                [
                    field_member("PartyID", 448),
                    field_member("PartyIDSource", 447),
                    field_member("PartyRole", 452),
                    field_member("PartySubID", 523),
                ],
            ),
        ],
    )
    source = _tags([(453, "1"), (448, "BUYSIDE"), (523, "SUB"), (802, "1"), (803, "2")])

    parties, residual = Parties(components=[component]).into_arrow_arrays(source)

    assert parties.to_pylist()[0][0]["buffer"] == [("PartySubID", "SUB")]
    assert _pairs(residual.to_pylist()[0]) == [(802, "1"), (803, "2")]


def test_unrelated_transport_components_do_not_disable_parties() -> None:
    unrelated = block("StandardHeader", [field_member("BeginString", 8, required=True)])
    parties, residual = Parties(components=[unrelated, PARTIES_SPEC]).into_arrow_arrays(
        _tags([(453, "1"), (448, "BUYSIDE"), (452, "1")])
    )

    assert parties.to_pylist()[0][0]["partyid"] == "BUYSIDE"
    assert residual.to_pylist() == [[]]


def test_chunk_boundaries_do_not_change_the_answer() -> None:
    first = _tags([(453, "1"), (448, "A")])
    second = _tags(None, [(453, "0")])
    whole = pyarrow.concat_arrays([first, second])
    chunked = pyarrow.chunked_array([first, second], type=ENTRIES)

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

    assert [[party["partyid"] for party in row] for row in parties.to_pylist()] == [
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

TRD_REG_SPEC = block(
    "TrdRegTimestamps",
    [
        group_member(
            "NoTrdRegTimestamps",
            768,
            [
                field_member("TrdRegTimestamp", 769),
                field_member("TrdRegTimestampType", 770),
                field_member("TrdRegTimestampOrigin", 771),
            ],
        ),
    ],
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
    # Its own projection: the delimiter lands in `TrdRegTimestamp` and the
    # two members it declares in their own columns, with nothing left over.
    assert [entry["trdregtimestamp"].isoformat()[:19] for entry in entries] == [
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:01",
    ]
    assert [entry["trdregtimestamptype"] for entry in entries] == [1, 2]
    assert [entry["trdregtimestamporigin"] for entry in entries] == ["FAKE-ORIGIN", None]
    assert [entry["buffer"] for entry in entries] == [None, None]
    assert _pairs(rest.to_pylist()[0]) == [
        (8, "FIX.4.4"),
        (55, "SYM-TEST"),
    ], "and nothing else moved"


def test_a_group_whose_entries_open_with_something_else_splits_there() -> None:
    """The delimiter is read off the declaration, not named `PartyID` in code."""
    reordered = block(
        "TrdRegTimestamps",
        [
            group_member(
                "NoTrdRegTimestamps",
                768,
                [
                    field_member("TrdRegTimestampType", 770),
                    field_member("TrdRegTimestamp", 769),
                ],
            ),
        ],
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
    assert [entry["trdregtimestamp"] for entry in entries] == [None, None], "770 is no stamp"
    assert [entry["trdregtimestamptype"] for entry in entries] == [1, 2]


def test_naming_a_component_the_registry_does_not_have_extracts_nothing() -> None:
    extractor = Parties(components=[TRD_REG_SPEC], component="NoSuchGroup", group="NoSuch")
    assert extractor._declaration == (set(), {}, {}, {})
    assert extractor.into_arrow_arrays(_tags([(768, "1"), (769, "x")]))[0].to_pylist() == [None]


# -- another component, extracted by the same machine -------------------------


def test_a_regulatory_stamp_is_the_exact_fix_named_shape() -> None:
    names = TrdRegTimestamp.into_field().names
    assert names == [
        "trdregtimestamp",
        "trdregtimestamptype",
        "trdregtimestamporigin",
        "buffer",
    ]
    field = TrdRegTimestamp.into_field()
    assert field.field("trdregtimestamp").metadata["fix:tag"] == "769"
    assert field.field("trdregtimestamptype").metadata["fix:tag"] == "770"
    assert field.field("trdregtimestamporigin").metadata["fix:tag"] == "771"


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
    assert [entry["trdregtimestamptype"] for entry in entries] == [1, 2]
    assert [entry["trdregtimestamporigin"] for entry in entries] == ["venue", None]
    assert entries[0]["trdregtimestamp"].isoformat().startswith("2026-08-14T09:30:00.123")
    assert _pairs(residual.to_pylist()[0]) == [(35, "8"), (10, "000")]


def test_a_malformed_stamp_is_kept_as_text_rather_than_becoming_null() -> None:
    """A null nobody can explain is worse than the value that actually arrived."""
    source = _tags([(768, "1"), (769, "not-a-stamp"), (770, "1")])
    stamps, residual = _stamps().into_arrow_arrays(source)
    (entry,) = stamps.to_pylist()[0]
    assert entry["trdregtimestamp"] is None
    assert entry["trdregtimestamptype"] == 1
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


# -- the instrument's two groups, extracted by the same machine ---------------
#
# Alt-ids and legs mirror the packaged declaration's shape: `SecAltIDGrp`
# declares its fields inline, while every leg group wraps the shared
# `InstrumentLeg` component -- and the order/trade variants add context
# members of their own, which must stay known members rather than break an
# entry.

SEC_ALT_SPEC = block(
    "SecAltIDGrp",
    [
        group_member(
            "NoSecurityAltID",
            454,
            [
                field_member("SecurityAltID", 455),
                field_member("SecurityAltIDSource", 456),
            ],
        ),
    ],
)

INSTRUMENT_LEG_SPEC = block(
    "InstrumentLeg",
    [
        field_member("LegSymbol", 600),
        field_member("LegSecurityID", 602),
        field_member("LegSecurityIDSource", 603),
        field_member("LegCFICode", 608),
        field_member("LegMaturityDate", 611),
        field_member("LegStrikePrice", 612),
        field_member("LegCurrency", 556),
        field_member("LegSide", 624),
        field_member("LegRatioQty", 623),
    ],
)

LEGS_SPEC = block(
    "InstrmtLegGrp",
    [group_member("NoLegs", 555, [reference_member("InstrumentLeg")])],
)

TRD_LEGS_SPEC = block(
    "TrdInstrmtLegGrp",
    [
        group_member(
            "NoLegs",
            555,
            [
                reference_member("InstrumentLeg"),
                field_member("LegQty", 687),
            ],
        ),
    ],
)


def _alt_ids(**declared: object) -> SecurityAltIDs:
    """The SecAltIDGrp extractor over FIX's own declaration of it."""
    return SecurityAltIDs(components=[SEC_ALT_SPEC], **declared)


def _legs(**declared: object) -> Legs:
    """The legs extractor over the wrapper, the variant and the shared leg."""
    return Legs(components=[LEGS_SPEC, TRD_LEGS_SPEC, INSTRUMENT_LEG_SPEC], **declared)


def test_an_alternative_identifier_is_the_exact_fix_named_shape() -> None:
    field = SecurityAltID.into_field()
    assert field.names == ["securityaltid", "securityaltidsource", "buffer"]
    assert field.field("securityaltid").metadata["fix:tag"] == "455"
    assert field.field("securityaltidsource").metadata["fix:tag"] == "456"
    assert SECURITY_ALT_IDS.value_field.nullable is False


def test_a_leg_is_the_exact_fix_named_shape() -> None:
    field = Leg.into_field()
    assert field.names == [
        "symbol",
        "securityid",
        "securityidsource",
        "securitytype",
        "cficode",
        "securityexchange",
        "maturitydate",
        "maturitymonthyear",
        "strikeprice",
        "putorcall",
        "contractmultiplier",
        "currency",
        "side",
        "ratioqty",
        "buffer",
    ]
    assert field.field("symbol").metadata["fix:tag"] == "600", (
        "the generic name, and the leg's own tag under it"
    )
    assert field.field("symbol").metadata[DISPLAY] == "LegSymbol"
    assert field.field("maturitydate").dtype == pyarrow.timestamp("us"), (
        "a LocalMktDate is the instant the day begins, in a zone the message never names"
    )
    assert field.field("ratioqty").dtype == pyarrow.float64()
    assert field.field("putorcall").dtype == pyarrow.int32()
    assert LEGS.value_field.nullable is False


def test_counted_alternative_identifiers_are_lifted_in_wire_order() -> None:
    source = _tags(
        [
            (48, "XS123"),
            (22, "4"),
            (454, "2"),
            (455, "US0378331005"),
            (456, "4"),
            (455, "037833100"),
            (456, "1"),
            (55, "AAPL"),
        ]
    )

    found, residual = _alt_ids().into_arrow_arrays(source)

    assert found.to_pylist() == [
        [
            {"securityaltid": "US0378331005", "securityaltidsource": "4", "buffer": None},
            {"securityaltid": "037833100", "securityaltidsource": "1", "buffer": None},
        ]
    ]
    assert _pairs(residual.to_pylist()[0]) == [(48, "XS123"), (22, "4"), (55, "AAPL")]


def test_legs_resolve_through_the_shared_instrument_leg_component() -> None:
    """The delimiter comes off `InstrumentLeg`'s first field, behind the ref."""
    assert sorted(_legs()._declaration[0]) == [555]
    assert _legs()._declaration[3][()] == {600}
    source = _tags(
        [
            (55, "SPREAD"),
            (555, "2"),
            (600, "AAPL"),
            (624, "1"),
            (623, "1"),
            (611, "20270115"),
            (612, "150.5"),
            (600, "MSFT"),
            (624, "2"),
            (623, "2"),
            (556, "USD"),
            (10, "000"),
        ]
    )

    legs, residual = _legs().into_arrow_arrays(source)

    first, second = legs.to_pylist()[0]
    assert (first["symbol"], first["side"], first["ratioqty"]) == ("AAPL", "1", 1.0)
    assert first["maturitydate"] == datetime.datetime(2027, 1, 15)
    assert first["strikeprice"] == 150.5
    assert (second["symbol"], second["side"], second["currency"]) == ("MSFT", "2", "USD")
    assert _pairs(residual.to_pylist()[0]) == [(55, "SPREAD"), (10, "000")]


def test_a_variant_groups_context_member_lands_in_buffer() -> None:
    """`LegQty <687>` is `TrdInstrmtLegGrp`'s, not a column -- and not a break."""
    source = _tags([(555, "1"), (600, "AAPL"), (687, "9"), (623, "1")])

    legs, residual = _legs().into_arrow_arrays(source)

    (entry,) = legs.to_pylist()[0]
    assert entry["symbol"] == "AAPL"
    assert entry["ratioqty"] == 1.0
    assert dict(entry["buffer"]) == {"LegQty": "9"}
    assert _pairs(residual.to_pylist()[0]) == []


def test_a_malformed_leg_maturity_is_kept_as_text_rather_than_null() -> None:
    source = _tags([(555, "1"), (600, "AAPL"), (611, "Jan 15 2027")])

    legs, residual = _legs().into_arrow_arrays(source)

    (entry,) = legs.to_pylist()[0]
    assert entry["maturitydate"] is None
    assert dict(entry["buffer"]) == {"LegMaturityDate": "Jan 15 2027"}
    assert _pairs(residual.to_pylist()[0]) == []


def test_alt_ids_and_legs_run_in_sequence_over_one_message() -> None:
    source = _tags(
        [
            (454, "1"),
            (455, "US0378331005"),
            (456, "4"),
            (555, "1"),
            (600, "AAPL"),
            (624, "1"),
        ]
    )

    altids, rest = _alt_ids().into_arrow_arrays(source)
    legs, rest = _legs().into_arrow_arrays(rest)

    assert altids.type == SECURITY_ALT_IDS and legs.type == LEGS
    assert [entry["securityaltid"] for entry in altids.to_pylist()[0]] == ["US0378331005"]
    assert [entry["symbol"] for entry in legs.to_pylist()[0]] == ["AAPL"]
    assert _pairs(rest.to_pylist()[0]) == []


# -- scoped extraction: an entry's group is not the message's -----------------
#
# The dictionary nests the instrument inside market-data and quote entries, so
# a `NoSecurityAltID`/`NoLegs` opening after such a count belongs to one entry
# -- and the scoped extractors leave it residual for the per-entry readers,
# where the regulatory components keep hoisting deliberately.

MD_ENTRIES_SPEC = block(
    "MDIncGrp",
    [
        group_member(
            "NoMDEntries",
            268,
            [
                field_member("MDUpdateAction", 279),
                field_member("MDEntryType", 269),
                reference_member("SecAltIDGrp"),
                reference_member("InstrmtLegGrp"),
                field_member("MDEntryPx", 270),
            ],
        ),
    ],
)

UNDERLYINGS_SPEC = block(
    "UndInstrmtGrp",
    [group_member("NoUnderlyings", 711, [field_member("UnderlyingSymbol", 311)])],
)


def _scoped_legs(**declared: object) -> Legs:
    """The legs extractor, aware of the groups whose entries can own one."""
    return Legs(
        components=[
            LEGS_SPEC,
            TRD_LEGS_SPEC,
            INSTRUMENT_LEG_SPEC,
            MD_ENTRIES_SPEC,
            UNDERLYINGS_SPEC,
        ],
        **declared,
    )


def test_the_enclosing_counts_come_out_of_the_declaration_forest() -> None:
    assert _scoped_legs()._enclosing_array.to_pylist() == [268]
    assert _legs()._enclosing_array.to_pylist() == [], "no declared owner, nothing to guard"


def test_a_scoped_group_inside_another_groups_entry_stays_residual() -> None:
    source = _tags(
        [(268, "1"), (279, "0"), (269, "0"), (555, "1"), (600, "AAPL"), (623, "1"), (270, "99")]
    )

    legs, residual = _scoped_legs().into_arrow_arrays(source)

    assert legs.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_a_scoped_group_without_its_count_is_still_the_entrys() -> None:
    # The inferred (count-free) reading refuses the same way the counted does.
    source = _tags([(268, "1"), (279, "0"), (600, "AAPL"), (623, "1")])

    legs, residual = _scoped_legs().into_arrow_arrays(source)

    assert legs.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_a_scoped_group_opening_before_the_entries_is_the_messages() -> None:
    source = _tags([(555, "1"), (600, "AAPL"), (623, "1"), (268, "1"), (279, "0"), (270, "100")])

    legs, residual = _scoped_legs().into_arrow_arrays(source)

    assert [entry["symbol"] for entry in legs.to_pylist()[0]] == ["AAPL"]
    assert _pairs(residual.to_pylist()[0]) == [(268, "1"), (279, "0"), (270, "100")]


def test_a_count_whose_entries_cannot_own_the_group_does_not_refuse_it() -> None:
    """Underlyings carry their own alt-id tags, so `711` is no owner of `555`."""
    source = _tags([(711, "1"), (311, "WTI"), (555, "1"), (600, "AAPL"), (623, "1")])

    legs, residual = _scoped_legs().into_arrow_arrays(source)

    assert [entry["symbol"] for entry in legs.to_pylist()[0]] == ["AAPL"]
    assert _pairs(residual.to_pylist()[0]) == [(711, "1"), (311, "WTI")]


def test_two_leg_blocks_stay_residual_without_owner_context() -> None:
    """A TradeCaptureReport can carry `555` at two levels; neither is guessed."""
    source = _tags(
        [
            (555, "2"),
            (600, "AAPL"),
            (623, "1"),
            (600, "MSFT"),
            (623, "2"),
            (552, "1"),
            (54, "1"),
            (555, "1"),
            (600, "XYZ"),
            (623, "9"),
        ]
    )

    legs, residual = _scoped_legs().into_arrow_arrays(source)

    assert legs.to_pylist() == [None]
    assert residual.to_pylist() == source.to_pylist()


def test_a_stamp_the_reader_range_checks_away_stays_buffered_text() -> None:
    """The readable gate answers with the cast, not with the pattern: a zone
    no calendar has matches the widened shape, and text the reader nulls must
    land in `buffer` rather than becoming a projected null nobody can explain."""
    source = _tags([(768, "1"), (769, "20260814-09:29:58-9945"), (770, "1")])

    stamps, residual = _stamps().into_arrow_arrays(source)

    (entry,) = stamps.to_pylist()[0]
    assert entry["trdregtimestamp"] is None
    assert dict(entry["buffer"]) == {"TrdRegTimestamp": "20260814-09:29:58-9945"}
    assert residual.to_pylist() == [[]]
