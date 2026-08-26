"""Names to the tags FIX gave them -- and what happens to the names it did not."""

from __future__ import annotations

import pathlib
import re
import warnings
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow
import pytest

import rekep
from rekep.fix import (
    NO_PROTOCOL,
    FixCodec,
    FixRegistry,
    Rule,
    Rules,
    infer_version_from_pairs,
    parse_arrow_array,
)
from rekep.fix.access import FieldAccess
from rekep.fix.columns import (
    COLUMNS,
    COMMON,
    FLAT,
    NAMESPACE_COLUMNS,
    QUOTE,
    SESSION,
    STAMPS,
    TYPES,
)
from rekep.fix.fields import fix_field
from rekep.fix.transcribe import (
    APPLICATION_VERSION_SOURCE,
    BEGIN_STRING_SOURCE,
    NO_SOURCE,
    TagIndex,
    _version_from_evidence,
    _version_key,
)
from rekep.kwargs import KWARG_PARTS, KWARGS

SOH = "\x01"

#: The dictionary this repository publishes, beside `python/`.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

BRIDGE = "toBridge " + "|".join(
    [
        "#ISINCODE=XX0000084733",
        "#CFICODE=FXXXSX",
        "#SYMBOL=TTF",
        "#SIDE=1",
        "#ORDERQTY=1200",
        "#NOPARTYIDS=2",
        "#NOPARTYIDS[0]=" + SOH.join(["PARTYID=BUYSIDE", "PARTYIDSOURCE=D", "PARTYROLE=1"]),
        "#NOPARTYIDS[1]=" + SOH.join(["PARTYID=XPAR", "PARTYIDSOURCE=G", "PARTYROLE=17"]),
        "#UNKNOWNVENUEFIELD=Z9",
    ]
)

#: Derived from the line, then pinned: the bridge message carries this many
#: fields, so a parser that lost one cannot move both sides of a count.
EXPECTED_BRIDGE_PAIRS = 13

#: A wire NewOrderSingle wrapped in prose: a full session layer around the four
#: fields an order is, which is every kind of tag the flat layer lifts.
WIRE = "sending " + "|".join(
    [
        "8=FIX.4.2",
        "9=176",
        "35=D",
        "34=7",
        "49=BUYSIDE",
        "56=XPAR",
        "52=20260814-09:30:00.123",
        "11=ORD-1",
        "55=TTF",
        "54=1",
        "38=1200",
        "43=Y",
        "10=203|",
    ]
)

#: Derived from the line, then pinned: how many of its tags are the session
#: layer and how many are the components an order is made of. Both halves are
#: counted, so a tag that moved from one tuple to the other cannot pass.
EXPECTED_WIRE_SESSION = 9
EXPECTED_WIRE_COMMON = 4

#: `52=20260814-09:30:00.123` as the documented UTC instant.
SENT_AT = datetime(2026, 8, 14, 9, 30, 0, 123000, tzinfo=UTC)


@pytest.fixture
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True))


@pytest.fixture
def pairs() -> pyarrow.Array:
    return parse_arrow_array(pyarrow.array([BRIDGE]))


@pytest.fixture
def wire_tags(codec: FixCodec) -> pyarrow.Array:
    """`WIRE` as `kwargs`, which is what `into_lifted_columns` is handed."""
    return codec.into_kwargs(parse_arrow_array(pyarrow.array([WIRE])), "4.2")


def _pairs(array: pyarrow.Array, row: int = 0) -> list[tuple[object, str]] | None:
    """One map or pair-list cell in the pair form assertions read."""
    cell = array.to_pylist()[row]
    if cell is None:
        return None
    return [
        (entry["key"], entry["value"]) if isinstance(entry, dict) else tuple(entry)
        for entry in cell
    ]


def _kwargs(array: pyarrow.Array, row: int = 0) -> list[tuple[int, str, str]] | None:
    """One `kwargs` cell as the `(tag, key, value)` an assertion reads."""
    cell = array.to_pylist()[row]
    if cell is None:
        return None
    return [(entry["tag"], entry["key"], entry["value"]) for entry in cell]


def _tags(array: pyarrow.Array, row: int = 0) -> list[tuple[int, str]]:
    """The fields of one `kwargs` cell that resolved to a tag."""
    return [(tag, value) for tag, _, value in _kwargs(array, row) or () if tag]


def _named(array: pyarrow.Array, row: int = 0) -> list[tuple[str, str]]:
    """The fields of one `kwargs` cell that resolved to nothing."""
    return [(key, value) for tag, key, value in _kwargs(array, row) or () if not tag]


def _kwargs_array(rows: Sequence[Sequence[tuple[Any, ...]] | None]) -> pyarrow.Array:
    """A `kwargs` column out of `(tag, key, value)` triples, for a test's input."""
    return pyarrow.array(
        [
            None
            if row is None
            else [
                {"tag": tag, "key": key, "value": value, "trans": None, "namespace": None}
                for tag, key, value in row
            ]
            for row in rows
        ],
        type=KWARGS,
    )


def test_the_fixture_is_the_shape_the_tests_assume(
    pairs: pyarrow.Array, wire_tags: pyarrow.Array
) -> None:
    assert len(pairs.to_pylist()[0]) == EXPECTED_BRIDGE_PAIRS
    carried = {tag for tag, _ in _tags(wire_tags)}
    assert len(carried) == EXPECTED_WIRE_SESSION + EXPECTED_WIRE_COMMON
    assert len(carried & {tag for tag, _ in SESSION}) == EXPECTED_WIRE_SESSION
    assert len(carried & {tag for tag, _ in COMMON}) == EXPECTED_WIRE_COMMON
    assert f"52={SENT_AT:%Y%m%d-%H:%M:%S}.{SENT_AT.microsecond // 1000:03d}" in WIRE
    assert int(SENT_AT.timestamp() * 1_000_000) == 1786699800123000


# -- what the dictionary makes of a field ------------------------------------


def test_a_name_the_dictionary_knows_becomes_its_tag(codec: FixCodec, pairs) -> None:
    resolved = codec.into_kwargs(pairs, "4.4")
    assert resolved.type == KWARGS
    found = dict(_tags(resolved))
    assert found[55] == "TTF" and found[54] == "1" and found[38] == "1200"
    assert found[453] == "2"


def test_a_group_keeps_its_meaning_through_order_and_not_through_the_key(
    codec: FixCodec, pairs
) -> None:
    """`453` then the entries flattened is what the wire looks like."""
    resolved = codec.into_kwargs(pairs, "4.4")
    tags = [tag for tag, _ in _tags(resolved)]
    assert tags[tags.index(453) :] == [453, 448, 447, 452, 448, 447, 452]
    values = [value for tag, value in _tags(resolved) if tag == 448]
    assert values == ["BUYSIDE", "XPAR"], "both entries kept, in the order they were sent"


def test_a_name_nothing_resolves_is_kept_and_never_guessed(codec: FixCodec, pairs) -> None:
    """Tag `0`, and the name exactly as the line spelled it."""
    resolved = codec.into_kwargs(pairs, "4.4")
    assert dict(_named(resolved)) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }


def test_every_field_is_one_entry_and_says_what_it_is(codec: FixCodec, pairs) -> None:
    """One column rather than three: what used to be the split is now `tag`."""
    resolved = codec.into_kwargs(pairs, "4.4")
    assert len(_kwargs(resolved)) == EXPECTED_BRIDGE_PAIRS, "nothing dropped, nothing doubled"
    assert len(_tags(resolved)) + len(_named(resolved)) == EXPECTED_BRIDGE_PAIRS


def test_an_unknown_numeric_tag_is_still_a_tag(codec: FixCodec) -> None:
    """A number is a tag whether or not this version wrote it up."""
    pairs = parse_arrow_array(pyarrow.array(["55=TTF|999999999=FAKE-VALUE|"]))
    assert _tags(codec.into_kwargs(pairs, "4.4")) == [(55, "TTF"), (999999999, "FAKE-VALUE")]


def test_a_value_its_field_enumerates_reads_its_meaning(codec: FixCodec) -> None:
    """What the dictionary adds beyond a tag: what the value means.

    Derived through the one accessor rather than stored beside every field --
    it is a fact about the dictionary and the value, not about the row.
    """
    access = FieldAccess.of(codec.registry, "4.4")
    pairs = parse_arrow_array(pyarrow.array(["35=D|54=1|55=TTF|"]))
    stored = codec.into_kwargs(pairs, "4.4").to_pylist()[0]
    assert access.reading(stored, 54).meaning == "Buy"
    assert access.reading(stored, 35).meaning == "NewOrderSingle", (
        "the newest version's spelling of the value"
    )
    assert access.reading(stored, 55).meaning is None, (
        "Symbol enumerates nothing, so there is nothing to say"
    )


def test_a_value_reads_its_meaning_from_the_whole_enumeration(codec: FixCodec) -> None:
    """A field's values are cross-version, so a code reads under any version it has.

    Which is the point of one record per identity: a value that only ever
    existed in 4.2 still parses off a 4.2 line, and a value 5.0 added still
    parses off a line a bridge stamped 4.0 -- rather than reading as nothing
    because the version in the header happened not to list it.
    """
    access = FieldAccess.of(codec.registry, None)
    newer = codec.into_kwargs(parse_arrow_array(pyarrow.array(["54=6|"])), "4.4").to_pylist()[0]
    assert access.reading(newer, 54).meaning == "Sell short exempt"
    older = codec.into_kwargs(parse_arrow_array(pyarrow.array(["54=A|"])), "4.0").to_pylist()[0]
    assert access.reading(older, 54).meaning == "Cross short exempt"
    unknown = codec.into_kwargs(parse_arrow_array(pyarrow.array(["54=ZZ|"])), "4.0").to_pylist()[0]
    assert access.reading(unknown, 54).meaning is None, (
        "and a code no version defines still reads as nothing rather than as a guess"
    )


def test_a_key_is_split_into_its_name_and_where_it_stood(codec: FixCodec) -> None:
    """A vendor prefix and a FIX container are both data, and not the same data."""
    parsed = parse_arrow_array(
        pyarrow.array(["#TECH.CLIENTID=ACCT-TEST-01|#NOPARTYIDS[0].PARTYID=PARTY-TEST-A"]),
        "|",
        named=True,
    )
    found = codec.into_kwargs(parsed, "4.4").to_pylist()[0]
    assert [(entry["namespace"], entry["comp"], entry["key"]) for entry in found] == [
        ("TECH", None, "CLIENTID"),
        (None, "NOPARTYIDS[0]", "PARTYID"),
    ], "`TECH` names nothing this version declares; `NOPARTYIDS` names a group"


def test_the_split_key_still_spells_the_key_the_line_wrote(codec: FixCodec) -> None:
    """Whichever half was filled, joined back to the name, is the rendered key."""
    written = ["TECH.CLIENTID", "NOPARTYIDS[0].PARTYID", "SYMBOL"]
    parsed = parse_arrow_array(
        pyarrow.array(["|".join(f"#{key}=FAKE-{index}" for index, key in enumerate(written))]),
        "|",
        named=True,
    )
    found = codec.into_kwargs(parsed, "4.4").to_pylist()[0]
    rebuilt = [
        ".".join(part for part in (entry["namespace"] or entry["comp"], entry["key"]) if part)
        for entry in found
    ]
    assert rebuilt == written, "the split loses nothing, so a reader can undo it"
    assert all(entry["namespace"] is None or entry["comp"] is None for entry in found), (
        "and a field stood in one place, so at most one of the two is filled"
    )


def test_a_vendor_namespace_does_not_borrow_the_tag_its_tail_names(codec: FixCodec) -> None:
    """`TECH.CLIENTID` is not `ClientID <109>`; `NoPartyIDs[0].PartyID` is `PartyID`.

    What tells them apart is whether the container in front names something
    this version declares -- a component, a group, a field -- and `TECH` names
    nothing.
    """
    parsed = parse_arrow_array(
        pyarrow.array(["#TECH.CLIENTID=ACCT-TEST-01|#NOPARTYIDS[0].PARTYID=PARTY-TEST-A"]),
        "|",
        named=True,
    )
    assert _kwargs(codec.into_kwargs(parsed, "4.4")) == [
        (0, "CLIENTID", "ACCT-TEST-01"),
        (448, "PARTYID", "PARTY-TEST-A"),
    ]


def test_a_promoted_tag_is_only_interpreted_in_a_version_that_declares_it(
    codec: FixCodec,
) -> None:
    pairs = parse_arrow_array(pyarrow.array(["461=FXXXSX|"]))
    columns, residual = codec.into_lifted_columns(codec.into_kwargs(pairs, "4.2"), "4.2")
    assert columns[COLUMNS[461]].to_pylist() == [None]
    assert _tags(residual) == [(461, "FXXXSX")]

    columns, residual = codec.into_lifted_columns(codec.into_kwargs(pairs, "4.4"), "4.4")
    assert columns[COLUMNS[461]].to_pylist() == ["FXXXSX"]
    assert _kwargs(residual) == []


def test_an_unknown_key_can_repeat_without_becoming_a_map(codec: FixCodec) -> None:
    parsed = parse_arrow_array(pyarrow.array(["#VENUEFIELD=one|#VENUEFIELD=two"]), "|", named=True)
    assert _named(codec.into_kwargs(parsed, "4.4")) == [
        ("VENUEFIELD", "one"),
        ("VENUEFIELD", "two"),
    ]


def test_isincode_is_lifted_from_rendered_names_without_losing_repeats(
    codec: FixCodec,
) -> None:
    kwargs = _kwargs_array(
        [
            [(0, "ISINCODE", "XX0000084733"), (0, "OTHER", "x")],
            [(0, "isincode", "A"), (0, "ISINCODE", "B")],
            None,
        ]
    )
    columns, rest = codec.into_lifted_columns(kwargs, "4.4")
    assert columns["ISINCODE"].to_pylist() == ["XX0000084733", None, None]
    assert _named(rest, 0) == [("OTHER", "x")]
    assert _named(rest, 1) == [("isincode", "A"), ("ISINCODE", "B")]
    assert _kwargs(rest, 2) is None


def test_no_version_keeps_every_field_raw(codec: FixCodec) -> None:
    """Nothing resolves and nothing lifts, but the fields are all still there."""
    parsed = parse_arrow_array(pyarrow.array(["#55=TTF|#ISINCODE=XX0000084733|"]), "|", named=True)

    kwargs, columns = codec.into_fixmsg_columns(parsed)

    assert _kwargs(kwargs) == [(55, "55", "TTF"), (0, "ISINCODE", "XX0000084733")]
    assert columns["ISINCODE"].to_pylist() == [None]


def test_wire_tags_resolve_without_any_dictionary_at_all(codec: FixCodec) -> None:
    """A numeric key is already a tag; the dictionary is for the other kind."""
    wire = parse_arrow_array(pyarrow.array(["8=FIX.4.2|35=D|55=TTF|10=203|"]))
    assert [tag for tag, _ in _tags(codec.into_kwargs(wire))] == [8, 35, 55, 10]


def test_a_null_row_stays_null(codec: FixCodec) -> None:
    """ "Not a message" and "a message with nothing in it" stay different facts."""
    both = parse_arrow_array(pyarrow.array([BRIDGE, None]))
    assert codec.into_kwargs(both, "4.4").to_pylist()[1] is None


def test_an_empty_pair_column_keeps_its_type(codec: FixCodec) -> None:
    empty = parse_arrow_array(pyarrow.array([], pyarrow.string()))
    assert codec.into_kwargs(empty).type == KWARGS


def test_a_chunked_column_is_the_same_answer_as_one_chunk(codec: FixCodec) -> None:
    lines = [BRIDGE, "8=FIX.4.2|35=D|10=203|", None]
    whole = parse_arrow_array(pyarrow.array(lines))
    chunked = pyarrow.chunked_array(
        [parse_arrow_array(pyarrow.array(lines[:1])), parse_arrow_array(pyarrow.array(lines[1:]))]
    )
    assert codec.into_kwargs(whole, "4.4").to_pylist() == (
        codec.into_kwargs(chunked, "4.4").combine_chunks().to_pylist()
    )


def test_a_key_too_wide_to_be_a_tag_is_not_read_as_one(codec: FixCodec) -> None:
    """An epoch-millis key is not a FIX tag, and must not overflow into one."""
    pairs = parse_arrow_array(pyarrow.array(["1786665901147=x|55=TTF"]), "|", named=True)
    resolved = codec.into_kwargs(pairs, "4.4")
    assert dict(_tags(resolved)) == {55: "TTF"}
    assert dict(_named(resolved)) == {"1786665901147": "x"}


# -- which version -----------------------------------------------------------


def test_the_message_says_which_version_when_it_carries_one(codec: FixCodec) -> None:
    assert codec.version_of("8=FIX.4.2|35=D|") == ("4.2", BEGIN_STRING_SOURCE)
    assert codec.version_of(f"8=FIXT.1.1{SOH}1128=9{SOH}35=A{SOH}") == (
        "5.0.SP2",
        APPLICATION_VERSION_SOURCE,
    )
    assert codec.version_of(f"8=FIXT.1.1{SOH}35=A{SOH}") == (None, NO_SOURCE)


@pytest.mark.parametrize(
    ("pairs", "expected"),
    [
        ([(8, "FIX.4.4"), (35, "D")], ("4.4", BEGIN_STRING_SOURCE)),
        (
            [(8, "FIXT.1.1"), (1128, "9"), (35, "8")],
            ("5.0.SP2", APPLICATION_VERSION_SOURCE),
        ),
        (
            [("BeginString", "FIXT.1.1"), ("DefaultApplVerID", "8")],
            ("5.0.SP1", APPLICATION_VERSION_SOURCE),
        ),
        ([(8, "FIXT.1.1"), (1128, "unknown"), (1137, "9")], (None, NO_SOURCE)),
        ([(8, "FIX.unknown"), (35, "D")], (None, NO_SOURCE)),
        ([(1128, "9"), (35, "8")], (None, NO_SOURCE)),
    ],
)
def test_scalar_pair_version_inference_matches_message_semantics(
    codec: FixCodec,
    pairs: list[tuple[object, str]],
    expected: tuple[str | None, str],
) -> None:
    assert infer_version_from_pairs(pairs, codec.registry) == expected


def test_scalar_pair_version_inference_stops_at_the_checksum(codec: FixCodec) -> None:
    pairs = [(8, "FIXT.1.1"), (10, "000"), (1128, "9")]
    assert infer_version_from_pairs(pairs, codec.registry) == (None, NO_SOURCE)


def test_kwargs_version_inference_matches_scalar_without_materialising_rows(
    codec: FixCodec, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = [
        (None, None, None),
        (None, None, None),
        (" FIX.4.4 ", None, None),
        ("8=fix.5.0sp2", None, None),
        ("FIXT.1.1", "9", None),
        ("fixt-1-1", None, "8"),
        ("FIXT.1.1", "bogus", "9"),
        (None, "9", None),
        ("FIX.unknown", None, None),
        ("FIXT.1.1", " 5.0.sp1 ", None),
        ("FIXT.1.1", "", "9"),
        ("FIX.4.2", None, None),
    ]
    rows = [
        None,
        [],
        [(8, "8", evidence[2][0])],
        [(None, "BeginString", evidence[3][0])],
        [(8, "8", evidence[4][0]), (1128, "1128", evidence[4][1])],
        [
            (None, "BeginString", evidence[5][0]),
            (None, "DefaultApplVerID", evidence[5][2]),
        ],
        [
            (8, "8", evidence[6][0]),
            (1128, "1128", evidence[6][1]),
            (1137, "1137", evidence[6][2]),
        ],
        [(1128, "1128", evidence[7][1])],
        [(8, "8", evidence[8][0])],
        [
            (None, "BeginString", evidence[9][0]),
            (None, "ApplVerID", evidence[9][1]),
        ],
        [
            (8, "8", evidence[10][0]),
            (1128, "1128", evidence[10][1]),
            (1137, "1137", evidence[10][2]),
        ],
        [
            (8, "8", evidence[11][0]),
            (8, "BeginString", "FIX.4.4"),
        ],
    ]
    expected = [
        _version_from_evidence(*one, codec._spellings)  # noqa: SLF001
        for one in evidence
    ]

    def scalar_path(*args: object, **kwargs: object) -> None:
        raise AssertionError("the batch path called the scalar resolver")

    monkeypatch.setattr("rekep.fix.transcribe._version_from_evidence", scalar_path)
    whole = _kwargs_array(rows)
    chunked = pyarrow.chunked_array([whole.slice(0, 5), whole.slice(5)], type=KWARGS)
    for kwargs in (whole, chunked):
        versions, sources = codec.versions_of_kwargs(kwargs)
        assert list(zip(versions.to_pylist(), sources.to_pylist(), strict=True)) == expected


def test_kwargs_version_inference_keeps_empty_columns_typed(codec: FixCodec) -> None:
    versions, sources = codec.versions_of_kwargs(pyarrow.array([], type=KWARGS))
    assert versions.type == sources.type == pyarrow.string()
    assert len(versions) == len(sources) == 0


def test_a_mixed_column_resolves_each_beginstring(codec: FixCodec) -> None:
    messages = pyarrow.array(
        [
            "recv 8=FIX.4.3|35=D|",
            "8=FIX.5.0SP2|35=8|",
            "8=FIXT.1.1|1128=9|35=8|",
            "8=FIXT.1.1|35=8|",
            "8=FIXT.1.1|35=8|58=text1128=9|",
            "8=FIXT.1.1|35=8|58=text#1128=9|",
            "8=FIXT.1.1|1137=9|35=8|",
            "8=FIXT.1.1|1128=bogus|1137=9|35=8|",
            "8=FIXT.1.1|1128=|1128=9|35=8|",
            "toBridge #A=1|#B=2",
        ]
    )
    assert codec.versions_of(messages, "FIX").to_pylist() == [
        "4.3",
        "5.0.SP2",
        "5.0.SP2",
        None,
        None,
        None,
        "5.0.SP2",
        None,
        "5.0.SP2",
        None,
    ]
    assert [codec.version_of(message, "FIX")[0] for message in messages.to_pylist()] == (
        codec.versions_of(messages, "FIX").to_pylist()
    )


def test_wrapped_fixt_uses_the_protocols_named_application_fields(codec: FixCodec) -> None:
    messages = pyarrow.array(
        [
            "8=FIXT.1.1|35=UL|#APPLVERID=9|#SYMBOL=X",
            "8=FIXT.1.1|35=UL|#1128=9|#SYMBOL=X",
            "#BeginString=fixt.1.1|#ApplVerID=9|#Symbol=X",
        ]
    )

    vector = codec.versions_of(messages, "UL").to_pylist()

    assert vector == ["5.0.SP2", "5.0.SP2", "5.0.SP2"]
    assert [codec.version_of(message, "UL")[0] for message in messages.to_pylist()] == vector


def test_a_named_checksum_ends_version_evidence(codec: FixCodec) -> None:
    messages = pyarrow.array(
        [
            "#BeginString=FIXT.1.1|#CheckSum=000|#ApplVerID=9|#Symbol=X",
            "#BeginString=FIXT.1.1|#Trailer.CheckSum=000|#ApplVerID=9|#Symbol=X",
            "#BeginString=FIXT.1.1|#Trailer.10=000|#ApplVerID=9|#Symbol=X",
        ]
    )
    assert codec.versions_of(messages, "UL").to_pylist() == [None, None, None]
    assert [codec.version_of(message, "UL") for message in messages.to_pylist()] == [
        (None, NO_SOURCE),
        (None, NO_SOURCE),
        (None, NO_SOURCE),
    ]


def test_mixed_case_wire_fixt_matches_the_scalar_reading(codec: FixCodec) -> None:
    messages = pyarrow.array(["8=fixt.1.1|1128=9|35=8|"])
    assert codec.versions_of(messages, "FIX").to_pylist() == ["5.0.SP2"]
    assert codec.version_of(messages[0].as_py(), "FIX")[0] == "5.0.SP2"


def test_automatic_vector_version_detection_categorises_like_the_scalar(codec: FixCodec) -> None:
    messages = pyarrow.array(
        [
            "8=FIX.4.2|35=D|",
            "#BeginString=FIXT.1.1|#ApplVerID=9|#Symbol=X",
            "plain log text",
        ]
    )
    vector = codec.versions_of(messages).to_pylist()
    assert vector == ["4.2", "5.0.SP2", None]
    assert [codec.version_of(message)[0] for message in messages.to_pylist()] == vector


@pytest.mark.parametrize("protocol", ["FIX", "UL"])
def test_an_empty_message_column_has_an_empty_version_column(
    codec: FixCodec, protocol: str
) -> None:
    assert codec.versions_of(pyarrow.array([], pyarrow.string()), protocol).to_pylist() == []


def test_chunked_mixed_separators_match_one_contiguous_column(codec: FixCodec) -> None:
    lines = [
        "8=FIX.4.3|35=D|",
        f"8=FIXT.1.1{SOH}1128=9{SOH}35=8{SOH}",
        "8=FIX.5.0SP1;35=8;",
    ]
    chunked = pyarrow.chunked_array([lines[:2], lines[2:]])

    assert (
        codec.versions_of(chunked, "FIX").to_pylist()
        == codec.versions_of(pyarrow.array(lines), "FIX").to_pylist()
    )


def test_separator_detection_ignores_beginstring_text_inside_another_tag(
    codec: FixCodec,
) -> None:
    messages = pyarrow.array(
        [
            "prefix 18=FIX.9 blah 8=FIX.4.3|35=D|",
            "prefix 18=FIX.9 blah 8=FIX.5.0SP1;35=8;",
        ]
    )

    assert codec.separators_of(messages, False).to_pylist() == ["|", ";"]
    assert codec.versions_of(messages, "FIX").to_pylist() == ["4.3", "5.0.SP1"]


def test_named_beginstrings_and_numeric_bridge_keys_detect_each_rows_separator(
    codec: FixCodec,
) -> None:
    messages = pyarrow.array(
        [
            "8=FIXT.1.1|35=UL|#1128=9|#55=X|",
            "8=FIXT.1.1;35=UL;#1128=8;#55=X;",
            "BeginString=FIXT.1.1|ApplVerID=9|Symbol=X|",
            "BeginString=FIXT.1.1;ApplVerID=8;Symbol=X;",
        ]
    )

    assert codec.separators_of(messages, True).to_pylist() == ["|", ";", "|", ";"]
    expected = ["5.0.SP2", "5.0.SP1", "5.0.SP2", "5.0.SP1"]
    assert codec.versions_of(messages, "UL").to_pylist() == expected
    assert [codec.version_of(message, "UL")[0] for message in messages.to_pylist()] == expected


def test_versionless_parties_stay_raw(codec: FixCodec) -> None:
    bare = FixCodec(registry=codec.registry)
    tags = _kwargs_array([[(453, "453", "1"), (448, "448", "BUYSIDE"), (2376, "2376", "7")]])

    columns, residual = bare.into_component_columns(tags)

    assert columns["Parties"].to_pylist() == [None]
    assert residual is tags


def test_a_beginstring_no_version_answers_raw(codec: FixCodec) -> None:
    """`8=FIX4` is truncated, and the nearest version is a guess."""
    assert codec.version_of("recv 8=FIX4^A35=0^A") == (None, NO_SOURCE)


def test_a_protocol_rule_does_not_supply_a_fix_version(codec: FixCodec) -> None:
    own = FixCodec(
        registry=codec.registry,
        rules=Rules(rules=[Rule(protocol="UL", pattern="#", codec="ul")]),
    )
    assert own.version_of("toBridge #A=1|#B=2", "UL") == (None, NO_SOURCE)
    assert own.versions_of(pyarrow.array(["toBridge #A=1|#B=2"])).to_pylist() == [None]


def test_nobody_saying_which_version_is_an_answer_too() -> None:
    bare = FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True))
    assert bare.version_of("toBridge #A=1|#B=2") == (None, NO_SOURCE)


@pytest.mark.parametrize(
    ("spelled", "expected"),
    [
        ("8=FIX.4.2", "42"),
        ("FIX.4.2", "42"),
        ("4.2", "42"),
        ("FIXT.1.1", "FIXT11"),
        ("FIXT1.1", "FIXT11"),
        ("FIX.5.0SP2", "50SP2"),
        ("5.0.SP2", "50SP2"),
    ],
)
def test_the_two_spellings_of_a_version_key_alike(spelled: str, expected: str) -> None:
    assert _version_key(spelled) == expected


# -- the index ---------------------------------------------------------------


def test_the_index_is_built_once_per_version(codec: FixCodec) -> None:
    """Per batch at the most, never per row -- which is the whole design."""
    first = codec.index_of("4.4")
    assert codec.index_of("4.4") is first
    assert codec.index_of("4.2") is not first


def test_an_empty_index_resolves_nothing() -> None:
    empty = TagIndex.from_tags({})
    resolved = empty.resolve(pyarrow.array(["Symbol", "55"]))
    assert resolved.to_pylist() == [None, 55], "a digit is a tag with or without a dictionary"


def test_a_registry_with_no_cached_version_resolves_nothing_and_raises_nothing(
    tmp_path: Path,
) -> None:
    """A cold cache loses the tags, never the capture."""
    cold = FixCodec(registry=FixRegistry(cache_dir=tmp_path, offline=True))
    pairs = parse_arrow_array(pyarrow.array([BRIDGE]))
    resolved = cold.into_kwargs(pairs)
    assert _tags(resolved) == [], "no dictionary, so nothing resolves"
    assert len(_named(resolved)) == EXPECTED_BRIDGE_PAIRS, "and every field is still there"
    assert cold.version_of("8=FIX.4.2|") == (None, NO_SOURCE)
    assert cold.tag_field(55, "4.4") is None


def test_an_offline_registry_never_reaches_the_site(tmp_path: Path) -> None:
    """Which is why a codec holds one: a parse must not start a scrape.

    The base URL points at a port nothing listens on, so a registry that did
    reach for it would hang on the retry ladder rather than fail quickly --
    which is exactly the failure this flag exists to make impossible.
    """
    registry = FixRegistry(cache_dir=tmp_path, offline=True, base_url="http://127.0.0.1:9/nope")
    assert registry.versions == (), "what it holds, rather than an error it never earned"
    assert registry.tags() == {}
    assert FixCodec().registry.offline is True


def test_an_offline_registry_still_serves_what_it_stored() -> None:
    """Offline is "do not fetch", not "do not answer"."""
    registry = FixRegistry(cache_dir=DATA, offline=True)
    assert "4.4" in registry.versions
    assert registry.tags("4.4")["symbol"] == 55


# -- values that mean nothing ------------------------------------------------


#: The spellings a renderer reaches for when it has nothing to say, one field
#: each, plus two fields that do have a value. ` NULL ` is the renderer's
#: padding and its casing, not a value -- and the two halves are pinned from
#: two arrivals, because `parse_arrow_array` strips a value on the way in:
#: through `into_pairs` only the casing is left to catch, and the trim in
#: `drop_null_values` is what answers for a map the parser did not build.
ABSENT = "|".join(
    [
        "#SYMBOL=TTF",
        "#SIDE=",
        "#ACCOUNT=<null>",
        "#CLIENTID=N/A",
        "#SECURITYEXCHANGE=null",
        "#TEXT= NULL ",
        "#ORDERQTY=1200",
    ]
)


def test_a_value_that_means_nothing_is_dropped(codec: FixCodec) -> None:
    """`ACCOUNT=<null>` is an absent account, not an account called `<null>`.

    Both arrivals, because only the second still carries the padding `ABSENT`
    describes -- and a trim nothing hands a padded value to is a trim no
    failure can reach.
    """
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), "UL")
    assert parsed.to_pylist()[0] == [("SYMBOL", "TTF"), ("ORDERQTY", "1200")]
    padded = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 2], pyarrow.int32()),
        pyarrow.array(["SYMBOL", "TEXT"]),
        pyarrow.array(["TTF", " NULL "]),
    )
    assert codec.drop_null_values(padded).to_pylist()[0] == [("SYMBOL", "TTF")]


def test_an_absent_field_is_stored_nowhere(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), "UL")
    resolved = codec.into_kwargs(parsed, "4.4")
    assert dict(_tags(resolved)) == {55: "TTF", 38: "1200"}
    assert _named(resolved) == []


def test_a_message_whose_every_field_was_absent_is_empty_and_not_null(codec: FixCodec) -> None:
    """It was a message. It said nothing. Those are different facts."""
    parsed = codec.into_pairs(pyarrow.array(["#A=null|#B=<NULL>"]), "UL")
    assert parsed.to_pylist() == [[]]


def test_a_null_row_stays_null_through_the_filter(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT, None]), "UL")
    assert parsed.to_pylist()[1] is None


def test_an_empty_set_keeps_every_pair(codec: FixCodec) -> None:
    """A feed whose `n/a` really is a value says so, and nothing is dropped."""
    keeping = FixCodec(registry=codec.registry, null_values=frozenset())
    parsed = keeping.into_pairs(pyarrow.array([ABSENT]), "UL")
    assert len(parsed.to_pylist()[0]) == ABSENT.count("|") + 1


def test_a_pair_with_no_value_goes_even_when_no_spelling_means_absent(codec: FixCodec) -> None:
    """An empty `null_values` says which *spellings* are values, not that a hole is.

    The map type forbids a missing value outright, so this half of the drop is
    not configuration: a row landing in a store with one would contradict the
    schema it was written under.
    """
    keeping = FixCodec(registry=codec.registry, null_values=frozenset())
    holed = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 3], pyarrow.int32()),
        pyarrow.array(["SYMBOL", "SIDE", "ORDERQTY"]),
        pyarrow.array(["TTF", None, "1200"]),
    )
    kept = keeping.drop_null_values(holed)
    assert kept.to_pylist()[0] == [("SYMBOL", "TTF"), ("ORDERQTY", "1200")]
    assert pyarrow.types.is_map(kept.type), "drop_null_values still serves the parser intermediate"


def test_a_stored_field_names_itself_and_never_nothing() -> None:
    """`tag` and `key` are what every consumer addresses a field by."""
    assert pyarrow.types.is_list(KWARGS)
    assert KWARGS.value_field.nullable is False
    assert KWARGS.value_type.field("tag").nullable is False
    assert KWARGS.value_type.field("key").nullable is False
    assert KWARGS.value_type.field("value").nullable is False
    assert [KWARGS.value_type.field(i).name for i in range(KWARGS.value_type.num_fields)] == [
        "tag",
        "key",
        "value",
        "namespace",
        "comp",
    ]
    assert KWARG_PARTS == ("tag", "key", "value", "namespace", "comp"), (
        "one declaration of the members, read off the type itself"
    )


def test_structuring_a_nullable_pair_fills_its_required_value(codec: FixCodec) -> None:
    pairs = pyarrow.array([[("55", None)]], type=pyarrow.map_(pyarrow.string(), pyarrow.string()))

    stored = codec.into_message_kwargs(pairs)

    values = pyarrow.compute.struct_field(pyarrow.compute.list_flatten(stored), "value")
    assert values.to_pylist() == [""]


def test_the_stored_members_are_declared_once_and_nowhere_else() -> None:
    """Two copies of this tuple is the shape bug that costs a whole column.

    The member list lived in two modules and a member removed from one would
    have left the other writing a struct Arrow refuses. It is read off
    `KWARGS` now, so the type is the only declaration, and this pins that no
    module spells it again.
    """
    package = pathlib.Path(rekep.__file__).parent
    spelled = [
        path.relative_to(package).as_posix()
        for path in package.rglob("*.py")
        if re.search(
            r"""\(\s*["']tag["']\s*,\s*["']key["']\s*,""",
            path.read_text(encoding="utf-8"),
        )
    ]
    assert not spelled, f"the stored members are spelled again in {spelled}"


def test_the_set_is_configuration_and_travels_in_a_document(
    codec: FixCodec, tmp_path: Path
) -> None:
    own = FixCodec(registry=codec.registry, null_values=frozenset({"-", "unset"}))
    parsed = own.into_pairs(pyarrow.array(["#A=-|#B=unset|#C=null|#D=1"]), "UL")
    assert parsed.to_pylist()[0] == [("C", "null"), ("D", "1")]
    path = tmp_path / "codec.yml"
    own.into_yaml(path)
    assert FixCodec.from_yaml(path).null_values == frozenset({"-", "unset"})


def test_a_capture_carrying_no_absent_field_is_retyped_and_not_rebuilt(codec: FixCodec) -> None:
    """The skip is the point: two kernels a batch, and the parser's own buffers.

    It cannot be the same object any more, because the parser's map admits a
    null value and the stored one does not -- but the cast that says so is a
    re-wrap, so a capture with nothing to drop still copies no data.
    """
    parsed = parse_arrow_array(pyarrow.array(["#A=1|#B=2"]))
    kept = codec.drop_null_values(parsed)
    assert pyarrow.types.is_map(kept.type)
    assert kept.to_pylist() == parsed.to_pylist()
    assert [buffer.address for buffer in kept.items.buffers() if buffer is not None] == [
        buffer.address for buffer in parsed.items.buffers() if buffer is not None
    ]


# -- the seam ----------------------------------------------------------------


def test_a_rule_that_reads_nothing_gives_nulls_and_not_empty_maps(codec: FixCodec) -> None:
    messages = pyarrow.array(["prose", "more prose"])
    parsed = codec.into_pairs(messages, NO_PROTOCOL)
    assert parsed.to_pylist() == [None, None]
    assert pyarrow.types.is_map(parsed.type)


def test_the_codec_reads_each_protocol_the_way_its_rule_says(codec: FixCodec) -> None:
    """The name is the whole address: the batch carries it, the rule is ours."""
    for line, expected in ((BRIDGE, "UL"), ("8=FIX.4.2|35=D|10=203|", "FIX")):
        assert Rules.into_default().categorise(line).protocol == expected
        assert codec.into_pairs(pyarrow.array([line]), expected).to_pylist()[0]


def test_the_typed_reading_of_a_value_is_a_cast_against_the_field_that_knows(
    codec: FixCodec,
) -> None:
    """Values stay text here; `tag_field` is where a type comes from."""
    side = codec.tag_field(54, "4.4")
    assert side.name == "Side"
    assert side.arrow_type == pyarrow.string(), "char is a string, as FIX_SCALARS says"
    assert "1" in side.fix["values"]


# -- the flat layer ----------------------------------------------------------


#: A message that repeats a header field, which a malformed sender does.
DUPLICATED = "8=FIX.4.4|49=FIRST|56=XPAR|49=SECOND|55=TTF|10=203|"

#: A multi-leg order: `NoLegs <555>` opened twice, a `LegSymbol <600>` and a
#: `Symbol <55>` inside the entries, and the strategy's own `55` on top. Two
#: tags this layer declares occur more than once, and neither has one value.
LEGS = "|".join(
    [
        "8=FIX.4.4",
        "35=AB",
        "34=8",
        "49=BUYSIDE",
        "56=XPAR",
        "555=2",
        "600=TTF",
        "55=SPREAD",
        "555=2",
        "55=OTHER",
        "10=011|",
    ]
)

#: The same order with nothing repeated -- the row whose `symbol` is not in
#: doubt, and which has to keep it while sharing a batch with `LEGS`.
SINGLE = "|".join(
    [
        "8=FIX.4.4",
        "35=D",
        "34=9",
        "49=BUYSIDE",
        "56=XPAR",
        "55=TTF",
        "54=1",
        "60=20260814-09:29:59.5",
        "10=012|",
    ]
)

#: `60=20260814-09:29:59.5` as the documented UTC instant.
HAPPENED_AT = datetime(2026, 8, 14, 9, 29, 59, 500000, tzinfo=UTC)

#: The one header field that is a repeating group: `NoHops` and two entries.
HOPS = "|".join(
    [
        "8=FIX.4.4",
        "49=RELAY",
        "627=2",
        "628=BRIDGE1",
        "629=20260814-09:30:00",
        "628=BRIDGE2",
        "629=20260814-09:31:00",
        "55=TTF",
        "10=203|",
    ]
)

#: A message made of nothing the flat layer names: a party group and no scalar
#: around it, so the one `is_in` that decides for the batch finds nothing.
UNLIFTABLE = "453=2|448=BUYSIDE|447=D|452=1|"


def test_the_flat_layer_becomes_columns_and_leaves_the_pair_list(
    codec: FixCodec, wire_tags: pyarrow.Array
) -> None:
    """Who sent it, what was traded, at what price is what a reader filters on.

    So each is a column of its own, and it is **removed** from the list rather
    than stored in both places: two copies of one field are two answers as soon
    as anything rewrites either.

    Addressed by tag through `COLUMNS`, because what is under test is that a
    tag's value lands in the column the declaration gives it -- not how that
    column is spelled, which the declaration is the authority on.
    """
    columns, rest = codec.into_lifted_columns(wire_tags, "4.2")
    assert rest.to_pylist()[0] == [], "an order names nothing the flat layer left behind"
    assert rest.type == KWARGS
    filled = {name: column[0].as_py() for name, column in columns.items() if column[0].is_valid}
    assert len(filled) == EXPECTED_WIRE_SESSION + EXPECTED_WIRE_COMMON
    assert filled == {
        COLUMNS[tag]: value
        for tag, value in (
            (8, "FIX.4.2"),
            (9, 176),
            (35, "D"),
            (34, 7),
            (49, "BUYSIDE"),
            (56, "XPAR"),
            (52, SENT_AT),
            (43, True),
            (10, "203"),
            (11, "ORD-1"),
            (55, "TTF"),
            (54, "1"),
            (38, 1200.0),
        )
    }


def test_every_declared_flat_field_comes_back_as_its_own_column(
    codec: FixCodec, wire_tags: pyarrow.Array
) -> None:
    """One column per declared tag, typed from the registry declaration."""
    columns, _ = codec.into_lifted_columns(wire_tags, "4.2")
    assert (len(SESSION), len(COMMON), len(QUOTE)) == (33, 26, 18), (
        "the session layer, shared components, then quote fields"
    )
    assert len(FLAT) == 77
    namespaced = {field.name for field in NAMESPACE_COLUMNS.values()}
    assert set(columns) == set(COLUMNS.values()) | namespaced, (
        "one pass lifts both kinds, so it answers with both"
    )
    assert sorted(STAMPS) == [52, 60, 62, 122, 370], (
        "SendingTime, TransactTime, ValidUntilTime, OrigSendingTime, OnBehalfOfSendingTime"
    )
    assert {name: column.type for name, column in columns.items()} == {
        name: TYPES[tag] for tag, name in COLUMNS.items()
    } | {field.name: field.arrow_type for field in NAMESPACE_COLUMNS.values()}
    assert columns[COLUMNS[57]].to_pylist() == [None], "never sent, so never guessed"


def test_a_tag_the_line_sent_once_is_lifted_and_leaves_the_pair_list(codec: FixCodec) -> None:
    """One occurrence is one value, and one value is what a column can answer with.

    `LegSymbol <600>` is undeclared here, so it stays
    exactly where the message put it while `55` and `54` go.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array(["55=TTF|600=LEG1|54=1|"])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert columns[COLUMNS[55]].to_pylist() == ["TTF"]
    assert columns[COLUMNS[54]].to_pylist() == ["1"]
    assert dict(_tags(rest)) == {600: "LEG1"}, "what was left, and only what was left"


def test_a_field_sent_twice_stays_in_the_pair_list_and_fills_no_column(codec: FixCodec) -> None:
    """A tag that repeats belongs to a repeating group, so no occurrence is the line's.

    Lifting the first would answer "who sent it" with whichever copy came
    first -- a wrong answer that looks like a right one. Both copies stay
    where a reader can see there are two, and the column says nothing.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([DUPLICATED])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert columns[COLUMNS[49]].to_pylist() == [None]
    assert _tags(rest) == [(49, "FIRST"), (49, "SECOND")]
    assert columns[COLUMNS[56]].to_pylist() == ["XPAR"], "the scalars around it lift as usual"


def test_a_symbol_inside_a_leg_is_no_order_symbol_and_every_copy_stays(codec: FixCodec) -> None:
    """A multi-leg order has no one symbol, and saying so is the honest column.

    `Symbol <55>` inside `NoLegs` is a leg's, so lifting the first occurrence
    would answer "the symbol" with whichever leg came first. Both `55`s stay,
    both `555`s with them and in wire order, and the column is null. The tags
    the same line sent once lift from that same row regardless.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([LEGS])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert _tags(rest) == [
        (555, "2"),
        (600, "TTF"),
        (55, "SPREAD"),
        (555, "2"),
        (55, "OTHER"),
    ]
    assert columns[COLUMNS[55]].to_pylist() == [None], "no one value, so no value"
    assert columns[COLUMNS[35]].to_pylist() == ["AB"]
    assert columns[COLUMNS[34]].to_pylist() == [8]


def test_a_repeat_in_one_row_costs_no_other_row_its_column(codec: FixCodec) -> None:
    """The rule is per row, and a batch of two rows is where a per-batch one shows.

    One `value_counts` over a composite (row, tag) key answers for the whole
    batch at once, which is what makes the rule cheap -- but a count keyed on
    the tag alone would find two `55`s across these two rows and null both
    columns, and the single-leg order's symbol was never in doubt. This is the
    case that tells the two constructions apart.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([LEGS, SINGLE])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert columns[COLUMNS[55]].to_pylist() == [None, "TTF"]
    assert [tag for tag, _ in _tags(rest)] == [555, 600, 55, 555, 55]
    assert rest.to_pylist()[1] == [], "the row that repeated nothing held nothing back"
    assert columns[COLUMNS[34]].to_pylist() == [8, 9], "each row's own sequence, both lifted"


def test_the_component_half_has_a_stamp_of_its_own_and_reads_it_the_same_way(
    codec: FixCodec,
) -> None:
    """`TransactTime <60>` is a microsecond UTC timestamp."""
    assert f"60={HAPPENED_AT:%Y%m%d-%H:%M:%S}.5" in SINGLE
    assert int(HAPPENED_AT.timestamp() * 1_000_000) == 1786699799500000
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([SINGLE])), "4.4")
    columns, _ = codec.into_lifted_columns(tags, "4.4")
    assert columns[COLUMNS[60]].to_pylist() == [HAPPENED_AT]
    assert columns[COLUMNS[60]].type == pyarrow.timestamp("us", tz="UTC")


def test_the_hop_group_stays_in_the_pair_list_because_one_row_of_it_is_not_one_value(
    codec: FixCodec,
) -> None:
    """`NoHops <627>` is header and is a repeating group; a column would keep one hop.

    Its members stay with it and in wire order, which is what a reader that
    knows the group walks. The scalars around it lift as usual.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([HOPS])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert not {627, 628, 629} & set(COLUMNS), "the group is declared nowhere in the flat layer"
    assert [tag for tag, _ in _tags(rest)] == [627, 628, 629, 628, 629]
    assert columns[COLUMNS[49]].to_pylist() == ["RELAY"]
    assert columns[COLUMNS[55]].to_pylist() == ["TTF"]


def test_a_batch_with_no_flat_field_gets_its_pair_list_back_untouched(codec: FixCodec) -> None:
    """One `is_in` decides for the whole batch, and finding nothing rebuilds nothing."""
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([UNLIFTABLE])), "4.4")
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert not {tag for tag, _ in _tags(tags)} & set(COLUMNS)
    assert rest is tags
    assert {column.null_count for column in columns.values()} == {len(tags)}


def test_a_line_that_carried_no_message_has_a_null_in_every_flat_column(
    codec: FixCodec,
) -> None:
    """`heartbeat emitted` is not a message: nothing to lift, and nothing invented."""
    nothing = pyarrow.nulls(2, KWARGS)
    columns, rest = codec.into_lifted_columns(nothing, "4.4")
    assert rest is nothing
    assert columns[COLUMNS[35]].to_pylist() == [None, None]
    assert {column.null_count for column in columns.values()} == {len(nothing)}


def test_an_empty_pair_list_stays_empty_and_a_null_one_stays_null(codec: FixCodec) -> None:
    """ "It said nothing" and "it was not a message" are different facts.

    The lift rebuilds the list around the row that had something to give up, so
    both of the rows that had nothing come through that rebuild -- which is
    where a construction spelling them alike would flatten them together.
    """
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([SINGLE, "heartbeat", None])), "4.4")
    assert tags.to_pylist()[1] == [], "a line that parsed, and carried no pair"
    columns, rest = codec.into_lifted_columns(tags, "4.4")
    assert rest.to_pylist() == [[], [], None]
    assert columns[COLUMNS[55]].to_pylist() == ["TTF", None, None]


def test_the_pair_list_the_lift_leaves_still_refuses_a_missing_value(codec: FixCodec) -> None:
    """The rebuilt list keeps values NOT NULL."""
    tags = codec.into_kwargs(parse_arrow_array(pyarrow.array([LEGS])), "4.4")
    _, rest = codec.into_lifted_columns(tags, "4.4")
    assert rest is not tags, "rebuilt, because this row had fields to give up"
    assert rest.type == KWARGS
    assert rest.type.value_field.nullable is False
    assert rest.type.value_type.field("key").nullable is False


def test_a_chunked_pair_list_lifts_the_same_way_one_chunk_does(codec: FixCodec) -> None:
    """A reader hands over chunks, so combining them is the ordinary case."""
    lines = [WIRE, HOPS]
    whole = codec.into_kwargs(parse_arrow_array(pyarrow.array(lines)), "4.4")
    halves = pyarrow.chunked_array(
        [
            codec.into_kwargs(parse_arrow_array(pyarrow.array(lines[:1])), "4.4"),
            codec.into_kwargs(parse_arrow_array(pyarrow.array(lines[1:])), "4.4"),
        ]
    )
    one, first = codec.into_lifted_columns(whole, "4.4")
    two, second = codec.into_lifted_columns(halves, "4.4")
    hops = [(627, "2"), (628, "BRIDGE1"), (629, "20260814-09:30:00")]
    assert first.to_pylist() == second.to_pylist()
    assert _tags(first, 1)[: len(hops)] == hops, "and what stayed stayed in wire order"
    senders = one[COLUMNS[49]].to_pylist()
    assert senders == two[COLUMNS[49]].to_pylist() == ["BUYSIDE", "RELAY"]


# -- the registry the wheel actually ships -----------------------------------
#
# Everything above reads `data/fix.zip`, which is the whole published
# dictionary. What a deployment loads is `FixRegistry.from_builtin()`, a
# projection of it -- and a projection that dropped the component declarations
# extracted no party from any message at all, for every version the wire named,
# with nothing anywhere to say so. These read the shipped artifact.

#: A counted Parties group with one sub-party, in the wire form a bridge sends.
#: Synthetic identifiers throughout: what is under test is the shape.
PARTIES_WIRE = (
    SOH.join(
        [
            "8=FIX.4.4",
            "35=8",
            "49=SENDER-TEST",
            "56=TARGET-TEST",
            "34=1",
            "52=20260101-00:00:00",
            "11=ORD-TEST-01",
            "453=2",
            "448=PARTY-TEST-A",
            "447=D",
            "452=1",
            "802=1",
            "523=SUB-TEST-A",
            "803=26",
            "448=PARTY-TEST-B",
            "447=D",
            "452=11",
            "54=1",
            "38=100",
            "10=000",
        ]
    )
    + SOH
)


@pytest.fixture
def packaged() -> FixCodec:
    """A codec over the registry the wheel carries, and nothing else."""
    return FixCodec(registry=FixRegistry.from_builtin())


def _party_rows(codec: FixCodec, message: str, version: str) -> list[dict[str, object]] | None:
    tags = codec.into_kwargs(codec.into_pairs(pyarrow.array([message]), "FIX"), version)
    columns, _ = codec.into_component_columns(tags, version)
    return columns["Parties"].to_pylist()[0]


def test_the_packaged_registry_declares_the_components_it_needs(packaged: FixCodec) -> None:
    """The regression, at the registry: `components()` was empty for every version."""
    registry = packaged.registry
    declared = [version for version in registry.versions if registry.components(version)]
    assert declared == ["5.0.SP2", "5.0.SP1", "5.0", "4.4", "4.3", "FIXT1.1"]
    assert registry.component("Parties", "4.4").members[0].tag == 453


def test_the_packaged_registry_extracts_parties_from_a_wire_message(
    packaged: FixCodec,
) -> None:
    """The consequence, end to end: this answered `[None]` before the fix."""
    parties = _party_rows(packaged, PARTIES_WIRE, "4.4")
    assert parties is not None, "a version was named, so the group must be read"
    assert [party["PartyID"] for party in parties] == ["PARTY-TEST-A", "PARTY-TEST-B"]
    assert [party["PartyRole"] for party in parties] == [1, 11]
    assert [party["PartyIDSource"] for party in parties] == ["D", "D"]
    assert dict(parties[0]["buffer"]) == {
        "NoPartySubIDs": "1",
        "NoPartySubIDs[0].PartySubID": "SUB-TEST-A",
        "NoPartySubIDs[0].PartySubIDType": "26",
    }, "the sub-party group is read through the component tree, not guessed"
    assert parties[1]["buffer"] is None


def test_the_packaged_registry_leaves_every_other_tag_where_it_was(
    packaged: FixCodec,
) -> None:
    """Extraction removes the group and nothing beside it, in wire order."""
    tags = packaged.into_kwargs(packaged.into_pairs(pyarrow.array([PARTIES_WIRE]), "FIX"), "4.4")
    _, residual = packaged.into_component_columns(tags, "4.4")
    assert [tag for tag, _ in _tags(residual)] == [8, 35, 49, 56, 34, 52, 11, 54, 38, 10]


def test_a_version_that_declares_no_parties_component_extracts_nothing_quietly(
    packaged: FixCodec,
) -> None:
    """4.0 through 4.2 predate the component, and the store says so rather than
    being silent about it -- so this is an answer, not a degraded one."""
    registry = packaged.registry
    assert registry.components_available("4.2") and not registry.components("4.2")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert packaged.parties_of("4.2")._member_names == {}


def test_a_version_whose_store_declares_no_component_extracts_nothing(tmp_path: Path) -> None:
    """No fallback tags: the declaration decides, and nothing else does.

    A regenerated dictionary always carries the component declarations of the
    versions that have them, so a version without one is a version FIX gave
    none -- and guessing the tags there would extract a group the standard
    never had.
    """
    bare = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    bare._store_fields("4.4", [fix_field("PartyID", 448, "String", version="4.4")])
    codec = FixCodec(registry=bare)
    extractor = codec.parties_of("4.4")
    assert extractor._member_names == {}
    assert _party_rows(codec, PARTIES_WIRE, "4.4") is None


# -- the two namespaces a bridge writes --------------------------------------


def test_a_fact_written_twice_is_still_lifted_when_both_readings_agree(
    packaged: FixCodec,
) -> None:
    """A third to a half of a real capture's lines carry both namespaces.

    Enrichment writes `#Side` as the field arrived and `Side` as it left, and
    refusing to lift a key that repeats dropped every such field out of its
    typed column into the residual pairs -- on lines that agreed with
    themselves, silently, at that rate.
    """
    line = "|".join(
        [
            "toBridge #BEGINSTRING=FIX.4.4",
            "#SIDE=1",
            "SIDE=1",
            "#CLORDID=ORD-TEST-01",
            "CLORDID=ORD-TEST-01",
            "#ORDERQTY=100",
        ]
    )
    tags, columns = packaged.into_fixmsg_columns(
        packaged.into_pairs(pyarrow.array([line]), "UL"), "4.4"
    )
    lifted = {name: column.to_pylist()[0] for name, column in columns.items()}
    assert lifted["Side"] == "1"
    assert lifted["ClOrdID"] == "ORD-TEST-01"
    assert lifted["OrderQty"] == 100.0
    assert _tags(tags) == [], "and both copies left with the fact they carried"


def test_two_readings_that_disagree_are_still_left_where_they_were(
    packaged: FixCodec,
) -> None:
    """Two values under one key is a group or a rewrite, and picking is a guess."""
    line = "toBridge #BEGINSTRING=FIX.4.4|#SIDE=1|SIDE=2"
    tags, columns = packaged.into_fixmsg_columns(
        packaged.into_pairs(pyarrow.array([line]), "UL"), "4.4"
    )
    assert columns["Side"].to_pylist() == [None]
    assert _tags(tags) == [(54, "1"), (54, "2")]


def test_a_repeated_group_member_is_untouched_by_any_of_this(
    packaged: FixCodec,
) -> None:
    """A wire group repeats a tag with different values, which is what it is for."""
    message = SOH.join(
        ["8=FIX.4.4", "35=8", "295=2", "299=Q-TEST-1", "132=1.0", "299=Q-TEST-2", "132=2.0"]
    )
    tags, columns = packaged.into_fixmsg_columns(
        packaged.into_pairs(pyarrow.array([message + SOH]), "FIX"), "4.4"
    )
    assert columns["QuoteEntryID"].to_pylist() == [None]
    assert columns["BidPx"].to_pylist() == [None]
    assert [tag for tag, _ in _tags(tags)] == [295, 299, 132, 299, 132]


# -- XmlData as the message it carries ---------------------------------------


def _payload(codec: FixCodec, message: str, protocol: str = "FIX") -> list[tuple[object, str]]:
    return _pairs(codec.into_pairs(pyarrow.array([message]), protocol))


def test_xml_data_carrying_a_message_is_read_as_one(packaged: FixCodec) -> None:
    """The standard calls tag 213 an XML stream; real bridges put pairs in it.

    Read as one opaque blob its fields are neither resolvable nor queryable,
    so a payload that reads as pairs becomes pairs where the tag sat.
    """
    message = (
        SOH.join(
            [
                "8=FIX.4.4",
                "35=8",
                "212=44",
                "213=ClOrdID=ORD-TEST-01|Side=1|Account=ACCT-TEST-01",
                "10=000",
            ]
        )
        + SOH
    )
    assert _payload(packaged, message) == [
        ("8", "FIX.4.4"),
        ("35", "8"),
        ("212", "44"),
        ("XmlData.ClOrdID", "ORD-TEST-01"),
        ("XmlData.Side", "1"),
        ("XmlData.Account", "ACCT-TEST-01"),
        ("10", "000"),
    ], "in the place the tag sat, so the message keeps its order"


def test_a_payload_is_read_under_its_own_separator(packaged: FixCodec) -> None:
    """It sits inside a token, so it cannot be written with the outer separator."""
    for payload, separator in (("^A", "^A"), (";", ";"), ("|", "|")):
        message = (
            SOH.join(["8=FIX.4.4", "35=8", f"213=ClOrdID=ORD-TEST-01{payload}Side=1", "10=000"])
            + SOH
        )
        assert ("XmlData.Side", "1") in _payload(packaged, message), separator


def test_a_batch_mixing_payload_separators_reads_each_row_its_own_way(
    packaged: FixCodec,
) -> None:
    head = SOH.join(["8=FIX.4.4", "35=8"])
    messages = pyarrow.array(
        [
            f"{head}{SOH}213=ClOrdID=ORD-TEST-01|Side=1{SOH}10=000{SOH}",
            f"{head}{SOH}213=ClOrdID=ORD-TEST-02;Side=2{SOH}10=000{SOH}",
        ]
    )
    parsed = packaged.into_pairs(messages, "FIX")
    assert _pairs(parsed, 0)[2:4] == [("XmlData.ClOrdID", "ORD-TEST-01"), ("XmlData.Side", "1")]
    assert _pairs(parsed, 1)[2:4] == [("XmlData.ClOrdID", "ORD-TEST-02"), ("XmlData.Side", "2")]


def test_a_payload_that_really_is_xml_is_left_exactly_as_it_was(
    packaged: FixCodec,
) -> None:
    """The defensive half: the standard's own reading, however rare it turns out."""
    message = SOH.join(["8=FIX.4.4", "35=8", '213=<FIXML><Ord ID="x"/></FIXML>', "10=000"]) + SOH
    assert ("213", '<FIXML><Ord ID="x"/></FIXML>') in _payload(packaged, message)


def test_a_payload_that_is_not_pairs_at_all_is_left_alone(packaged: FixCodec) -> None:
    """One `a=b` is a sentence, not a message -- the same rule `BRIDGE` applies."""
    for payload in ("nothing here at all", "onlyone=value"):
        message = SOH.join(["8=FIX.4.4", "35=8", f"213={payload}", "10=000"]) + SOH
        assert ("213", payload) in _payload(packaged, message)


def test_a_payload_field_lands_in_the_column_its_name_earns(packaged: FixCodec) -> None:
    """Which is the point: `XmlData.ClOrdID` resolves like `NoPartyIDs.PartyID` does."""
    message = (
        SOH.join(
            ["8=FIX.4.4", "35=8", "213=ClOrdID=ORD-TEST-01|Side=1|Account=ACCT-TEST-01", "10=000"]
        )
        + SOH
    )
    tags, columns = packaged.into_fixmsg_columns(
        packaged.into_pairs(pyarrow.array([message]), "FIX"), "4.4"
    )
    assert columns["ClOrdID"].to_pylist() == ["ORD-TEST-01"]
    assert columns["Side"].to_pylist() == ["1"]
    assert columns["Account"].to_pylist() == ["ACCT-TEST-01"]
    assert _kwargs(tags) == [], "the payload's fields all found a column"


def test_a_rendered_xmldata_is_read_the_same_way(packaged: FixCodec) -> None:
    """The tag and the rendered name are two spellings of one field."""
    line = "toBridge #BEGINSTRING=FIX.4.4|#XMLDATA=ClOrdID=ORD-TEST-02;Side=2|#SIDE=2"
    assert _payload(packaged, line, "UL") == [
        ("BEGINSTRING", "FIX.4.4"),
        ("XmlData.ClOrdID", "ORD-TEST-02"),
        ("XmlData.Side", "2"),
        ("SIDE", "2"),
    ]


def test_a_row_with_no_payload_costs_nothing_and_changes_nothing(
    packaged: FixCodec,
) -> None:
    message = SOH.join(["8=FIX.4.4", "35=8", "54=1", "10=000"]) + SOH
    assert _payload(packaged, message) == [
        ("8", "FIX.4.4"),
        ("35", "8"),
        ("54", "1"),
        ("10", "000"),
    ]
