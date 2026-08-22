"""Names to the tags FIX gave them -- and what happens to the names it did not."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import FixCodec, FixRegistry, Rule, Rules, parse_arrow_array
from rekep.fix.transcribe import (
    BEGIN_STRING_SOURCE,
    DEFAULT_SOURCE,
    FIX_TAGS,
    KEYVAL,
    NO_SOURCE,
    RULE_SOURCE,
    TagIndex,
    _version_key,
)

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


@pytest.fixture
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True), fix_version="4.4")


@pytest.fixture
def pairs() -> pyarrow.Array:
    return parse_arrow_array(pyarrow.array([BRIDGE]))


def test_the_fixture_is_the_shape_the_tests_assume(pairs: pyarrow.Array) -> None:
    assert len(pairs.to_pylist()[0]) == EXPECTED_BRIDGE_PAIRS


# -- what resolves, and what does not ----------------------------------------


def test_a_name_the_dictionary_knows_becomes_its_tag(codec: FixCodec, pairs) -> None:
    resolved, _ = codec.into_fix_pairs(pairs)
    assert resolved.type == FIX_TAGS
    found = dict(resolved.to_pylist()[0])
    assert found[55] == "TTF" and found[54] == "1" and found[38] == "1200"
    assert found[453] == "2"


def test_a_group_keeps_its_meaning_through_order_and_not_through_the_key(
    codec: FixCodec, pairs
) -> None:
    """`453` then the entries flattened is what the wire looks like."""
    resolved, _ = codec.into_fix_pairs(pairs)
    tags = [tag for tag, _ in resolved.to_pylist()[0]]
    assert tags[tags.index(453) :] == [453, 448, 447, 452, 448, 447, 452]
    values = [value for tag, value in resolved.to_pylist()[0] if tag == 448]
    assert values == ["BUYSIDE", "XPAR"], "both entries kept, in the order they were sent"


def test_a_name_nothing_resolves_is_kept_and_never_guessed(codec: FixCodec, pairs) -> None:
    _, rest = codec.into_fix_pairs(pairs)
    assert rest.type == KEYVAL
    assert dict(rest.to_pylist()[0]) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }


def test_every_pair_lands_in_exactly_one_of_the_two(codec: FixCodec, pairs) -> None:
    """Nothing is dropped, and nothing is counted twice."""
    resolved, rest = codec.into_fix_pairs(pairs)
    assert len(resolved.to_pylist()[0]) + len(rest.to_pylist()[0]) == EXPECTED_BRIDGE_PAIRS


def test_wire_tags_resolve_without_any_dictionary_at_all(codec: FixCodec) -> None:
    """A numeric key is already a tag; the dictionary is for the other kind."""
    wire = parse_arrow_array(pyarrow.array(["8=FIX.4.2|35=D|55=TTF|10=203|"]))
    resolved, rest = codec.into_fix_pairs(wire)
    assert [tag for tag, _ in resolved.to_pylist()[0]] == [8, 35, 55, 10]
    assert rest.to_pylist()[0] == []


def test_a_null_row_stays_null_in_both_halves(codec: FixCodec) -> None:
    """ "Not a message" and "a message with nothing in it" stay different facts."""
    both = parse_arrow_array(pyarrow.array([BRIDGE, None]))
    resolved, rest = codec.into_fix_pairs(both)
    assert resolved.to_pylist()[1] is None
    assert rest.to_pylist()[1] is None


def test_a_chunked_column_is_the_same_answer_as_one_chunk(codec: FixCodec) -> None:
    lines = [BRIDGE, "8=FIX.4.2|35=D|10=203|", None]
    whole = parse_arrow_array(pyarrow.array(lines))
    chunked = pyarrow.chunked_array(
        [parse_arrow_array(pyarrow.array(lines[:1])), parse_arrow_array(pyarrow.array(lines[1:]))]
    )
    assert codec.into_fix_pairs(whole)[0].to_pylist() == (
        codec.into_fix_pairs(chunked)[0].combine_chunks().to_pylist()
    )


def test_a_key_too_wide_to_be_a_tag_is_not_read_as_one(codec: FixCodec) -> None:
    """An epoch-millis key is not a FIX tag, and must not overflow into one."""
    pairs = parse_arrow_array(pyarrow.array(["1786665901147=x|55=TTF"]), "|", named=True)
    resolved, rest = codec.into_fix_pairs(pairs)
    assert dict(resolved.to_pylist()[0]) == {55: "TTF"}
    assert dict(rest.to_pylist()[0]) == {"1786665901147": "x"}


# -- which version -----------------------------------------------------------


def test_the_message_says_which_version_when_it_carries_one(codec: FixCodec) -> None:
    assert codec.version_of("8=FIX.4.2|35=D|") == ("4.2", BEGIN_STRING_SOURCE)
    assert codec.version_of(f"8=FIXT.1.1{SOH}35=A{SOH}") == ("FIXT1.1", BEGIN_STRING_SOURCE)


def test_a_beginstring_no_version_answers_for_falls_through(codec: FixCodec) -> None:
    """`8=FIX4` is truncated, and the nearest version is a guess."""
    assert codec.version_of("recv 8=FIX4^A35=0^A") == ("4.4", DEFAULT_SOURCE)


def test_the_rule_answers_before_the_configured_default(codec: FixCodec) -> None:
    rule = Rule(name="UL", category_id=2, codec="ul", fix_version="4.2")
    assert codec.version_of("toBridge #A=1|#B=2", rule) == ("4.2", RULE_SOURCE)
    assert codec.version_of("toBridge #A=1|#B=2") == ("4.4", DEFAULT_SOURCE)


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
    cold = FixCodec(registry=FixRegistry(cache_dir=tmp_path, offline=True), fix_version="4.4")
    pairs = parse_arrow_array(pyarrow.array([BRIDGE]))
    resolved, rest = cold.into_fix_pairs(pairs)
    assert resolved.to_pylist()[0] == []
    assert len(rest.to_pylist()[0]) == EXPECTED_BRIDGE_PAIRS
    assert cold.version_of("8=FIX.4.2|") == ("4.4", DEFAULT_SOURCE)
    assert cold.tag_field(55) is None


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
#: each, plus two fields that do have a value.
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
    """`ACCOUNT=<null>` is an absent account, not an account called `<null>`."""
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), Rules.DEFAULT.rule(2))
    assert parsed.to_pylist()[0] == [("SYMBOL", "TTF"), ("ORDERQTY", "1200")]


def test_the_absent_spellings_are_matched_case_blind_and_trimmed(codec: FixCodec) -> None:
    """` NULL ` is the renderer's padding and its casing, not a value."""
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), Rules.DEFAULT.rule(2))
    assert dict(parsed.to_pylist()[0]) == {"SYMBOL": "TTF", "ORDERQTY": "1200"}


def test_an_absent_field_reaches_neither_map(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), Rules.DEFAULT.rule(2))
    resolved, rest = codec.into_fix_pairs(parsed)
    assert dict(resolved.to_pylist()[0]) == {55: "TTF", 38: "1200"}
    assert rest.to_pylist()[0] == []


def test_a_message_whose_every_field_was_absent_is_empty_and_not_null(codec: FixCodec) -> None:
    """It was a message. It said nothing. Those are different facts."""
    parsed = codec.into_pairs(pyarrow.array(["#A=null|#B=<NULL>"]), Rules.DEFAULT.rule(2))
    assert parsed.to_pylist() == [[]]


def test_a_null_row_stays_null_through_the_filter(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT, None]), Rules.DEFAULT.rule(2))
    assert parsed.to_pylist()[1] is None


def test_an_empty_set_keeps_every_pair(codec: FixCodec) -> None:
    """A feed whose `n/a` really is a value says so, and nothing is dropped."""
    keeping = FixCodec(registry=codec.registry, fix_version="4.4", null_values=frozenset())
    parsed = keeping.into_pairs(pyarrow.array([ABSENT]), Rules.DEFAULT.rule(2))
    assert len(parsed.to_pylist()[0]) == ABSENT.count("|") + 1


def test_the_set_is_configuration_and_travels_in_a_document(
    codec: FixCodec, tmp_path: Path
) -> None:
    own = FixCodec(registry=codec.registry, null_values=frozenset({"-", "unset"}))
    parsed = own.into_pairs(pyarrow.array(["#A=-|#B=unset|#C=null|#D=1"]), Rules.DEFAULT.rule(2))
    assert parsed.to_pylist()[0] == [("C", "null"), ("D", "1")]
    path = tmp_path / "codec.yml"
    own.into_yaml(path)
    assert FixCodec.from_yaml(path).null_values == frozenset({"-", "unset"})


def test_a_capture_carrying_no_absent_field_is_handed_straight_back(codec: FixCodec) -> None:
    """The skip is the point: two kernels a batch and nothing rebuilt."""
    parsed = parse_arrow_array(pyarrow.array(["#A=1|#B=2"]))
    assert codec.drop_null_values(parsed) is parsed


# -- the seam ----------------------------------------------------------------


def test_a_rule_that_reads_nothing_gives_nulls_and_not_empty_maps(codec: FixCodec) -> None:
    messages = pyarrow.array(["prose", "more prose"])
    parsed = codec.into_pairs(messages, Rules.DEFAULT.rule(0))
    assert parsed.to_pylist() == [None, None]
    assert parsed.type == KEYVAL


def test_the_codec_reads_each_category_the_way_its_rule_says(codec: FixCodec) -> None:
    for line, expected in ((BRIDGE, 2), ("8=FIX.4.2|35=D|10=203|", 1)):
        rule = Rules.DEFAULT.categorise(line)
        assert rule.category_id == expected
        assert codec.into_pairs(pyarrow.array([line]), rule).to_pylist()[0]


def test_the_typed_reading_of_a_value_is_a_cast_against_the_field_that_knows(
    codec: FixCodec,
) -> None:
    """Values stay text here; `tag_field` is where a type comes from."""
    side = codec.tag_field(54)
    assert side.name == "Side"
    assert side.arrow_type == pyarrow.string(), "char is a string, as FIX_SCALARS says"
    assert "1" in side.fix["values"]
