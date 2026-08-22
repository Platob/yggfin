"""Names to the tags FIX gave them -- and what happens to the names it did not."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow
import pytest

from rekep.fix import NO_PROTOCOL, FixCodec, FixRegistry, Rule, Rules, parse_arrow_array
from rekep.fix.columns import COLUMNS, COMMON, FLAT, SESSION, STAMPS
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

#: `52=20260814-09:30:00.123` as this package stores an instant: nanoseconds
#: since the epoch, and UTC because that is what a FIX stamp is. Derived from
#: the stamp the line carries and pinned below, so a reading that shifted by a
#: zone or lost the millisecond cannot move both sides of the comparison.
SENT_AT = datetime(2026, 8, 14, 9, 30, 0, 123000, tzinfo=UTC)
EXPECTED_SENDING_UNIX = int(SENT_AT.timestamp()) * 1_000_000_000 + SENT_AT.microsecond * 1000


@pytest.fixture
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True), fix_version="4.4")


@pytest.fixture
def pairs() -> pyarrow.Array:
    return parse_arrow_array(pyarrow.array([BRIDGE]))


@pytest.fixture
def wire_tags(codec: FixCodec) -> pyarrow.Array:
    """`WIRE` as `fix_tags`, which is what `into_flat_columns` is handed."""
    resolved, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([WIRE])))
    return resolved


def test_the_fixture_is_the_shape_the_tests_assume(
    pairs: pyarrow.Array, wire_tags: pyarrow.Array
) -> None:
    assert len(pairs.to_pylist()[0]) == EXPECTED_BRIDGE_PAIRS
    carried = {tag for tag, _ in wire_tags.to_pylist()[0]}
    assert len(carried) == EXPECTED_WIRE_SESSION + EXPECTED_WIRE_COMMON
    assert len(carried & {tag for tag, _ in SESSION}) == EXPECTED_WIRE_SESSION
    assert len(carried & {tag for tag, _ in COMMON}) == EXPECTED_WIRE_COMMON
    assert f"52={SENT_AT:%Y%m%d-%H:%M:%S}.{SENT_AT.microsecond // 1000:03d}" in WIRE
    assert EXPECTED_SENDING_UNIX == 1786699800123000000


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
    own = FixCodec(
        registry=codec.registry,
        fix_version="4.4",
        rules=Rules(rules=[Rule(protocol="UL", pattern="#", codec="ul", fix_version="4.2")]),
    )
    assert own.version_of("toBridge #A=1|#B=2", "UL") == ("4.2", RULE_SOURCE)
    assert own.version_of("toBridge #A=1|#B=2") == ("4.4", DEFAULT_SOURCE)


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


def test_an_absent_field_reaches_neither_map(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT]), "UL")
    resolved, rest = codec.into_fix_pairs(parsed)
    assert dict(resolved.to_pylist()[0]) == {55: "TTF", 38: "1200"}
    assert rest.to_pylist()[0] == []


def test_a_message_whose_every_field_was_absent_is_empty_and_not_null(codec: FixCodec) -> None:
    """It was a message. It said nothing. Those are different facts."""
    parsed = codec.into_pairs(pyarrow.array(["#A=null|#B=<NULL>"]), "UL")
    assert parsed.to_pylist() == [[]]


def test_a_null_row_stays_null_through_the_filter(codec: FixCodec) -> None:
    parsed = codec.into_pairs(pyarrow.array([ABSENT, None]), "UL")
    assert parsed.to_pylist()[1] is None


def test_an_empty_set_keeps_every_pair(codec: FixCodec) -> None:
    """A feed whose `n/a` really is a value says so, and nothing is dropped."""
    keeping = FixCodec(registry=codec.registry, fix_version="4.4", null_values=frozenset())
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
    assert kept.type == KEYVAL


def test_both_maps_declare_a_value_that_cannot_be_missing() -> None:
    """Pinned on the type as well as the data, because Arrow enforces neither.

    `MapArray.from_arrays` builds a NOT NULL map holding a null value without a
    word, so the declaration is only true where the codec made it true -- which
    is what the test above guards and why this one sits beside it.
    """
    assert FIX_TAGS.item_field.nullable is False
    assert KEYVAL.item_field.nullable is False
    unenforced = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 2], pyarrow.int32()),
        pyarrow.array(["A", "B"]),
        pyarrow.array(["1", None]),
        type=KEYVAL,
    )
    assert unenforced.to_pylist()[0] == [("A", "1"), ("B", None)]


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
    assert kept.type == KEYVAL
    assert kept.to_pylist() == parsed.to_pylist()
    assert [buffer.address for buffer in kept.items.buffers() if buffer is not None] == [
        buffer.address for buffer in parsed.items.buffers() if buffer is not None
    ]


# -- the seam ----------------------------------------------------------------


def test_a_rule_that_reads_nothing_gives_nulls_and_not_empty_maps(codec: FixCodec) -> None:
    messages = pyarrow.array(["prose", "more prose"])
    parsed = codec.into_pairs(messages, NO_PROTOCOL)
    assert parsed.to_pylist() == [None, None]
    assert parsed.type == KEYVAL


def test_the_codec_reads_each_protocol_the_way_its_rule_says(codec: FixCodec) -> None:
    """The name is the whole address: the batch carries it, the rule is ours."""
    for line, expected in ((BRIDGE, "UL"), ("8=FIX.4.2|35=D|10=203|", "FIX")):
        assert Rules.DEFAULT.categorise(line).protocol == expected
        assert codec.into_pairs(pyarrow.array([line]), expected).to_pylist()[0]


def test_the_typed_reading_of_a_value_is_a_cast_against_the_field_that_knows(
    codec: FixCodec,
) -> None:
    """Values stay text here; `tag_field` is where a type comes from."""
    side = codec.tag_field(54)
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

#: `60=20260814-09:29:59.5` as this package stores an instant: nanoseconds
#: since the epoch, like the session layer's stamps and like every other
#: instant in this schema. Derived from the stamp `SINGLE` carries and pinned
#: below, so a reading that shifted by a zone or lost the tenth cannot move
#: both sides of the comparison.
HAPPENED_AT = datetime(2026, 8, 14, 9, 29, 59, 500000, tzinfo=UTC)
EXPECTED_TRANSACT_UNIX = (
    int(HAPPENED_AT.timestamp()) * 1_000_000_000 + HAPPENED_AT.microsecond * 1000
)

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


def test_the_flat_layer_becomes_columns_and_leaves_the_map(
    codec: FixCodec, wire_tags: pyarrow.Array
) -> None:
    """Who sent it, what was traded, at what price is what a reader filters on.

    So each is a column of its own, and it is **removed** from the map rather
    than stored in both places: two copies of one field are two answers as soon
    as anything rewrites either.

    Addressed by tag through `COLUMNS`, because what is under test is that a
    tag's value lands in the column the declaration gives it -- not how that
    column is spelled, which the declaration is the authority on.
    """
    columns, rest = codec.into_flat_columns(wire_tags)
    assert rest.to_pylist()[0] == [], "an order names nothing the flat layer left behind"
    assert rest.type == FIX_TAGS
    filled = {name: column[0].as_py() for name, column in columns.items() if column[0].is_valid}
    assert len(filled) == EXPECTED_WIRE_SESSION + EXPECTED_WIRE_COMMON
    assert filled == {
        COLUMNS[tag]: value
        for tag, value in (
            (8, "FIX.4.2"),
            (9, "176"),
            (35, "D"),
            (34, "7"),
            (49, "BUYSIDE"),
            (56, "XPAR"),
            (52, EXPECTED_SENDING_UNIX),
            (43, "Y"),
            (10, "203"),
            (11, "ORD-1"),
            (55, "TTF"),
            (54, "1"),
            (38, "1200"),
        )
    }


def test_every_declared_flat_field_comes_back_as_its_own_column(
    codec: FixCodec, wire_tags: pyarrow.Array
) -> None:
    """One column per declared tag, filled or not, and text unless it is a stamp.

    A version that never defined a field still gets its column -- a shape that
    changed per version would cost a migration -- and what the text decodes to
    is the schema's to say (`cast_arrow_fix` against the declared field). The
    stamps are the exception the schema cannot make: `20260814-09:30:00.123` is
    not something a cast to `int64` finds its way to, so it is read here.
    """
    columns, _ = codec.into_flat_columns(wire_tags)
    assert (len(SESSION), len(COMMON)) == (33, 26), "the session layer, then the components"
    assert len(columns) == len(FLAT) == 59
    assert set(columns) == set(COLUMNS.values())
    assert sorted(STAMPS) == [52, 60, 122, 370], (
        "SendingTime, TransactTime, OrigSendingTime, OnBehalfOfSendingTime"
    )
    assert {name: column.type for name, column in columns.items()} == {
        name: pyarrow.int64() if tag in STAMPS else pyarrow.string()
        for tag, name in COLUMNS.items()
    }
    assert columns[COLUMNS[57]].to_pylist() == [None], "never sent, so never guessed"


def test_a_tag_the_line_sent_once_is_lifted_and_leaves_the_map_behind(codec: FixCodec) -> None:
    """One occurrence is one value, and one value is what a column can answer with.

    What is left of the map is what says the lift was a rebuild rather than an
    emptying: `LegSymbol <600>` is nothing this layer declares, so it stays
    exactly where the message put it while `55` and `54` go.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array(["55=TTF|600=LEG1|54=1|"])))
    columns, rest = codec.into_flat_columns(tags)
    assert columns[COLUMNS[55]].to_pylist() == ["TTF"]
    assert columns[COLUMNS[54]].to_pylist() == ["1"]
    assert dict(rest.to_pylist()[0]) == {600: "LEG1"}, "what was left, and only what was left"


def test_a_field_sent_twice_stays_in_the_map_and_fills_no_column(codec: FixCodec) -> None:
    """A tag that repeats belongs to a repeating group, so no occurrence is the line's.

    Lifting the first would answer "who sent it" with whichever copy came
    first -- a wrong answer that looks like a right one. Both copies stay
    where a reader can see there are two, and the column says nothing.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([DUPLICATED])))
    columns, rest = codec.into_flat_columns(tags)
    assert columns[COLUMNS[49]].to_pylist() == [None]
    assert rest.to_pylist()[0] == [(49, "FIRST"), (49, "SECOND")]
    assert columns[COLUMNS[56]].to_pylist() == ["XPAR"], "the scalars around it lift as usual"


def test_a_symbol_inside_a_leg_is_no_order_symbol_and_every_copy_stays(codec: FixCodec) -> None:
    """A multi-leg order has no one symbol, and saying so is the honest column.

    `Symbol <55>` inside `NoLegs` is a leg's, so lifting the first occurrence
    would answer "the symbol" with whichever leg came first. Both `55`s stay,
    both `555`s with them and in wire order, and the column is null. The tags
    the same line sent once lift from that same row regardless.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([LEGS])))
    columns, rest = codec.into_flat_columns(tags)
    assert rest.to_pylist()[0] == [
        (555, "2"),
        (600, "TTF"),
        (55, "SPREAD"),
        (555, "2"),
        (55, "OTHER"),
    ]
    assert columns[COLUMNS[55]].to_pylist() == [None], "no one value, so no value"
    assert columns[COLUMNS[35]].to_pylist() == ["AB"]
    assert columns[COLUMNS[34]].to_pylist() == ["8"]


def test_a_repeat_in_one_row_costs_no_other_row_its_column(codec: FixCodec) -> None:
    """The rule is per row, and a batch of two rows is where a per-batch one shows.

    One `value_counts` over a composite (row, tag) key answers for the whole
    batch at once, which is what makes the rule cheap -- but a count keyed on
    the tag alone would find two `55`s across these two rows and null both
    columns, and the single-leg order's symbol was never in doubt. This is the
    case that tells the two constructions apart.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([LEGS, SINGLE])))
    columns, rest = codec.into_flat_columns(tags)
    assert columns[COLUMNS[55]].to_pylist() == [None, "TTF"]
    assert [tag for tag, _ in rest.to_pylist()[0]] == [555, 600, 55, 555, 55]
    assert rest.to_pylist()[1] == [], "the row that repeated nothing held nothing back"
    assert columns[COLUMNS[34]].to_pylist() == ["8", "9"], "each row's own sequence, both lifted"


def test_the_component_half_has_a_stamp_of_its_own_and_reads_it_the_same_way(
    codec: FixCodec,
) -> None:
    """`TransactTime <60>` is when the business event happened, in nanoseconds.

    Read here rather than left to the schema, for the reason the session
    layer's stamps are: `20260814-09:29:59.5` is not something a cast to
    `int64` finds its way to. Nanoseconds and not an Arrow timestamp, because
    pyiceberg refuses `timestamp[ns]` outright and `timestamp[us]` would
    truncate a value whose text has just been lifted out of the map.
    """
    assert f"60={HAPPENED_AT:%Y%m%d-%H:%M:%S}.5" in SINGLE
    assert EXPECTED_TRANSACT_UNIX == 1786699799500000000
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([SINGLE])))
    columns, _ = codec.into_flat_columns(tags)
    assert columns[COLUMNS[60]].to_pylist() == [EXPECTED_TRANSACT_UNIX]
    assert columns[COLUMNS[60]].type == pyarrow.int64()


def test_the_hop_group_stays_in_the_map_because_one_row_of_it_is_not_one_value(
    codec: FixCodec,
) -> None:
    """`NoHops <627>` is header and is a repeating group; a column would keep one hop.

    Its members stay with it and in wire order, which is what a reader that
    knows the group walks. The scalars around it lift as usual.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([HOPS])))
    columns, rest = codec.into_flat_columns(tags)
    assert not {627, 628, 629} & set(COLUMNS), "the group is declared nowhere in the flat layer"
    assert [tag for tag, _ in rest.to_pylist()[0]] == [627, 628, 629, 628, 629]
    assert columns[COLUMNS[49]].to_pylist() == ["RELAY"]
    assert columns[COLUMNS[55]].to_pylist() == ["TTF"]


def test_a_batch_with_no_flat_field_gets_its_map_back_untouched(codec: FixCodec) -> None:
    """One `is_in` decides for the whole batch, and finding nothing rebuilds nothing."""
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([UNLIFTABLE])))
    columns, rest = codec.into_flat_columns(tags)
    assert not {tag for tag, _ in tags.to_pylist()[0]} & set(COLUMNS)
    assert rest is tags
    assert {column.null_count for column in columns.values()} == {len(tags)}


def test_a_line_that_carried_no_message_has_a_null_in_every_flat_column(
    codec: FixCodec,
) -> None:
    """`heartbeat emitted` is not a message: nothing to lift, and nothing invented."""
    nothing = pyarrow.nulls(2, FIX_TAGS)
    columns, rest = codec.into_flat_columns(nothing)
    assert rest is nothing
    assert columns[COLUMNS[35]].to_pylist() == [None, None]
    assert {column.null_count for column in columns.values()} == {len(nothing)}


def test_an_empty_map_stays_empty_and_a_null_one_stays_null(codec: FixCodec) -> None:
    """ "It said nothing" and "it was not a message" are different facts.

    The lift rebuilds the map around the row that had something to give up, so
    both of the rows that had nothing come through that rebuild -- which is
    where a construction spelling them alike would flatten them together.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([SINGLE, "heartbeat", None])))
    assert tags.to_pylist()[1] == [], "a line that parsed, and carried no pair"
    columns, rest = codec.into_flat_columns(tags)
    assert rest.to_pylist() == [[], [], None]
    assert columns[COLUMNS[55]].to_pylist() == ["TTF", None, None]


def test_the_map_the_lift_leaves_still_refuses_a_missing_value(codec: FixCodec) -> None:
    """The half that stays is the same declaration as the half that arrived.

    `MapArray.from_arrays` builds the reduced map, so the NOT NULL on the value
    is the rebuild's to keep rather than something the type carries by itself
    -- and a reduced map that had widened it would let a later writer store the
    pair `drop_null_values` exists to refuse.
    """
    tags, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array([LEGS])))
    _, rest = codec.into_flat_columns(tags)
    assert rest is not tags, "rebuilt, because this row had tags to give up"
    assert rest.type == FIX_TAGS
    assert rest.type.item_field.nullable is False


def test_a_chunked_map_lifts_the_same_way_one_chunk_does(codec: FixCodec) -> None:
    """A reader hands over chunks, so combining them is the ordinary case."""
    lines = [WIRE, HOPS]
    whole, _ = codec.into_fix_pairs(parse_arrow_array(pyarrow.array(lines)))
    halves = pyarrow.chunked_array(
        [
            codec.into_fix_pairs(parse_arrow_array(pyarrow.array(lines[:1])))[0],
            codec.into_fix_pairs(parse_arrow_array(pyarrow.array(lines[1:])))[0],
        ]
    )
    one, first = codec.into_flat_columns(whole)
    two, second = codec.into_flat_columns(halves)
    hops = [(627, "2"), (628, "BRIDGE1"), (629, "20260814-09:30:00")]
    assert first.to_pylist() == second.to_pylist()
    assert first.to_pylist()[1][: len(hops)] == hops, "and what stayed stayed in wire order"
    senders = one[COLUMNS[49]].to_pylist()
    assert senders == two[COLUMNS[49]].to_pylist() == ["BUYSIDE", "RELAY"]
