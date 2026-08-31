"""One accessor: four ways of naming a field, and two executions of one rule table."""

from __future__ import annotations

import pyarrow
import pytest

from rekep import FixMsg
from rekep.fix import FixRegistry
from rekep.fix.access import Entry, FieldAccess
from rekep.fix.transcribe import FixCodec

#: The four ways a caller has a field in hand, for one field that is all four:
#: `TrdRegTimestamp <769>` inside its own repeating group.
FOUR_WAYS = (769, "TrdRegTimestamp", "NoTrdRegTimestamps[0].TrdRegTimestamp", "trd_reg_timestamp")


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    return FixRegistry.from_builtin()


@pytest.fixture(scope="module")
def access(registry: FixRegistry) -> FieldAccess:
    return FieldAccess.of(registry, None)


# -- the four spellings ------------------------------------------------------


@pytest.mark.parametrize("named", FOUR_WAYS)
def test_every_way_of_naming_one_field_reads_the_same_value(
    access: FieldAccess, named: int | str
) -> None:
    """A tag, a canonical name, a component path and a snake spelling are one field."""
    stored = [
        {
            "tag": 769,
            "key": "TrdRegTimestamp",
            "value": "20260824-10:00:01.123",
            "comp": "NoTrdRegTimestamps[0]",
        }
    ]
    assert access.reading(stored, named).raw == "20260824-10:00:01.123"


def test_a_namespace_qualified_key_reads_only_under_its_namespace(access: FieldAccess) -> None:
    """`TECH.CLIENTID` is a field of its own, not `CLIENTID` wearing a prefix.

    So the qualified spelling finds it and the bare one does not -- the
    opposite of a group entry, where the container is only *where* the field
    sits.
    """
    stored = [{"tag": 0, "key": "TECH.CLIENTID", "value": "42", "comp": None}]
    assert access.reading(stored, "TECH.CLIENTID").raw == "42"
    assert not access.reading(stored, "CLIENTID")


def test_a_group_entry_answers_a_bare_name_and_its_own_path(access: FieldAccess) -> None:
    """The index and the group are where a field sits, not what it is."""
    stored = [
        {"tag": 448, "key": "PartyID", "value": "A", "comp": "NoPartyIDs[0]"},
        {"tag": 448, "key": "PartyID", "value": "B", "comp": "NoPartyIDs[1]"},
    ]
    assert access.reading(stored, "PartyID").raw == "A"
    assert access.reading(stored, "NoPartyIDs[1].PartyID").raw == "B"
    assert [found.raw for found in access.readings(stored, 448)] == ["A", "B"]


def test_a_repeated_tag_reads_every_value_in_wire_order(access: FieldAccess) -> None:
    """`readings` is what a repeating tag is: order is the identity."""
    message = FixMsg.from_text("8=FIX.4.4|453=2|448=ONE|448=TWO|448=THREE|10=000")
    assert [found.raw for found in access.readings(message.pairs, 448)] == ["ONE", "TWO", "THREE"]


# -- the raw value and the typed reading, from one call ----------------------


def test_one_call_answers_the_stored_text_and_the_typed_reading(access: FieldAccess) -> None:
    """No call site chooses an accessor by which half of the answer it wants."""
    found = access.reading([("38", "125")], "OrderQty")
    assert found.raw == "125"
    assert found.value == 125.0


def test_an_unregistered_value_reads_back_coherently_typed(access: FieldAccess) -> None:
    """The floor under registry promotion: a key no record explains still
    gets the plainest reading its value spells -- integer, float, dashed
    date, clock time or boolean word -- and anything else stays the text it
    was. Only the typed half sniffs; `raw` is the stored fact either way."""
    import datetime

    def read(value: str) -> object:
        stored = [{"tag": 0, "key": "UNREGISTEREDFIELD", "value": value, "comp": None}]
        return access.reading(stored, "UNREGISTEREDFIELD")

    assert read("12345").value == 12345
    assert read("007").value == 7 and read("007").raw == "007", "raw keeps the spelling"
    assert read("-2.50").value == -2.5
    assert read("2026-08-21").value == datetime.date(2026, 8, 21)
    assert read("20260821").value == 20260821, "an all-digit run is likelier an identifier"
    assert read("13:45:59.123").value == datetime.time(13, 45, 59, 123000)
    assert read("true").value is True
    assert read("N").value is False
    assert read("ULBRIDGE01").value == "ULBRIDGE01", "an identifier is not a number"
    assert read("2026-13-45").value == "2026-13-45", "an impossible day is not a date"
    assert read("25:00:00").value == "25:00:00", "nor an impossible clock a time"
    assert read("202608-21").value == "202608-21", "a half-dashed run is a code"
    assert read("13:4559").value == "13:4559", "and so is a half-coloned one"
    endless = "9" * 5000
    assert read(endless).value == endless, "past int()'s digit cap, a run is text"
    saturated = "1" + "0" * 309 + ".5"
    assert read(saturated).value == saturated, "past float64, infinity is a fabrication"


def test_a_value_spelled_by_its_meaning_resolves_through_the_dictionary(
    access: FieldAccess,
) -> None:
    """The dictionary's own `translate`, applied on the way out.

    A non-matching spelling resolves without the call site knowing there was
    anything to resolve.
    """
    assert access.reading([("54", "Buy")], "Side").value == "1"
    assert access.reading([("54", "BUY")], "Side").value == "1"
    assert access.reading([("54", "1")], "Side").value == "1"


def test_a_field_the_row_does_not_carry_is_falsy(access: FieldAccess) -> None:
    """Absence is an answer, and it is distinguishable from a null value."""
    found = access.reading([("54", "1")], "Price")
    assert not found
    assert found.raw is None
    assert found.value is None


# -- the two executions agree ------------------------------------------------

#: The same message written the two ways a capture writes one: as the wire
#: spells it, all tags, and as a bridge renders it, all names. A rendered line
#: is the only one that *can* carry a component path or a vendor namespace --
#: in wire mode a non-digit key is not a token at all -- so the four naming
#: shapes are asked of the rendered row and the tag shape of both.
WIRE = "8=FIX.4.4|35=8|54=1|38=125|453=1|448=ABC|447=D|10=000"
RENDERED = (
    "send #BeginString=FIX.4.4|#MsgType=8|#Side=1|#OrderQty=125|"
    "#NoPartyIDs[0].PartyID=ABC|#TECH.CLIENTID=42"
)


def test_the_scalar_and_vectorized_paths_resolve_one_input_alike(registry: FixRegistry) -> None:
    """One declared rule table, two executions -- not two rule sets that agree today.

    The columnar path resolves a whole column through `TagIndex`; the scalar
    one reads the same index a key at a time. Asked for the same keys of the
    same line, the two answer with the same tags, the same terminal names and
    the same containment.
    """
    codec = FixCodec(registry=registry)
    keys = pyarrow.array(
        ["54", "38", "NoPartyIDs[0].PartyID", "TECH.CLIENTID", "Side", "NotAFieldAnywhere"],
        pyarrow.string(),
    )
    index = codec.index_of("4.4")
    tags, matched, reduced, contained = index.resolve_with_match(keys)
    for position, key in enumerate(keys.to_pylist()):
        scalar = index.resolve_key(key)
        assert scalar[0] == tags[position].as_py(), key
        assert scalar[1] == matched[position].as_py(), key
        assert scalar[2] == reduced[position].as_py(), key
        assert scalar[3] == contained[position].as_py(), key


def test_the_accessor_reads_what_the_codec_transcribed(registry: FixRegistry) -> None:
    """The stored column a batch produced, read back through the one accessor.

    The vectorized transcription decides what a field's tag, name and
    container are; the accessor has to find the field under every one of the
    four spellings of that same answer.
    """
    codec = FixCodec(registry=registry)
    stored = codec.into_entries(
        codec.into_pairs(pyarrow.array([RENDERED], pyarrow.string()), "FIXML"), "4.4"
    ).to_pylist()[0]
    access = FieldAccess.of(registry, "4.4")
    assert access.reading(stored, 54).raw == "1"
    assert access.reading(stored, "Side").raw == "1"
    assert access.reading(stored, "PartyID").raw == "ABC"
    assert access.reading(stored, "NoPartyIDs[0].PartyID").raw == "ABC"
    assert access.reading(stored, "TECH.CLIENTID").raw == "42"
    assert not access.reading(stored, "CLIENTID")


def test_a_wire_row_answers_a_name_and_a_rendered_row_answers_a_tag(
    registry: FixRegistry,
) -> None:
    """The dictionary is what makes the two spellings one field.

    A wire row stores tag 54 and a rendered one stores `Side`; asked for
    either spelling, both answer -- which is the whole point of resolving in
    one place rather than at each call site.
    """
    codec = FixCodec(registry=registry)
    access = FieldAccess.of(registry, "4.4")
    wire = codec.into_entries(
        codec.into_pairs(pyarrow.array([WIRE], pyarrow.string()), "FIX"), "4.4"
    ).to_pylist()[0]
    rendered = codec.into_entries(
        codec.into_pairs(pyarrow.array([RENDERED], pyarrow.string()), "FIXML"), "4.4"
    ).to_pylist()[0]
    for named in (54, "Side", "OrderQty", "PartyID"):
        assert access.reading(wire, named).raw == access.reading(rendered, named).raw, named


def test_pairs_and_stored_entries_read_alike(registry: FixRegistry) -> None:
    """A row addressed as pairs and the same row stored answer the same asks."""
    codec = FixCodec(registry=registry)
    stored = codec.into_entries(
        codec.into_pairs(pyarrow.array([RENDERED], pyarrow.string()), "FIXML"), "4.4"
    ).to_pylist()[0]
    pairs = FixMsg.from_text(RENDERED).pairs
    access = FieldAccess.of(registry, "4.4")
    for named in (54, "Side", "OrderQty", "NoPartyIDs[0].PartyID", "TECH.CLIENTID"):
        assert access.reading(stored, named).raw == access.reading(pairs, named).raw, named


# -- one way in --------------------------------------------------------------


def test_the_wire_model_reads_through_the_same_accessor() -> None:
    """`FixMsg.get` is the accessor with no dictionary, not a second reading.

    No dictionary, so a name resolves by spelling alone: a rendered message
    answers `Side` and a wire one answers `54`, and neither invents the
    other's spelling. What the shared rules still give it is the rest -- case,
    an entry index, and the group a member sits in.
    """
    rendered = FixMsg.from_text(RENDERED)
    assert rendered.get("Side").raw == "1"
    assert rendered.get("side").raw == "1"
    assert rendered.get("PartyID").raw == "ABC"
    assert rendered.get("NoPartyIDs[0].PartyID").raw == "ABC"
    assert [reading.raw for reading in rendered.readings("PartyID")] == ["ABC"]
    assert FixMsg.from_text(WIRE).get(54).raw == "1"


def test_a_parsed_row_reads_its_columns_and_its_pairs_through_one_call() -> None:
    """A lifted fact and a residual one answer the same way on a stored row."""
    from rekep.text import FixMsg

    row = FixMsg(
        unix=1,
        hash=1,
        side="1",
        orderqty=125.0,
        entries=[
            {
                "tag": 448,
                "key": "PartyID",
                "value": "ABC",
                "comp": "NoPartyIDs[0]",
            }
        ],
    )
    assert row.get("Side").raw == "1"
    assert row.get(54).raw == "1"
    assert row.get("PartyID").raw == "ABC"
    assert row.get("NoPartyIDs[0].PartyID").raw == "ABC"
    assert not row.get("Price")


def test_entries_read_pairs_stored_structs_and_ready_entries_alike() -> None:
    """One `Entry` view over the three shapes a row is held in."""
    pair = next(FieldAccess.entries_of([("NoPartyIDs[0].PartyID", "A")]))
    stored = next(
        FieldAccess.entries_of(
            [
                {
                    "tag": 448,
                    "key": "PartyID",
                    "value": "A",
                    "comp": "NoPartyIDs[0]",
                }
            ]
        )
    )
    assert (pair.name, pair.lead, pair.entry_lead, pair.value) == (
        "PartyID",
        "NoPartyIDs[0]",
        True,
        "A",
    )
    assert (stored.name, stored.lead, stored.entry_lead, stored.value) == (
        "PartyID",
        "NoPartyIDs[0]",
        True,
        "A",
    )
    ready = Entry(tag=54, key="Side", value="1")
    assert next(FieldAccess.entries_of([ready])) is ready


def test_a_field_resolves_to_one_wire_tag_however_it_is_spelled(access: FieldAccess) -> None:
    """`tag_text` is memoized per spelling, so the answer must not depend on it."""
    assert access.tag_text("TrdRegTimestamp") == "769"
    assert access.tag_text(769) == "769"
    assert access.tag_text("TrdRegTimestamp") == "769", "the memo answers the same"
    # A key already spelled in digits is the tag, and keeps its own spelling:
    # a wire message keys `007` that way and reading it back must find it.
    assert access.tag_text("007") == "007"
    assert access.tag_text("NoSuchFieldAnywhere") == "NoSuchFieldAnywhere"


def test_one_accessor_answers_for_one_dictionary(registry: FixRegistry) -> None:
    """Its memos are the point: a second accessor would resolve everything twice."""
    assert FieldAccess.of(registry, None) is FieldAccess.of(registry, None)
    assert FieldAccess.of(registry, None) is not FieldAccess.of(registry, "4.4")
