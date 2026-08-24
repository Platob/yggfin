"""The FIX dictionary in `data/fix.zip` is checked here, so a bad scrape cannot ship.

A dump nobody verifies is a directory of files that merely *look* like a
dictionary. The first scrape of this one came back a fifth empty -- the site
throttles a seven-thousand-page walk, and every page it refused became a field
with no type, no description and no enumeration -- and every file it wrote
still parsed, still loaded, still answered a lookup. So the checks here are the
ones that would have failed then: what a version file holds is `Field`s, the
tags are its own, and the parts the pages carry are carried through.
"""

import json
import urllib.request
import zipfile
from importlib.resources import files
from pathlib import Path

import pyarrow
import pytest

from rekep import Field
from rekep.fix import FIX_SCALARS, FixRegistry
from rekep.fix.columns import _ORDER
from rekep.fix.publish import (
    LOG_FIELDS,
    MARKET_FIELDS,
    PROJECTED,
    latest_fields,
    missing_from,
    publish_builtin,
)
from rekep.market.fix import CARRIED_FIELDS, market_tags

#: The dictionary is at the repo root, beside `python/` -- published data,
#: not something shipped in the wheel. One archive: a registry pointed at it
#: reads the JSON inside, and the extension is what tells it to.
DATA = Path(__file__).resolve().parents[2] / "data" / "fix.zip"


def member(name: str) -> dict[str, object]:
    """One document out of the published archive, read without a registry.

    The tests below have to be able to fail when the registry is wrong, so
    what they read the archive with is `zipfile`, not the code under test.
    """
    with zipfile.ZipFile(DATA) as archive:
        return json.loads(archive.read(name).decode("utf-8"))


VERSIONS: list[str] = member("versions.json")["versions"]

#: Pinned so a moved or half-written directory fails here rather than passing
#: every test below by iterating over nothing.
EXPECTED_VERSIONS = 9


class OfflineRegistry(FixRegistry):
    """A registry over the dump, for which fetching anything is the failure.

    The dump is meant to answer every version of every question offline, so a
    test that quietly fetched a page it was missing would be testing the site.
    """

    def _read(self, request: urllib.request.Request) -> str:
        raise AssertionError(f"the dump did not answer for {request.full_url}")


@pytest.fixture(scope="module")
def registry() -> OfflineRegistry:
    return OfflineRegistry(cache_dir=DATA)


def test_the_archive_holds_the_versions_it_lists() -> None:
    assert len(VERSIONS) == EXPECTED_VERSIONS
    with zipfile.ZipFile(DATA) as archive:
        names = archive.namelist()
    assert len(names) == len(set(names)), "one member per name, never a shadowed one"
    assert {name[: -len(".json")] for name in names} - {"versions"} == set(VERSIONS)
    assert VERSIONS[0] == "5.0.SP2", "newest first"
    assert VERSIONS[-1] == "FIXT1.1", "and the transport last"


@pytest.mark.parametrize("version", VERSIONS)
def test_a_version_file_holds_the_fields_it_says(version: str) -> None:
    dumped = member(f"{version}.json")
    assert dumped["version"] == version
    assert dumped["url"].endswith(f"/{version}/")
    fields = [Field.from_dict(member) for member in dumped["fields"]]
    tags = [int(member.fix["tag"]) for member in fields]
    assert len(fields) > 50, "the transport is the small one, at 74"
    assert tags == sorted(set(tags)), "one entry per tag, in tag order"
    assert {member.fix["version"] for member in fields} == {version}
    assert all(member.arrow_type for member in fields)


@pytest.mark.parametrize("version", VERSIONS)
def test_a_version_carries_what_its_pages_say(version: str, registry: FixRegistry) -> None:
    """Typed, described, and enumerated where the field is an enumeration.

    Ratios rather than counts, because the site does edit its dictionary --
    but ratios far above what a throttled scrape reaches (it scored 0.78
    typed) and far below what a clean one does (1.00 typed, 0.97 described).
    """
    fields = registry.fields(version)
    typed = sum(1 for member in fields if member.fix.get("type"))
    described = sum(1 for member in fields if member.description)
    enumerated = sum(1 for member in fields if member.fix.get("values"))
    assert typed / len(fields) > 0.95
    assert described / len(fields) > 0.90
    assert enumerated > len(fields) // 10, "a tenth of FIX is enumerations, at least"


def test_the_dump_answers_a_lookup_offline(registry: FixRegistry) -> None:
    """What the directory is for: a registry that never fetches anything."""
    side = registry.field("Side")
    assert side.fix["tag"] == "54"
    assert side.fix["version"] == "5.0.SP2", "the newest version that has it"
    assert side.description == "Side of order."
    assert json.loads(side.fix["values"])["1"] == "Buy"
    assert registry.field(35).name == "MsgType"
    assert [member.fix["version"] for member in registry.lookup("Side")] == [
        version for version in VERSIONS if version != "FIXT1.1"
    ], "every application version defines Side, and the transport does not"


def test_the_dump_is_the_name_to_tag_mapping_a_rendered_log_needs(
    registry: FixRegistry,
) -> None:
    """`tag_arrow_array(names=...)` is the whole point of carrying the dictionary."""
    tags = registry.tags()
    assert tags["side"] == 54
    assert tags["msgtype"] == 35
    assert tags["partyid"] == 448
    assert len(tags) > 1500, "every distinct name of every version, newest winning"


def test_the_archive_is_what_publishing_it_produces(tmp_path: Path) -> None:
    """Byte for byte, so "nothing changed" looks like nothing changed.

    `into_zip` stamps every member at the start of zip time and deflates it
    the same way each run, so rebuilding the published archive from its own
    contents has to give the published archive back. A refresh that changes
    one field then shows up as a change, and a rebuild that changes nothing
    shows up as nothing.
    """
    rebuilt = FixRegistry(cache_dir=DATA, retries=0).into_zip(tmp_path / "fix.zip")
    assert rebuilt.read_bytes() == DATA.read_bytes()


def test_a_projection_is_a_small_exact_offline_registry(
    registry: FixRegistry, tmp_path: Path
) -> None:
    target = registry.into_projection(tmp_path / "projected.zip", ["Side", "QuoteID"])
    projected = FixRegistry(cache_dir=target, offline=True)
    assert projected.versions == registry.versions
    assert set(projected.tags()) == {"side", "quoteid"}
    for version in projected.versions:
        expected = [
            member for member in registry.fields(version) if member.name in {"Side", "QuoteID"}
        ]
        assert projected.fields(version) == expected
    # A seventh of the published dictionary, and every byte of the remainder
    # is component declarations: those travel whole rather than being selected
    # with the fields, because a component says where a repeating group starts
    # and ends and a tree missing members would end it somewhere else.
    assert target.stat().st_size < DATA.stat().st_size // 5
    for version in projected.versions:
        assert projected.components(version) == registry.components(version)

    again = registry.into_projection(tmp_path / "again.zip", ["Side", "QuoteID"])
    assert again.read_bytes() == target.read_bytes()


def test_a_projection_refuses_missing_fields_and_its_source(
    registry: FixRegistry, tmp_path: Path
) -> None:
    with pytest.raises(KeyError, match="AbsentField"):
        registry.into_projection(tmp_path / "bad.zip", ["AbsentField"])
    with pytest.raises(ValueError, match="cannot replace its source"):
        registry.into_projection(DATA, ["Side"])


def test_the_builtin_projection_matches_the_published_versions(
    registry: FixRegistry,
) -> None:
    builtin = FixRegistry.from_builtin()
    assert builtin.versions == ("FIX.Latest", *registry.versions)
    # Derived from `publish.PROJECTED`, then pinned: 170 keys resolve to 185
    # distinct names because a version may spell one tag differently, and the
    # projection selects by tag once a key has resolved.
    assert len(builtin.tags()) == 185
    selected = {
        int(member.fix["tag"])
        for version in registry.versions
        for member in builtin.fields(version)
    }
    for version in registry.versions:
        expected = [
            member for member in registry.fields(version) if int(member.fix["tag"]) in selected
        ]
        assert builtin.fields(version) == expected, version
    assert [member.name for member in builtin.fields("FIX.Latest")] == [
        "ExposureDuration",
        "ExposureDurationUnit",
    ]


def test_the_builtin_projection_is_what_publishing_it_produces(tmp_path: Path) -> None:
    """Byte for byte, from the published dictionary and the declared key list.

    The wheel's registry is generated, and a generated artifact nobody can
    regenerate is a hand-edited one. This is the command in `data/README.md`,
    run against the archive that ships beside it.
    """
    rebuilt = publish_builtin(DATA, tmp_path / "registry.zip")
    packaged = Path(str(files("rekep.fix").joinpath("registry.zip")))
    assert rebuilt.read_bytes() == packaged.read_bytes()


def test_the_builtin_projection_answers_every_key_the_package_looks_up(
    registry: FixRegistry,
) -> None:
    """`publish.PROJECTED` is a hand-written list, and these are its authorities.

    A field added to the log schema or to market translation but not to the
    list would ship a registry that cannot answer for it -- which is the same
    silence as a name nobody has ever seen, and reads as one downstream.
    """
    assert missing_from(registry, PROJECTED) == [member.name for member in latest_fields()], (
        "the two the dictionary's per-version pages predate, and nothing else"
    )
    assert set(LOG_FIELDS) == set(_ORDER)
    assert set(PROJECTED) >= set(CARRIED_FIELDS)
    assert set(MARKET_FIELDS).isdisjoint(LOG_FIELDS), "each name declared once"
    builtin = FixRegistry.from_builtin()
    assert not missing_from(builtin, PROJECTED)
    assert not missing_from(builtin, tuple(market_tags())), "every tag translation reads"


def test_the_builtin_projection_carries_the_component_declarations(
    registry: FixRegistry,
) -> None:
    """The regression: a projection that drops these extracts no party at all.

    `components()` answers `[]` for a version whose spec declares none *and*
    for a store that never held any, so the assertion has to be both -- the
    declarations for the versions that have them, and the stored-and-empty
    answer for 4.0 through 4.2, which is what tells a reader the store was
    asked rather than never written.
    """
    builtin = FixRegistry.from_builtin()
    parties = builtin.component("Parties", "4.4")
    assert parties.name == "Parties"
    assert [member.name for member in parties.members] == ["NoPartyIDs"]
    assert parties.members[0].tag == 453
    for version in registry.versions:
        assert builtin.components_available(version), version
        assert builtin.components(version) == registry.components(version), version
    declared = {version for version in registry.versions if builtin.components(version)}
    assert declared == {"4.3", "4.4", "5.0", "5.0.SP1", "5.0.SP2", "FIXT1.1"}
    assert {"4.0", "4.1", "4.2"}.isdisjoint(declared), "no component existed before 4.3"


def test_the_archive_says_it_came_from_nowhere_in_particular() -> None:
    """The other half of "built twice is the same file": the host.

    `ZipInfo` reads `create_system` off `os.name`, so publishing this
    dictionary from Windows wrote a different byte per member than publishing
    it from Linux -- and the rebuild above failed on the Windows CI leg alone,
    where the archive is otherwise identical. Pinned to Unix because the
    permission bits the member already carries are POSIX ones.
    """
    with zipfile.ZipFile(DATA) as archive:
        systems = {entry.create_system for entry in archive.infolist()}
        modes = {entry.external_attr >> 16 for entry in archive.infolist()}
    assert systems == {3}, "every member, whichever host published it"
    assert modes == {0o644}


def test_the_archive_is_worth_being_an_archive() -> None:
    """Derived, then pinned: the dictionary compresses to about a sixth."""
    with zipfile.ZipFile(DATA) as archive:
        stored = sum(entry.file_size for entry in archive.infolist())
    assert stored > 2_500_000, "the JSON inside is the whole dictionary"
    assert DATA.stat().st_size < stored // 4


def test_the_dictionary_is_ascii_where_it_matters(registry: FixRegistry) -> None:
    """Names and descriptions carry no cased character outside ASCII.

    Derived, then pinned: a search folds case, and a store that folds it any
    other way than Python does would answer differently. Nothing here needs
    more than ASCII folding today, and this is what says so if a refresh ever
    brings a character that does.
    """
    cased = {
        character
        for version in VERSIONS
        for member in registry.fields(version)
        for character in member.description + member.name
        if ord(character) > 127 and character.lower() != character.upper()
    }
    assert cased == set()


def test_every_datatype_the_dictionary_names_is_projected(registry: FixRegistry) -> None:
    """A FIX datatype the map does not know reads as a string, which is a guess.

    The dump is what says which spellings exist -- forty of them across nine
    versions, the dictionary's own misspellings included -- so it is also what
    says whether a guess is being made, and for what.
    """
    spelled = {
        member.fix["type"]
        for version in VERSIONS
        for member in registry.fields(version)
        if member.fix.get("type")
    }
    assert {"char", "String", "Price", "UTCTimestamp", "Boolean"} <= spelled
    guessed = sorted(spelling for spelling in spelled if spelling.lower() not in FIX_SCALARS)
    assert guessed == ["Stirng", "month"], "both are a string either way"
    assert registry.field("RatioQty", "4.3").arrow_type == pyarrow.float64(), (
        "`Quantity` is the dictionary's spelling of Qty, and a quantity is not text"
    )
    assert registry.field("MaturityDay", "4.1").arrow_type == pyarrow.int64()
    assert registry.field("LegFutSettDate", "4.3").arrow_type == pyarrow.date32()


# -- the second source --------------------------------------------------------


def test_the_published_dictionary_carries_the_symbol_beside_the_description(
    registry: OfflineRegistry,
) -> None:
    """Both halves of a value, from the two sources that each have one.

    The site writes `Side <54>` value `1` as "Buy", for a person; the QuickFIX
    spec writes it as `BUY`, for a program. A consumer of this archive that
    wants an identifier should not have to invent one by upper-casing prose.
    """
    side = registry.field(54, "4.4")
    assert json.loads(side.fix["values"])["1"] == "Buy"
    assert json.loads(side.fix["value_names"])["1"] == "BUY"
    assert json.loads(side.fix["value_names"])["3"] == "BUY_MINUS"


def test_every_version_published_here_has_its_symbols(registry: OfflineRegistry) -> None:
    """Derived from the archive, then pinned: a version that lost them fails here."""
    counted = {
        version: sum(1 for member in registry.fields(version) if member.fix.get("value_names"))
        for version in registry.versions
    }
    assert counted == {
        "4.0": 39,
        "4.1": 53,
        "4.2": 104,
        "4.3": 159,
        "4.4": 245,
        "5.0": 290,
        "5.0.SP1": 327,
        "5.0.SP2": 355,
        "FIXT1.1": 9,
    }


def test_a_symbol_is_never_written_where_a_description_goes(registry: OfflineRegistry) -> None:
    """The failure this shape exists to prevent, checked across the archive.

    `description=` in the spec holds the value's *name*, so a merge that wrote
    it into `values` would replace every prose reading with SHOUTING_SNAKE.
    """
    shouted = 0
    for version in registry.versions:
        for member in registry.fields(version):
            for text in json.loads(member.fix.get("values") or "{}").values():
                if text and text.isupper() and "_" in text:
                    shouted += 1
    assert shouted == 0


def test_the_published_dictionary_says_what_every_message_carries(
    registry: OfflineRegistry,
) -> None:
    """The spec's own header and trailer, stored so it works offline."""
    session = registry.session("4.4")
    assert [name for name, required in session if required] == [
        "BeginString",
        "BodyLength",
        "MsgType",
        "SenderCompID",
        "TargetCompID",
        "MsgSeqNum",
        "SendingTime",
        "CheckSum",
    ]
    assert ("PossDupFlag", False) in session

    # And the versions from 5.0 on carry **no** session layer of their own,
    # because FIXT 1.1 carries it for them -- that is the split the transport
    # version exists for, and the spec says so with an empty `<header/>`.
    carried = {version for version in registry.versions if registry.session(version)}
    assert carried == {"4.0", "4.1", "4.2", "4.3", "4.4", "FIXT1.1"}
    assert len(registry.session("FIXT1.1")) > len(registry.session("4.0"))
