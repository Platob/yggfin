"""The FIX dictionary under `data/` is checked here, so a bad scrape cannot ship.

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
from pathlib import Path

import pyarrow
import pytest

from rekep import Field
from rekep.fix import FIX_SCALARS, FixRegistry, SqliteFixRegistry

#: The dictionary is at the repo root, beside `python/` -- published data,
#: not something shipped in the wheel.
DATA = Path(__file__).resolve().parents[2] / "data" / "fix"

VERSIONS: list[str] = json.loads((DATA / "versions.json").read_text("utf-8"))["versions"]

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


def test_the_directory_holds_the_versions_it_lists() -> None:
    assert len(VERSIONS) == EXPECTED_VERSIONS
    dumped = {path.stem for path in DATA.glob("*.json")} - {"versions"}
    assert dumped == set(VERSIONS)
    assert VERSIONS[0] == "5.0.SP2", "newest first"
    assert VERSIONS[-1] == "FIXT1.1", "and the transport last"


@pytest.mark.parametrize("version", VERSIONS)
def test_a_version_file_holds_the_fields_it_says(version: str) -> None:
    dumped = json.loads((DATA / f"{version}.json").read_text("utf-8"))
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


def test_the_dump_indexes_into_sqlite_and_answers_the_same(
    registry: FixRegistry, tmp_path: Path
) -> None:
    """The other way to read this directory, over the whole of it.

    `tests/fix/test_sqlite.py` pins the two registries against each other on
    four fixture fields; this is the same comparison over 6,479 real ones,
    where a name declared in nine versions, a description with a `%` in it and
    a tag that is four digits all exist for real.
    """
    with SqliteFixRegistry(cache_dir=DATA, database=tmp_path / "fix.db", retries=0) as indexed:
        assert indexed.load() == {version: len(registry.fields(version)) for version in VERSIONS}
        assert indexed.versions == registry.versions
        assert indexed.tags() == registry.tags()
        assert indexed.lookup("Side") == registry.lookup("Side")
        assert indexed.field(35) == registry.field(35)
        for text in ("reject", "side", 54, "Sied", "100%", "px", "time"):
            assert indexed.search(text) == registry.search(text), text
        for version in VERSIONS:
            assert indexed.fields(version) == registry.fields(version), version


def test_no_description_needs_more_case_folding_than_sqlite_does(
    registry: FixRegistry,
) -> None:
    """The one assumption the indexed search rests on, checked rather than hoped.

    A search matches descriptions with SQL `LIKE`, which folds case for ASCII
    and nothing else, where Python's `lower()` folds everything. The dump
    carries no cased character outside ASCII -- so the two agree -- and this
    is what says so if a refresh ever brings one in.
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
