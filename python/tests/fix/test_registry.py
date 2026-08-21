"""`FixRegistry` against fixture pages: scraping, the cache, lookup and search.

The fixtures under `fixtures/` mirror the OnixS layout -- the version index,
one by-tag page, one field page per tag -- so no test touches the network:
`FixtureRegistry` serves files where the real one fetches URLs, and
`OfflineRegistry` refuses the network outright to prove the cache carries
everything.
"""

import json
from pathlib import Path

import pyarrow
import pytest

from rekep.fix import FixRegistry
from rekep.fix.registry import _levenshtein, _version_key

FIXTURES = Path(__file__).parent / "fixtures"

#: Derived from the by-tag fixture, then pinned: four fields are listed, and a
#: broken link regex cannot move both sides of the assertion together.
EXPECTED_FIELDS = 4


class FixtureRegistry(FixRegistry):
    """The real registry over local fixture files instead of the network."""

    fetched: list[str]

    def _fetch(self, url: str) -> str:
        self.__dict__.setdefault("fetched", []).append(url)
        if url.endswith("fix-dictionary.html"):
            name = "fix-dictionary.html"
        elif "/4.4/" in url:
            name = url.rsplit("/", 1)[-1]
        else:
            # Only 4.4 has fixture pages, so the other versions behave like a
            # version the network cannot serve right now.
            raise OSError(f"404 {url}")
        path = FIXTURES / name
        if not path.exists():
            raise OSError(f"404 {url}")
        return path.read_text()


class OfflineRegistry(FixRegistry):
    """A registry that must answer from the cache alone."""

    def _fetch(self, url: str) -> str:
        raise OSError(f"offline: {url}")


@pytest.fixture
def registry(tmp_path: Path) -> FixtureRegistry:
    return FixtureRegistry(cache_dir=tmp_path / "fix")


# -- versions ----------------------------------------------------------------


def test_versions_come_back_newest_first_transport_last(registry: FixtureRegistry) -> None:
    assert registry.versions == ("5.0.SP2", "5.0", "4.4", "4.2", "4.0", "FIXT1.1")


def test_the_latest_alias_is_not_a_version(registry: FixtureRegistry) -> None:
    assert "latest" not in registry.versions


def test_version_ordering_reads_the_sp_suffix() -> None:
    ordered = sorted(["4.0", "5.0.SP2", "FIXT1.1", "5.0", "4.4"], key=_version_key, reverse=True)
    assert ordered == ["5.0.SP2", "5.0", "4.4", "4.0", "FIXT1.1"]


# -- scraping ----------------------------------------------------------------


def test_a_version_scrapes_every_listed_field(registry: FixtureRegistry) -> None:
    fields = registry.fields("4.4")
    assert len(fields) == EXPECTED_FIELDS
    assert [int(member.fix["tag"]) for member in fields] == [43, 54, 103, 205]


def test_the_field_pages_fill_name_type_comment_and_values(registry: FixtureRegistry) -> None:
    side = registry.field("Side", "4.4")
    assert side.name == "Side"
    assert side.arrow_type == pyarrow.string(), "char projects to string"
    assert side.nullable
    assert side.description == "Side of order."
    assert side.fix["type"] == "char"
    assert json.loads(side.fix["values"])["1"] == "Buy"
    assert "Execution Report" in json.loads(side.fix["used_in"])


def test_a_boolean_field_projects_to_arrow_bool(registry: FixtureRegistry) -> None:
    assert registry.field(43, "4.4").arrow_type == pyarrow.bool_()


def test_a_missing_field_page_still_yields_the_field(registry: FixtureRegistry) -> None:
    """Tag 205 has no fixture page: the by-tag row alone is still a field."""
    maturity = registry.field(205, "4.4")
    assert maturity.name == "MaturityDay"
    assert maturity.arrow_type == pyarrow.string()
    assert maturity.fix["note"] == "no longer used", "the parenthetical is annotation, not name"


# -- the cache ---------------------------------------------------------------


def test_a_second_call_answers_from_the_cache(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    fetched = len(registry.fetched)
    registry.fields("4.4")
    assert len(registry.fetched) == fetched, "no page is fetched twice"
    assert (Path(registry.cache_dir) / "4.4.json").exists()


def test_the_cache_survives_offline(registry: FixtureRegistry) -> None:
    assert registry.versions  # touched, so versions.json lands in the cache
    registry.fields("4.4")
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert offline.versions == registry.versions
    assert offline.field("Side").fix["tag"] == "54"


def test_offline_with_only_field_caches_still_knows_its_versions(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")  # versions.json deliberately never written
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert offline.versions == ("4.4",)
    assert offline.field("Side").fix["version"] == "4.4"


def test_a_torn_cache_file_is_scraped_over(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    (Path(registry.cache_dir) / "4.4.json").write_text("{ torn")
    assert len(registry.fields("4.4")) == EXPECTED_FIELDS


def test_refresh_scrapes_again(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    fetched = len(registry.fetched)
    registry.fields("4.4", refresh=True)
    assert len(registry.fetched) > fetched


def test_load_reports_field_counts(registry: FixtureRegistry) -> None:
    assert registry.load("4.4") == {"4.4": EXPECTED_FIELDS}


# -- lookup ------------------------------------------------------------------


def test_lookup_by_tag_name_and_case(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    assert registry.field(54, "4.4").name == "Side"
    assert registry.field("54", "4.4").name == "Side"
    assert registry.field("side", "4.4").name == "Side"
    assert registry.field("SIDE", "4.4").name == "Side"


def test_the_version_filter_is_case_insensitive_too(registry: FixtureRegistry) -> None:
    """`fixt1.1` names `FIXT1.1`; a version that exists in no case is refused."""
    assert registry.lookup("Side", "fixt1.1") == [], "resolved, and that version has no pages"
    with pytest.raises(KeyError, match="not a FIX version"):
        registry.lookup("Side", "9.9")


def test_the_cache_answers_a_version_in_any_case(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    cached = Path(registry.cache_dir) / "4.4.json"
    (Path(registry.cache_dir) / "FIXT1.1.json").write_text(cached.read_text())
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert [f.name for f in offline.fields("fixt1.1")] == [f.name for f in offline.fields("4.4")]


def test_tags_maps_lowercased_names_to_tag_numbers(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    tags = registry.tags()
    assert tags["side"] == 54
    assert tags["possdupflag"] == 43
    assert registry.tags("4.4")["ordrejreason"] == 103


def test_tags_for_an_explicit_version_raises_when_it_cannot_load(
    registry: FixtureRegistry,
) -> None:
    """An empty mapping would silently un-resolve every key downstream."""
    with pytest.raises(OSError, match="404"):
        registry.tags("FIXT1.1")


def test_a_version_that_would_be_a_path_is_refused(registry: FixtureRegistry) -> None:
    """The version lands in a cache file name, so it must never carry a path."""
    for hostile in ("../evil", "a/b", "..", ""):
        with pytest.raises(ValueError, match="does not name a FIX version"):
            registry.fields(hostile)


def test_duplicate_case_spellings_in_the_cache_are_one_version(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")
    cached = (Path(registry.cache_dir) / "4.4.json").read_text()
    (Path(registry.cache_dir) / "FIXT1.1.json").write_text(cached)
    (Path(registry.cache_dir) / "fixt1.1.json").write_text(cached)
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert [version.lower() for version in offline.versions] == ["4.4", "fixt1.1"]


def test_lookup_without_a_version_walks_them_newest_first(registry: FixtureRegistry) -> None:
    """Only 4.4 has pages here, so the walk must skip the versions it cannot get."""
    registry.fields("4.4")
    found = registry.lookup("Side")
    assert [member.fix["version"] for member in found] == ["4.4"]


def test_an_unknown_version_is_refused_by_name(registry: FixtureRegistry) -> None:
    with pytest.raises(KeyError, match="not a FIX version"):
        registry.lookup("Side", "9.9")


def test_an_unknown_field_raises_key_error(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    with pytest.raises(KeyError, match="NoSuchField"):
        registry.field("NoSuchField")


# -- search ------------------------------------------------------------------


def test_search_matches_name_tag_and_description_case_insensitively(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")
    assert [f.name for f in registry.search("side")] == ["Side"]
    assert [f.name for f in registry.search(54)] == ["Side"]
    assert "OrdRejReason" in [f.name for f in registry.search("REJECTION")]


def test_search_ranks_exact_before_prefix_before_substring(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    found = [f.name for f in registry.search("Maturity")]
    assert found[0] == "MaturityDay"


def test_search_falls_back_to_levenshtein_only_when_nothing_matches(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")
    assert [f.name for f in registry.search("Sied")] == ["Side"]
    assert registry.search("Sied", fuzzy=False) == []
    assert registry.search("zzzzzz") == []


def test_levenshtein_stops_at_its_ceiling() -> None:
    assert _levenshtein("side", "side", 2) == 0
    assert _levenshtein("sied", "side", 2) == 2
    assert _levenshtein("abcdef", "zzzzzz", 2) is None
    assert _levenshtein("a", "abcdefgh", 2) is None, "the length gap alone settles it"
