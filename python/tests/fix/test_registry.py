"""`FixRegistry` against fixture pages: scraping, the cache, lookup and search.

The fixtures under `fixtures/` mirror the OnixS layout -- the version index,
one by-tag page, one field page per tag -- so no test touches the network:
`FixtureRegistry` serves files where the real one fetches URLs, and
`OfflineRegistry` refuses the network outright to prove the cache carries
everything.

Those pages are written by hand, which is how the scrape came to read values
out of `<li>` items and prose out of whatever followed the type line: the live
site carries neither. So the parsing is pinned a second time, by
`CaptureRegistry`, against pages captured from the site unedited.
"""

import email.message
import json
import re
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix import FixRegistry
from rekep.fix.registry import _is_transient, _levenshtein, _version_key, _wait_for

from .conftest import FIXTURES, fixture_page

#: Pages captured from `onixs.biz/fix-dictionary/4.0/` on 2026-08-21, byte for
#: byte (`.gitattributes` keeps them out of the line-ending normalisation):
#: the by-tag page and three field pages -- an enumerated one, one with no
#: enumeration, and MsgType, whose values *are* message links.
CAPTURE = FIXTURES / "capture"

#: Derived from the by-tag fixture, then pinned: four fields are listed, and a
#: broken link regex cannot move both sides of the assertion together.
EXPECTED_FIELDS = 4


class FixtureRegistry(FixRegistry):
    """The real registry over local fixture files instead of the network."""

    fetched: list[str]

    def _fetch(self, url: str) -> str:
        self.__dict__.setdefault("fetched", []).append(url)
        return fixture_page(url)


class CaptureRegistry(FixRegistry):
    """The real registry over captured pages, and nothing else.

    Only `4.0` was captured, and only three of its field pages: every other
    fetch raises, which is the same path a page the site cannot serve takes.
    The index is not captured either, so these tests name their version and
    never walk `versions`.
    """

    def _fetch(self, url: str) -> str:
        version, _, name = url.rpartition("/")
        path = CAPTURE / version.rsplit("/", 1)[-1] / name
        if not path.exists():
            raise OSError(f"404 {url}")
        return path.read_bytes().decode("utf-8", "replace")


def _refused(url: str, code: int = 429) -> urllib.error.HTTPError:
    """The site's "later": a `429`, with no `Retry-After` unless one is set."""
    return urllib.error.HTTPError(url, code, "Too Many Requests", email.message.Message(), None)


class RefusingRegistry(FixRegistry):
    """A registry the site refuses `refusals` times, then serves.

    `_read` is the seam and not `_fetch`, so what runs is the retrying itself.
    """

    #: How many answers are a refusal, and what every attempt was asked for.
    refusals: int = 2
    asked: list[str]

    def _read(self, request: urllib.request.Request) -> str:
        asked = self.__dict__.setdefault("asked", [])
        asked.append(request.full_url)
        if len(asked) <= self.refusals:
            raise _refused(request.full_url)
        return (FIXTURES / "tagNum_54.html").read_text()


class ThrottledRegistry(FixtureRegistry):
    """Fixture pages, except the field pages: those are refused, always."""

    def _fetch(self, url: str) -> str:
        if "tagNum_" in url:
            raise _refused(url)
        return super()._fetch(url)


class OfflineRegistry(FixRegistry):
    """A registry that must answer from the cache alone."""

    def _fetch(self, url: str) -> str:
        raise OSError(f"offline: {url}")


@pytest.fixture
def registry(tmp_path: Path) -> FixtureRegistry:
    return FixtureRegistry(cache_dir=tmp_path / "fix")


@pytest.fixture
def captured(tmp_path: Path) -> dict[int, Field]:
    """Every field of the captured version, by tag."""
    registry = CaptureRegistry(cache_dir=tmp_path / "capture")
    return {int(member.fix["tag"]): member for member in registry.fields("4.0")}


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


# -- the live layout ---------------------------------------------------------


def test_the_capture_lists_every_field_its_by_tag_page_links(tmp_path: Path) -> None:
    """Derived from the captured page, then pinned: 4.0 lists 139 fields."""
    page = (CAPTURE / "4.0" / "fields_by_tag.html").read_bytes().decode()
    linked = {int(tag) for tag in re.findall(r'href="tagNum_(\d+)\.html"', page)}
    assert len(linked) == 139
    registry = CaptureRegistry(cache_dir=tmp_path / "capture")
    assert {int(member.fix["tag"]) for member in registry.fields("4.0")} == linked


def test_a_captured_page_fills_description_and_paragraph_values(
    captured: dict[int, Field],
) -> None:
    """The live pages spell an enumeration as paragraphs, not as a list."""
    side = captured[54]
    assert side.name == "Side"
    assert side.description == "Side of order."
    assert json.loads(side.fix["values"]) == {
        "1": "Buy",
        "2": "Sell",
        "3": "Buy minus",
        "4": "Sell plus",
        "5": "Sell short",
        "6": "Sell short exempt",
    }
    assert json.loads(side.fix["used_in"])[:2] == ["Indication of Interest", "Execution Report"]


def test_a_captured_page_with_no_enumeration_still_has_its_prose(
    captured: dict[int, Field],
) -> None:
    """The prose sits under a `Description` heading, not beside the type."""
    account = captured[1]
    assert account.description == "Account mnemonic as agreed between broker and institution."
    assert "values" not in account.fix
    assert "Order Cancel/Replace Request" in json.loads(account.fix["used_in"])


def test_message_links_that_are_values_are_not_read_as_messages(
    captured: dict[int, Field],
) -> None:
    """MsgType lists its messages *as values*, and its own Used In is empty."""
    msg_type = captured[35]
    values = json.loads(msg_type.fix["values"])
    assert values["0"] == "Heartbeat <0>"
    assert values["D"] == "New Order - Single <D>"
    assert "used_in" not in msg_type.fix, "the value links belong to the enumeration"


def test_every_captured_page_that_was_read_carries_a_description(
    captured: dict[int, Field],
) -> None:
    """The three captured pages are read; the 136 that were not are still fields."""
    described = [member for member in captured.values() if member.description]
    assert len(described) == 3
    assert sorted(member.name for member in described) == ["Account", "MsgType", "Side"]


# -- being throttled ---------------------------------------------------------


def test_a_refused_page_is_asked_for_again(tmp_path: Path) -> None:
    """Two `429`s and the third answer is the page: the scrape rides it out."""
    registry = RefusingRegistry(cache_dir=tmp_path / "fix", backoff=0.0)
    assert "Side of order." in registry._fetch("https://example.test/tagNum_54.html")
    assert len(registry.asked) == 3


def test_a_page_refused_past_the_retries_raises(tmp_path: Path) -> None:
    """The last attempt is the caller's, so a site that stays shut is reported."""
    registry = RefusingRegistry(cache_dir=tmp_path / "fix", backoff=0.0, retries=2)
    registry.refusals = 99
    with pytest.raises(urllib.error.HTTPError, match="429"):
        registry._fetch("https://example.test/tagNum_54.html")
    assert len(registry.asked) == 3, "the retries, and then the attempt that raises"


def test_a_throttled_field_page_fails_the_version(tmp_path: Path) -> None:
    """A refusal is not an absent page.

    Swallowed, it caches a field with no type and no description and answers
    every later call from it -- which is how a whole scrape came back a fifth
    empty with nothing to say so.
    """
    registry = ThrottledRegistry(cache_dir=tmp_path / "fix", backoff=0.0, retries=0)
    with pytest.raises(urllib.error.HTTPError, match="429"):
        registry.fields("4.4")
    assert not (Path(registry.cache_dir) / "4.4.json").exists(), "nothing half-scraped is kept"


def test_the_pause_is_what_the_site_asked_for(tmp_path: Path) -> None:
    """`Retry-After` wins over the backoff, up to the ceiling; a date does not."""
    refused = _refused("https://example.test/x")
    assert _wait_for(refused, 2.0) == 2.0, "no header: the caller's pause"
    refused.headers["Retry-After"] = "5"
    assert _wait_for(refused, 2.0) == 5.0
    refused.headers.replace_header("Retry-After", "9999")
    assert _wait_for(refused, 2.0) == 60.0, "an absurd pause is capped, not obeyed"
    refused.headers.replace_header("Retry-After", "Wed, 21 Oct 2026 07:28:00 GMT")
    assert _wait_for(refused, 2.0) == 2.0, "a date is the site's clock, not ours"


def test_only_the_answers_that_mean_later_are_retried() -> None:
    assert _is_transient(_refused("u", 503))
    assert _is_transient(TimeoutError())
    assert not _is_transient(_refused("u", 404)), "a page that is not there is not coming"
    assert not _is_transient(OSError("404 u")), "and neither is anything else"


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
