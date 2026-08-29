"""`FixRegistry` against fixture pages: scraping, the cache, lookup and search.

The fixtures under `fixtures/` mirror the registry source layouts, so no test
touches the network. `FixtureRegistry` serves files where the real one fetches
URLs, and `OfflineRegistry` refuses the network to prove the cache carries
everything.

Those pages are written by hand, which is how the scrape came to read values
out of `<li>` items and prose out of whatever followed the type line: the live
site carries neither. So the parsing is pinned a second time, by
`CaptureRegistry`, against pages captured from the site unedited.
"""

import dataclasses
import email.message
import json
import re
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep.enums import EventType, State
from rekep.fields import Field, newest_rank
from rekep.fix import (
    ANY_VERSION,
    Alias,
    ComponentRecord,
    ConflictReport,
    FixFieldValue,
    FixRegistry,
    NanocondaSource,
    OnixSSource,
    QuickFixSource,
    RegistrySource,
)
from rekep.fix.fields import fix_field, namespaced_field
from rekep.fix.quickfix import block, is_group, members_of
from rekep.fix.registry import _is_missing, _is_transient, _levenshtein, _wait_for
from rekep.fix.store import Collision

from .conftest import FIXTURES, fixture_page

#: Pages captured from `onixs.biz/fix-dictionary/4.0/` on 2026-08-21, byte for
#: byte (`.gitattributes` keeps them out of the line-ending normalisation):
#: the by-tag page and three field pages -- an enumerated one, one with no
#: enumeration, and MsgType, whose values *are* message links.
CAPTURE = FIXTURES / "capture"
PUBLISHED = Path(__file__).resolve().parents[3] / "data"

#: Derived from field pages the fixture carries, then pinned. The fourth
#: by-tag link has no page and therefore contributes no field.
EXPECTED_LISTED = 3

#: The spec adds fields used by its component declarations and one extra enum.
EXPECTED_SPEC_ONLY = 8
EXPECTED_FIELDS = EXPECTED_LISTED + EXPECTED_SPEC_ONLY

#: What a store of those eleven fields holds: two tag shards (the fixture's
#: tags straddle 500), two components, one message and the version index.
#: Derived from the fixture, then pinned, so a layout that stopped sharding
#: fails here.
EXPECTED_DOCUMENTS = 6

#: Where the fixture's `Side <54>` lands: tags 0 to 499.
SIDE_SHARD = "fields/000000.json"


class FixtureRegistry(FixRegistry):
    """The real registry over local fixture files instead of the network."""

    fetched: list[str]

    def __post_init__(self, registry_token: str | None) -> None:
        self.sources = tuple(
            dataclasses.replace(source, field_pause_seconds=0.0) for source in self.sources
        )
        super().__post_init__(registry_token)

    def _fetch(self, url: str) -> str:
        self.__dict__.setdefault("fetched", []).append(url)
        return fixture_page(url)


class CaptureRegistry(FixtureRegistry):
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


class ResumeSource(RegistrySource):
    """Two fields whose source pages make interrupted scrapes observable."""

    def versions(self, fetch) -> tuple[str, ...]:
        if fetch(f"{self.url}/versions") != "versions":
            raise ValueError("the source version layout changed")
        return ("4.4",)

    def tags(self, fetch, _version: str) -> dict[int, tuple[str, str]]:
        if fetch(f"{self.url}/4.4/tags") != "tags":
            raise ValueError("the source tag layout changed")
        return {1: ("Account", ""), 54: ("Side", "")}

    def field(self, fetch, _version: str, tag: int) -> dict[str, object]:
        if fetch(f"{self.url}/4.4/fields/{tag}") != f"field {tag}":
            raise ValueError(f"the source field {tag} layout changed")
        return {"name": "Account" if tag == 1 else "Side", "type": "String"}


def _resume_page(url: str) -> str:
    """The valid fixture answer for one `ResumeSource` URL."""
    if url.endswith("/versions"):
        return "versions"
    if url.endswith("/tags"):
        return "tags"
    return f"field {url.rsplit('/', 1)[-1]}"


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
    assert registry.versions == (
        "5.0.SP2",
        "5.0.SP1",
        "5.0",
        "4.4",
        "4.3",
        "4.2",
        "4.1",
        "4.0",
        "FIXT1.1",
    )


def test_the_latest_alias_is_not_a_version(registry: FixtureRegistry) -> None:
    assert "latest" not in registry.versions


def test_version_ordering_reads_the_sp_suffix() -> None:
    ordered = sorted(["4.0", "5.0.SP2", "FIXT1.1", "5.0", "4.4"], key=newest_rank, reverse=True)
    assert ordered == ["5.0.SP2", "5.0", "4.4", "4.0", "FIXT1.1"]


# -- scraping ----------------------------------------------------------------


def test_nanoconda_names_every_value_once_and_casefolds_the_lookup(tmp_path: Path) -> None:
    registry = FixtureRegistry(
        sources=(NanocondaSource(),),
        cache_dir=tmp_path / "nanoconda",
    )
    assert [member.name for member in registry.fields("4.2")] == ["ExecType"]
    exec_type = registry.field("ExecType", "4.2")
    assert exec_type.fix.type == "char"
    assert exec_type.fix.added == "4.1"
    assert exec_type.description == "Describes the specific execution report event."
    assert exec_type.fix.msgtypes == ("ExecutionReport",)
    assert exec_type.fix.source == "nanoconda"
    assert exec_type.fix.sources == ("nanoconda",)
    assert exec_type.fix.source_of("added") == "nanoconda"
    assert [(value.value, value.meaning, value.aliases) for value in exec_type.fix.enumerated] == [
        ("1", "Partial fill", ("PartialFill",)),
        ("2", "Fill", ("Fill",)),
    ]
    assert exec_type.fix.value_of("2").meaning == "Fill"
    assert exec_type.fix.value_of("2").aliases == ("Fill",)
    assert exec_type.fix.encode("fill") == "2"
    assert exec_type.fix.encode("FILL") == "2"
    assert "_source_pages" not in registry.__dict__


@pytest.mark.parametrize("source", [NanocondaSource(), OnixSSource()])
def test_a_source_index_with_unknown_markup_fails(source: RegistrySource) -> None:
    with pytest.raises(ValueError, match="lists no FIX versions"):
        source.versions(lambda _url: "<html>sign in</html>")


@pytest.mark.parametrize(
    ("source", "version"), [(NanocondaSource(), "4.2"), (OnixSSource(), "4.4")]
)
def test_a_source_field_list_with_unknown_markup_fails(
    source: RegistrySource, version: str
) -> None:
    with pytest.raises(ValueError, match="lists no FIX fields"):
        source.tags(lambda _url: "<html>sign in</html>", version)


@pytest.mark.parametrize(
    ("source", "version"), [(NanocondaSource(), "4.2"), (OnixSSource(), "4.4")]
)
def test_a_present_field_page_with_unknown_markup_fails(
    source: RegistrySource, version: str
) -> None:
    with pytest.raises(ValueError, match="does not describe FIX field 150"):
        source.field(lambda _url: "<html>sign in</html>", version, 150)


def test_quickfix_usage_follows_components_messages_and_session_parts() -> None:
    document = """
    <fix>
      <header><field name='HeaderID' required='Y'/></header>
      <trailer><field name='TrailerID' required='Y'/></trailer>
      <messages>
        <message name='NestedMessage' msgtype='U1'>
          <component name='Outer' required='N'/>
        </message>
      </messages>
      <components>
        <component name='Outer'><component name='Inner' required='N'/></component>
        <component name='Inner'><field name='NestedID' required='N'/></component>
      </components>
      <fields>
        <field number='1' name='HeaderID' type='STRING'/>
        <field number='2' name='TrailerID' type='STRING'/>
        <field number='3' name='NestedID' type='STRING'/>
      </fields>
    </fix>
    """
    source = QuickFixSource()

    header = source.field(lambda _url: document, "4.4", 1)
    trailer = source.field(lambda _url: document, "4.4", 2)
    nested = source.field(lambda _url: document, "4.4", 3)

    assert header["components"] == ("StandardHeader",)
    assert trailer["components"] == ("StandardTrailer",)
    assert nested["components"] == ("Inner", "Outer")
    assert nested["used_in"] == ("NestedMessage",)


def test_a_full_rebuild_fetches_each_shared_source_page_once(tmp_path: Path) -> None:
    class Shared(RegistrySource):
        def versions(self, fetch) -> tuple[str, ...]:
            assert fetch(f"{self.url}/versions")
            return ("4.4", "4.2")

        def tags(self, fetch, _version: str) -> dict[int, tuple[str, str]]:
            assert fetch(f"{self.url}/tags")
            return {tag: (f"Field{tag}", "") for tag in range(100, 124)}

        def field(self, fetch, _version: str, tag: int) -> dict[str, object]:
            assert fetch(f"{self.url}/fields")
            return {"name": f"Field{tag}", "type": "char"}

    class Counting(FixRegistry):
        fetched: list[str]

        def _fetch(self, url: str) -> str:
            self.__dict__.setdefault("fetched", []).append(url)
            if url.endswith("/fields"):
                time.sleep(0.01)
            return "fields"

    registry = Counting(
        sources=(
            Shared(
                name="shared",
                url="https://shared.example",
                workers=8,
                shared_field_page=True,
            ),
        ),
        cache_dir=tmp_path / "shared",
    )

    registry.rebuild()

    assert registry.fetched == [
        f"{registry.sources[0].url}/versions",
        f"{registry.sources[0].url}/tags",
        f"{registry.sources[0].url}/fields",
    ]
    assert "_source_pages" not in registry.__dict__


def test_a_source_spaces_each_field_page_by_its_own_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paused: list[float] = []

    class Spaced(RegistrySource):
        def versions(self, _fetch) -> tuple[str, ...]:
            return ("4.4",)

        def tags(self, _fetch, _version: str) -> dict[int, tuple[str, str]]:
            return {1: ("Account", ""), 54: ("Side", "")}

        def field(self, fetch, _version: str, tag: int) -> dict[str, object]:
            assert fetch(f"{self.url}/{tag}") == "field"
            return {"name": "Account" if tag == 1 else "Side", "type": "String"}

    class Fetched(FixRegistry):
        def _fetch(self, _url: str) -> str:
            return "field"

    monkeypatch.setattr(time, "sleep", paused.append)
    registry = Fetched(
        cache_dir=tmp_path / "fix",
        sources=(Spaced("spaced", "https://spaced.example", field_pause_seconds=0.25),),
    )

    registry.rebuild()

    assert paused == [0.25, 0.25]


def test_a_source_field_pause_cannot_be_negative(tmp_path: Path) -> None:
    source = RegistrySource("broken", "https://broken.example", field_pause_seconds=-1.0)
    with pytest.raises(ValueError, match="field pauses cannot be negative"):
        FixRegistry(cache_dir=tmp_path / "fix", sources=(source,))


def test_sources_merge_in_priority_order_and_attribute_conflicts(tmp_path: Path) -> None:
    class Nanoconda(RegistrySource):
        def versions(self, _fetch) -> tuple[str, ...]:
            return ("4.2",)

        def tags(self, _fetch, _version: str) -> dict[int, tuple[str, str]]:
            return {150: ("ExecType", "")}

        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            return {
                "name": "ExecType",
                "description": "The execution event type.",
                "values": (FixFieldValue("2", "Fill", ("Fill",)),),
            }

    class OnixS(Nanoconda):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            return {
                "name": "ExecutionType",
                "type": "char",
                "description": "A fallback description.",
                "values": (FixFieldValue("2", "Trade", ("EXECUTION_FILL",)),),
            }

    class QuickFIX(Nanoconda):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            return {
                "name": "ExecType",
                "type": "String",
                "values": (FixFieldValue("2", aliases=("FILL",)),),
            }

    class Missing(Nanoconda):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            return {}

    registry = FixRegistry(
        cache_dir=tmp_path / "fix",
        sources=(
            Missing("missing", "https://missing.example"),
            Nanoconda("nanoconda", "https://nanoconda.example"),
            OnixS("onixs", "https://onixs.example"),
            QuickFIX(
                "quickfix",
                "https://quickfix.example",
                shared_field_page=True,
            ),
        ),
    )
    report = registry.rebuild("4.2")
    exec_type = registry.field(150)

    assert exec_type.name == "ExecType"
    assert exec_type.description == "The execution event type."
    assert exec_type.fix.source == "nanoconda"
    assert exec_type.fix.sources == ("nanoconda", "onixs", "quickfix")
    assert exec_type.fix.origins == {
        "name": "nanoconda",
        "type": "onixs",
        "description": "nanoconda",
        "values": {"2": "nanoconda"},
        "aliases": {"2": "nanoconda"},
    }
    assert exec_type.fix.value_of("2").meaning == "Fill"
    assert exec_type.fix.value_of("2").aliases == ("Fill", "EXECUTION_FILL")
    by_part = {conflict.part: conflict for conflict in report.collapses}
    assert {"name", "type", "values", "aliases"} <= by_part.keys()
    assert by_part["type"].keptsource == "onixs"
    assert {dropped.source for dropped in by_part["type"].dropped} == {"quickfix"}
    for part in ("name", "values", "aliases"):
        assert by_part[part].keptsource == "nanoconda"
        assert {dropped.source for dropped in by_part[part].dropped} == {"onixs"}
    assert "_source_conflicts" not in registry.__dict__


def test_every_source_contributes_parts_the_others_do_not_carry(tmp_path: Path) -> None:
    asked: list[str] = []

    class Primary(RegistrySource):
        def versions(self, _fetch) -> tuple[str, ...]:
            return ("4.2",)

        def tags(self, _fetch, _version: str) -> dict[int, tuple[str, str]]:
            return {150: ("ExecType", "")}

        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "description": "The execution event type.",
                "values": (FixFieldValue("2", "Fill", ("Fill",)),),
            }

    class Fallback(Primary):
        def tags(self, _fetch, _version: str) -> dict[int, tuple[str, str]]:
            return {150: ("ExecType", "Deprecated")}

        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "used_in": ("ExecutionReport",),
                "components": ("FallbackComponent",),
                "values": (FixFieldValue("2", aliases=("EXECUTION_FILL",)),),
            }

    class Shared(Primary):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "values": (FixFieldValue("2", aliases=("FILL",)),),
            }

    registry = FixRegistry(
        cache_dir=tmp_path / "fix",
        sources=(
            Primary(
                "primary",
                "https://primary.example",
                shared_field_page=True,
            ),
            Fallback("fallback", "https://fallback.example"),
            Shared(
                "shared",
                "https://shared.example",
                shared_field_page=True,
            ),
        ),
    )

    registry.rebuild("4.2")

    assert asked == ["primary", "fallback", "shared"]
    stored = registry.field(150)
    assert stored.fix.sources == ("primary", "fallback", "shared")
    assert stored.fix.note == "Deprecated"
    assert stored.fix.msgtypes == ("ExecutionReport",)
    assert stored.fix.components == ("FallbackComponent",)
    assert stored.fix.value_of("2").aliases == ("Fill", "EXECUTION_FILL")


def test_source_fields_are_fetched_once_in_priority_order(tmp_path: Path) -> None:
    asked: list[str] = []

    class Primary(RegistrySource):
        def versions(self, _fetch) -> tuple[str, ...]:
            return ("4.2",)

        def tags(self, _fetch, _version: str) -> dict[int, tuple[str, str]]:
            return {150: ("ExecType", "")}

        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "description": "The execution event type.",
                "values": (FixFieldValue("2", "Fill", ("Fill",)),),
            }

    class Fallback(Primary):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "description": "The execution event type.",
                "values": (FixFieldValue("1", "Partial fill", ("PartialFill",)),),
            }

    class Shared(Primary):
        def field(self, _fetch, _version: str, _tag: int) -> dict[str, object]:
            asked.append(self.name)
            return {
                "name": "ExecType",
                "type": "char",
                "values": (
                    FixFieldValue("1", aliases=("PARTIAL_FILL",)),
                    FixFieldValue("2", aliases=("FILL",)),
                ),
            }

    registry = FixRegistry(
        cache_dir=tmp_path / "fix",
        sources=(
            Primary("primary", "https://primary.example"),
            Fallback("fallback", "https://fallback.example"),
            Shared(
                "shared",
                "https://shared.example",
                shared_field_page=True,
            ),
        ),
    )

    registry.rebuild("4.2")

    assert asked == ["primary", "fallback", "shared"]
    assert [value.value for value in registry.field(150).fix.enumerated] == ["2", "1"]


def test_scrape_replaces_a_dump_folder_from_scratch(tmp_path: Path) -> None:
    class OneVersion(FixRegistry):
        def _scrape_versions(self) -> tuple[str, ...]:
            return ("4.4",)

        def _spec_document(self, version: str) -> str:
            return ""

        def _scrape_version(self, version: str) -> list[Field]:
            return [fix_field("Side", 54, "char", version=version, values={"1": "Buy"})]

        def rebuild(self, *versions: str) -> ConflictReport:
            super().rebuild(*versions)
            report = ConflictReport(
                collisions=(
                    Collision(
                        "Side",
                        "buy",
                        ("1", "2"),
                        54,
                        ("fixture", "fixture"),
                    ),
                )
            )
            self.__dict__["_conflicts"] = report
            return report

    target = tmp_path / "fix"
    target.mkdir()
    (target / "stale.json").write_text("{}")

    registry = OneVersion.scrape(target, sources=(NanocondaSource(),))

    assert registry.offline and registry.cache_dir == target
    assert registry.field("Side", "4.4").name == "Side"
    assert registry.conflicts.collisions[0].name == "Side"
    assert not (target / "stale.json").exists()


def test_scrape_preserves_local_declarations_and_standard_overlays(tmp_path: Path) -> None:
    class OneVersion(FixRegistry):
        def _scrape_versions(self) -> tuple[str, ...]:
            return ("4.4",)

        def _scrape_version(self, version: str) -> list[Field]:
            return [
                fix_field("MsgType", 35, "String", version=version, values={"D": "Order"}),
                fix_field("OrderSide", 54, "char", version=version),
            ]

    target = tmp_path / "fix"
    held = FixRegistry(cache_dir=target, offline=True)
    msg_type = fix_field("MsgType", 35, "String", version="4.2", values={"D": "Order"})
    msg_type.fix.event_types = {"D": EventType.ORDER}
    side = fix_field("Side", 54, "char", version="4.2")
    side.fix.states = {"1": State.NEW}
    side.fix.column = "sidecode"
    side.fix.named_aliases = (
        Alias("OldSide", source="4.2"),
        Alias("VenueSide", source="venue"),
    )
    held._store_fields("4.2", [msg_type, side])
    held.add_field(namespaced_field("LOCAL.CODE", "String", column="localcode"))
    held.add_component(
        ComponentRecord.from_components([block("LocalMessage", (), "U9")], [ANY_VERSION])
    )

    scraped = OneVersion.scrape(target, sources=(NanocondaSource(),))

    assert scraped.resolve("LOCAL.CODE").fix.column == "localcode"
    assert scraped.merged_component("LocalMessage").msg_type == "U9"
    assert scraped.msg_type_event_types()["D"] is EventType.ORDER
    assert scraped.state_values("OrderSide") == {"1": State.NEW}
    assert scraped.field(54).name == "OrderSide"
    assert scraped.field(54).fix.column == "sidecode"
    assert scraped.resolve("VenueSide").fix.tag == 54
    assert scraped.resolve("OldSide") is None


def test_rebuild_replaces_source_records_and_keeps_local_declarations(tmp_path: Path) -> None:
    class OneVersion(FixRegistry):
        def _scrape_version(self, version: str) -> list[Field]:
            return [fix_field("Side", 54, "char", version=version)]

    registry = OneVersion(
        cache_dir=tmp_path / "fix",
        sources=(NanocondaSource(),),
        offline=True,
    )
    side = fix_field("Side", 54, "char", version="4.2")
    side.fix.states = {"1": State.NEW}
    registry._store_fields("4.2", [fix_field("Account", 1, "String", version="4.2"), side])
    registry.add_field(namespaced_field("LOCAL.CODE", "String", column="localcode"))
    assert registry.versions == ("4.2",)

    registry.rebuild("4.4")

    assert registry.versions == ("4.4",)
    assert registry.field(1) is None
    assert registry.field(54).name == "Side"
    assert registry.state_values("Side") == {"1": State.NEW}
    assert registry.resolve("LOCAL.CODE").fix.column == "localcode"


def test_rebuild_requires_the_configured_quickfix_document(tmp_path: Path) -> None:
    class MissingSpec(FixRegistry):
        def _spec_document(self, version: str) -> str:
            return ""

        def _scrape_version(self, version: str) -> list[Field]:
            return [fix_field("Side", 54, "char", version=version)]

    with pytest.raises(ValueError, match="QuickFIX source has no document"):
        MissingSpec(cache_dir=tmp_path / "fix").rebuild("4.4")


def test_scrape_requires_a_local_dump_folder() -> None:
    with pytest.raises(ValueError, match="local dump folder"):
        FixRegistry.scrape("s3://bucket/fix")


def test_an_interrupted_scrape_reuses_valid_pages_and_cleans_up_after_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[str] = []
    paused: list[float] = []
    state = {"refuse": True}

    class Interrupted(FixRegistry):
        def _fetch(self, url: str) -> str:
            asked.append(url)
            if state["refuse"] and url.endswith("/fields/54"):
                raise _refused(url)
            return _resume_page(url)

    monkeypatch.setattr(time, "sleep", paused.append)
    source = ResumeSource(
        "resume",
        "https://resume.example",
        workers=1,
        field_pause_seconds=0.25,
    )
    target = tmp_path / "fix"
    target.mkdir()
    sentinel = target / "held.txt"
    sentinel.write_text("published")

    with pytest.raises(urllib.error.HTTPError, match="429"):
        Interrupted.scrape(target, sources=(source,), retries=0, backoff=0.0)

    page_cache = tmp_path / ".fix-source-pages"
    assert sentinel.read_text() == "published"
    assert len(list(page_cache.rglob("*.page"))) == 3
    assert paused == [0.25, 0.25]
    first_requests = len(asked)

    state["refuse"] = False
    installed = Interrupted.scrape(target, sources=(source,), retries=0, backoff=0.0)

    assert asked[first_requests:] == ["https://resume.example/4.4/fields/54"]
    assert paused == [0.25, 0.25, 0.25], "a cached field pays no source pause"
    assert installed.field(1).name == "Account"
    assert not sentinel.exists()
    assert not page_cache.exists()


def test_source_page_resume_keys_the_exact_url(tmp_path: Path) -> None:
    asked: list[str] = []
    state = {"refuse": True}

    class Interrupted(FixRegistry):
        def _fetch(self, url: str) -> str:
            asked.append(url)
            if state["refuse"] and url.endswith("/fields/54"):
                raise _refused(url)
            return _resume_page(url)

    target = tmp_path / "fix"
    first = ResumeSource("resume", "https://first.example", workers=1)
    with pytest.raises(urllib.error.HTTPError, match="429"):
        Interrupted.scrape(target, sources=(first,), retries=0, backoff=0.0)

    state["refuse"] = False
    asked.clear()
    second = dataclasses.replace(first, url="https://second.example")
    Interrupted.scrape(target, sources=(second,), retries=0, backoff=0.0)

    assert asked == [
        "https://second.example/versions",
        "https://second.example/4.4/tags",
        "https://second.example/4.4/fields/1",
        "https://second.example/4.4/fields/54",
    ]
    assert not (tmp_path / ".fix-source-pages").exists()


def test_a_source_layout_failure_does_not_cache_its_page(tmp_path: Path) -> None:
    asked: list[str] = []
    state = {"valid": False}

    class SharedLayout(ResumeSource):
        def versions(self, fetch) -> tuple[str, ...]:
            fetch(f"{self.url}/document")
            return ("4.4",)

        def tags(self, fetch, _version: str) -> dict[int, tuple[str, str]]:
            if fetch(f"{self.url}/document") != "document":
                raise ValueError("the source tag layout changed")
            return {1: ("Account", ""), 54: ("Side", "")}

    class ChangingLayout(FixRegistry):
        def _fetch(self, url: str) -> str:
            asked.append(url)
            if not state["valid"] and url.endswith("/document"):
                return "sign in"
            if url.endswith("/document"):
                return "document"
            return _resume_page(url)

    source = SharedLayout("resume", "https://layout.example", workers=1)
    target = tmp_path / "fix"
    with pytest.raises(ValueError, match="tag layout changed"):
        ChangingLayout.scrape(target, sources=(source,))

    page_cache = tmp_path / ".fix-source-pages"
    assert not list(page_cache.rglob("*.page")), "the rejected shared page was evicted"

    state["valid"] = True
    asked.clear()
    ChangingLayout.scrape(target, sources=(source,))

    assert asked == [
        "https://layout.example/document",
        "https://layout.example/4.4/fields/1",
        "https://layout.example/4.4/fields/54",
    ]
    assert not page_cache.exists()


def test_a_version_scrapes_every_listed_field(registry: FixtureRegistry) -> None:
    """Every source, in tag order; the spec also names component fields."""
    fields = registry.fields("4.4")
    assert len(fields) == EXPECTED_FIELDS
    assert [int(member.fix["tag"]) for member in fields] == [
        43,
        54,
        103,
        447,
        448,
        452,
        453,
        523,
        802,
        803,
        828,
    ]
    assert "_source_conflicts" not in registry.__dict__


def test_the_field_pages_fill_name_type_comment_and_values(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    side = registry.field("Side", "4.4")
    assert side.name == "Side"
    assert side.dtype == pyarrow.string(), "char projects to string"
    assert side.nullable
    assert side.description == "Side of order."
    assert side.fix["type"] == "char"
    assert side.fix.value_of("1").meaning == "Buy"
    assert "ExecutionReport" in json.loads(side.fix["msgtypes"])


def test_onixs_field_usage_distinguishes_component_links() -> None:
    page = """
    <h2>FIX 5.0 SP2 : PartyID &lt;448&gt; field</h2>
    <p>Type: <a>String</a></p>
    <h3>Description</h3><p>Party identifier.</p>
    <h3>Used in</h3>
    <a href="compBlock_PartyDetail.html">&lt;PartyDetail&gt;</a>
    <a href="msgType_D_68.html">New Order - Single &lt;D&gt;</a>
    """

    detail = OnixSSource().field(lambda _url: page, "5.0.SP2", 448)

    assert detail["components"] == ["PartyDetail"]
    assert detail["used_in"] == ["NewOrderSingle"]


def test_a_boolean_field_projects_to_arrow_bool(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    assert registry.field(43, "4.4").dtype == pyarrow.bool_()


def test_a_missing_field_page_contributes_nothing(registry: FixtureRegistry) -> None:
    """Tag 205 has no fixture page, so that source does not declare it."""
    registry.fields("4.4")
    assert registry.field(205, "4.4") is None


# -- the live layout ---------------------------------------------------------


def test_the_capture_reads_only_fields_with_captured_pages(tmp_path: Path) -> None:
    """A by-tag link without a field page contributes nothing."""
    page = (CAPTURE / "4.0" / "fields_by_tag.html").read_bytes().decode()
    linked = {int(tag) for tag in re.findall(r'href="tagNum_(\d+)\.html"', page)}
    assert len(linked) == 139
    registry = CaptureRegistry(cache_dir=tmp_path / "capture")
    assert {int(member.fix["tag"]) for member in registry.fields("4.0")} == {1, 35, 54}


def test_a_captured_prose_only_value_list_is_not_an_enumeration(
    captured: dict[int, Field],
) -> None:
    """OnixS supplies meanings here, but no source supplies symbolic names."""
    side = captured[54]
    assert side.name == "Side"
    assert side.description == "Side of order."
    assert not side.fix.enumerated
    assert json.loads(side.fix["msgtypes"])[:2] == ["IndicationofInterest", "ExecutionReport"]


def test_a_captured_page_with_no_enumeration_still_has_its_prose(
    captured: dict[int, Field],
) -> None:
    """The prose sits under a `Description` heading, not beside the type."""
    account = captured[1]
    assert account.description == "Account mnemonic as agreed between broker and institution."
    assert "values" not in account.fix
    assert "OrderCancelReplaceRequest" in json.loads(account.fix["msgtypes"])


def test_message_links_that_are_values_are_not_read_as_messages(
    captured: dict[int, Field],
) -> None:
    """MsgType lists its messages *as values*, and its own Used In is empty."""
    msg_type = captured[35]
    assert msg_type.fix.value_of("0").aliases == ("Heartbeat",)
    assert msg_type.fix.value_of("D").aliases == ("NewOrderSingle",)
    assert "msgtypes" not in msg_type.fix, "the value links belong to the enumeration"


def test_every_captured_page_that_was_read_carries_a_description(
    captured: dict[int, Field],
) -> None:
    """The three captured pages are read and described."""
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
    with pytest.raises(urllib.error.HTTPError, match=r"429.*https://example\.test/tagNum_54\.html"):
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
    cached = Path(registry.cache_dir)
    assert not list((cached / "fields").glob("*.json"))
    assert not list((cached / "components").glob("*.json")), "nothing half-scraped is kept"


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
    assert _is_missing(_refused("u", 404))
    assert _is_missing(OSError("404 u"))
    assert not _is_missing(_refused("u", 403))
    assert not _is_missing(OSError("offline: https://example.test/fields/404.html"))


def test_a_forbidden_field_page_fails_the_version(tmp_path: Path) -> None:
    class ForbiddenRegistry(FixtureRegistry):
        def _fetch(self, url: str) -> str:
            if "tagNum_" in url:
                raise _refused(url, 403)
            return super()._fetch(url)

    with pytest.raises(urllib.error.HTTPError, match="403"):
        ForbiddenRegistry(cache_dir=tmp_path / "fix").fields("4.4")


# -- a directory or a zip -----------------------------------------------------


def _rooted(folder: Path, target: Path) -> Path:
    """`zip -r fix.zip fix/`: every member under the folder it came from."""
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as out:
        for path in sorted(folder.rglob("*.json")):
            out.write(path, f"fix/{path.relative_to(folder).as_posix()}")
    return target


@pytest.fixture
def dumped(registry: FixtureRegistry) -> Path:
    """The fixture dictionary as a directory of JSON, ready to be read either way."""
    assert registry.versions
    registry.load("4.4")
    return Path(registry.cache_dir)


def test_the_extension_says_which_store_this_is(tmp_path: Path) -> None:
    """Read off the path, and only off the path: a store says what it is
    before anything has been written to it."""
    assert not FixRegistry(cache_dir=tmp_path / "fix").archived
    assert FixRegistry(cache_dir=tmp_path / "fix.zip").archived
    assert FixRegistry(cache_dir=tmp_path / "FIX.ZIP").archived, "the extension, in any case"
    assert not FixRegistry(cache_dir=tmp_path / "fix.zipped").archived


def test_the_published_folder_is_the_archive_uncompressed() -> None:
    folder = PUBLISHED / "fix"
    archive = PUBLISHED / "fix.zip"
    files = {
        path.relative_to(folder).as_posix(): path.read_bytes() for path in folder.rglob("*.json")
    }
    with zipfile.ZipFile(archive) as opened:
        members = {name: opened.read(name) for name in opened.namelist()}
    assert files == members
    assert {name.split("/")[0] for name in members} == {
        "fields",
        "components",
        "versions.json",
    }, "one file per identity, in two folders beside the version index"
    unpacked = FixRegistry(cache_dir=folder, offline=True)
    zipped = FixRegistry(cache_dir=archive, offline=True)
    assert unpacked.fields_available("4.4") and zipped.fields_available("4.4")
    assert unpacked.versions == zipped.versions
    assert unpacked.field("Side", "4.4") == zipped.field("Side", "4.4")
    assert unpacked.component("Parties", "4.4") == zipped.component("Parties", "4.4")


def test_a_file_url_reads_the_original_archive_without_materializing() -> None:
    archive = (PUBLISHED / "fix.zip").resolve()
    registry = FixRegistry(cache_dir=archive.as_uri(), offline=True)

    assert Path(registry._cache_path) == archive
    assert registry.field("Side", "4.4").fix["tag"] == "54"


def test_an_arrow_filesystem_directory_is_a_registry_store() -> None:
    filesystem = pyarrow.fs._MockFileSystem()
    folder = PUBLISHED / "fix"
    for name in ("registry", "registry/fields", "registry/components"):
        filesystem.create_dir(name)
    for source in folder.rglob("*.json"):
        name = source.relative_to(folder).as_posix()
        with filesystem.open_output_stream(f"registry/{name}") as stream:
            stream.write(source.read_bytes())

    registry = FixRegistry(cache_dir="registry", filesystem=filesystem, offline=True)

    assert registry.versions
    assert registry.field("Side", "4.4").fix["tag"] == "54"


def test_a_scrape_is_cached_in_an_arrow_filesystem_directory() -> None:
    filesystem = pyarrow.fs._MockFileSystem()
    registry = FixtureRegistry(cache_dir="registry", filesystem=filesystem)
    registry.fields("4.4")

    assert registry.field("Side", "4.4").fix["tag"] == "54"

    offline = OfflineRegistry(cache_dir="registry", filesystem=filesystem, offline=True)
    assert offline.field("Side", "4.4").fix["tag"] == "54"


def test_a_remote_archive_is_materialized_once_and_reused() -> None:
    class CountingFilesystem(pyarrow.fs._MockFileSystem):
        reads = 0

        def open_input_stream(self, path, *args, **kwargs):
            self.reads += 1
            return super().open_input_stream(path, *args, **kwargs)

    filesystem = CountingFilesystem()
    filesystem.create_dir("registry")
    with filesystem.open_output_stream("registry/fix.zip") as stream:
        stream.write((PUBLISHED / "fix.zip").read_bytes())
    registry = FixRegistry(cache_dir="registry/fix.zip", filesystem=filesystem, offline=True)

    first = registry._cache_path
    assert registry.field("Side", "4.4").fix["tag"] == "54"
    assert registry._cache_path == first
    assert registry.field("Side", "4.4").fix["tag"] == "54"
    assert filesystem.reads == 1


def test_a_zip_answers_everything_the_directory_answers(dumped: Path, tmp_path: Path) -> None:
    """The reference is the directory the archive was made from."""
    folder = OfflineRegistry(cache_dir=dumped)
    archive = OfflineRegistry(cache_dir=folder.into_zip(tmp_path / "fix.zip"))
    assert archive.versions == folder.versions
    assert archive.fields("4.4") == folder.fields("4.4")
    assert archive.field("Side") == folder.field("Side")
    assert archive.tags() == folder.tags()
    assert archive.search("reject") == folder.search("reject")
    assert archive.lookup(54) == folder.lookup(54)
    assert archive.components("4.4") == folder.components("4.4")


def test_the_archive_holds_one_member_per_file(dumped: Path, tmp_path: Path) -> None:
    """One member per document, whatever the layout spells them."""
    archive = FixRegistry(cache_dir=dumped).into_zip(tmp_path / "fix.zip")
    with zipfile.ZipFile(archive) as opened:
        names = opened.namelist()
    written = [path.relative_to(dumped).as_posix() for path in dumped.rglob("*.json")]
    assert sorted(names) == sorted(written)
    # Twelve fields in two tag shards, three blocks and the version index.
    # Not a size claim -- six documents of a few hundred bytes each cost more
    # in zip headers than deflating them saves, and what compresses is the
    # whole dictionary (`tests/test_data.py`), not a fixture.
    assert len(names) == EXPECTED_DOCUMENTS


def test_a_zip_made_of_the_folder_reads_the_same(dumped: Path, tmp_path: Path) -> None:
    """`zip -r fix.zip fix/` prefixes every member, and is what people type."""
    rooted = _rooted(dumped, tmp_path / "rooted.zip")
    prefixed = OfflineRegistry(cache_dir=rooted)
    assert prefixed.versions == OfflineRegistry(cache_dir=dumped).versions
    assert prefixed.field("Side").fix["tag"] == "54"
    assert members_of(prefixed.component("Parties", "4.4"))[0].fix.tag == 453


def test_a_scrape_lands_in_the_archive_it_was_pointed_at(tmp_path: Path) -> None:
    """A zip is a store, not only a way to publish one: it is written to."""
    archived = FixtureRegistry(cache_dir=tmp_path / "fix.zip")
    assert len(archived.fields("4.4")) == EXPECTED_FIELDS
    with zipfile.ZipFile(tmp_path / "fix.zip") as opened:
        names = opened.namelist()
    assert SIDE_SHARD in names
    assert "components/parties.json" in names
    assert not [name for name in names if name.count("/") > 1], "no member nested twice"
    reopened = OfflineRegistry(cache_dir=tmp_path / "fix.zip")
    assert len(reopened.fields("4.4")) == EXPECTED_FIELDS, "and read back without a fetch"


def test_writing_a_member_twice_replaces_it(dumped: Path, tmp_path: Path) -> None:
    """A zip will hold two members of one name, and then a reader has to guess."""
    archive = FixRegistry(cache_dir=dumped).into_zip(tmp_path / "fix.zip")
    registry = OfflineRegistry(cache_dir=archive)
    registry._store_fields("4.4", [fix_field("Side", 54, "String", version="4.4")])
    with zipfile.ZipFile(archive) as opened:
        names = opened.namelist()
    assert names.count(SIDE_SHARD) == 1
    assert len(names) == len(set(names))
    reopened = OfflineRegistry(cache_dir=archive)
    assert reopened.field("Side", "4.4").fix["type"] == "String", "the fresh reading"
    assert [member.name for member in reopened.fields("4.4")] == ["Side"]


def test_a_member_written_into_a_prefixed_zip_joins_its_neighbours(
    dumped: Path, tmp_path: Path
) -> None:
    """A member written at the root of a `zip -r` archive would be an orphan."""
    rooted = _rooted(dumped, tmp_path / "rooted.zip")
    registry = OfflineRegistry(cache_dir=rooted)
    registry._store_fields("9.9", [fix_field("Marvellous", 9999, "char", version="9.9")])
    with zipfile.ZipFile(rooted) as opened:
        names = opened.namelist()
    assert "fix/fields/000019.json" in names, "tag 9999 shards into 9999 // 500"
    assert not [name for name in names if name.startswith("fields/")], "never at the root"
    reopened = OfflineRegistry(cache_dir=rooted)
    assert [member.name for member in reopened.fields("9.9")] == ["Marvellous"]


def test_a_torn_archive_is_a_cold_cache_and_not_a_dead_registry(tmp_path: Path) -> None:
    """The same reading a torn file gets: scrape over it rather than refuse."""
    torn = tmp_path / "fix.zip"
    torn.write_bytes(b"PK\x03\x04 and then nothing that follows a zip's rules")
    registry = FixtureRegistry(cache_dir=torn)
    assert registry.versions
    assert len(registry.fields("4.4")) == EXPECTED_FIELDS, "scraped over the wreck"
    with zipfile.ZipFile(torn) as opened:
        assert SIDE_SHARD in opened.namelist(), "and left a readable archive behind"


def test_an_archive_that_holds_nothing_yet_is_not_an_error(tmp_path: Path) -> None:
    """A path that does not exist is a cold store, whichever kind it names."""
    for cache in (tmp_path / "fix", tmp_path / "fix.zip"):
        registry = OfflineRegistry(cache_dir=cache)
        assert not registry.fields_available()
        assert registry._stored_versions() == ()
        assert registry._stored_spellings() == ()
        assert registry._stored_fields("4.4") is None


# -- the cache ---------------------------------------------------------------


def test_a_second_call_answers_from_the_cache(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    fetched = len(registry.fetched)
    registry.fields("4.4")
    assert len(registry.fetched) == fetched, "no page is fetched twice"
    assert (Path(registry.cache_dir) / SIDE_SHARD).exists()


def test_the_cache_survives_offline(registry: FixtureRegistry) -> None:
    assert registry.versions  # touched, so versions.json lands in the cache
    registry.fields("4.4")
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert offline.versions == registry.versions
    assert offline.field("Side").fix["tag"] == "54"
    assert offline.component("parties", "4.4").name == "Parties"


def test_offline_with_only_field_caches_still_knows_its_versions(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")  # versions.json deliberately never written
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert offline.versions == ("4.4",)
    assert offline.field("Side").fix["version"] == "4.4"


def test_a_torn_cache_file_is_scraped_over(registry: FixtureRegistry) -> None:
    """A torn write costs one shard, and the store says so rather than answering short.

    Which is what makes it survivable: a version that still answers, a few
    fields short, is a silence nothing downstream can tell from the truth. So
    it says so and writes the store again.
    """
    registry.fields("4.4")
    (Path(registry.cache_dir) / SIDE_SHARD).write_text("{ torn")
    fresh = FixtureRegistry(cache_dir=registry.cache_dir)
    with pytest.warns(RuntimeWarning, match=r"cannot read \['fields/000000.json'\]"):
        assert len(fresh.fields("4.4")) == EXPECTED_FIELDS
    assert FixtureRegistry(cache_dir=registry.cache_dir).field("Side", "4.4").name == "Side"


def test_a_torn_cache_file_offline_is_reported_and_not_hidden(
    registry: FixtureRegistry,
) -> None:
    """Offline there is nothing to write over it with, so saying so is all there is."""
    registry.fields("4.4")
    (Path(registry.cache_dir) / SIDE_SHARD).write_text("{ torn")
    offline = OfflineRegistry(cache_dir=registry.cache_dir, offline=True)
    torn = len(json.loads((Path(registry.cache_dir) / "fields" / "000001.json").read_text()))
    with pytest.warns(RuntimeWarning, match="cannot read"):
        assert len(offline.fields("4.4")) == torn, "the shard that still reads"


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
    registry._store_fields("FIXT1.1", registry.fields("4.4"))
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert [f.name for f in offline.fields("fixt1.1")] == [f.name for f in offline.fields("4.4")]


def test_tags_maps_folded_names_to_tag_numbers(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    tags = registry.tags()
    assert tags["side"] == 54
    assert tags["possdupflag"] == 43
    assert registry.tags("4.4")["ordrejreason"] == 103
    assert registry.resolve("Poss_Dup-Flag").fix.tag == 43


def test_versioned_tags_and_search_include_recorded_aliases(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")
    registry.alias_field("Side", "TradeSide")

    assert registry.tags("4.4")["tradeside"] == 54
    assert [field.name for field in registry.search("Trade_Side", fuzzy=False)] == ["Side"]


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
    stored = registry.fields("4.4")
    registry._store_fields("FIXT1.1", stored)
    registry._store_fields("fixt1.1", stored)
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert [version.lower() for version in offline.versions] == ["4.4", "fixt1.1"]


def test_lookup_without_a_version_walks_them_newest_first(registry: FixtureRegistry) -> None:
    """Only 4.4 has pages here, so the walk must skip the versions it cannot get."""
    registry.fields("4.4")
    found = registry.lookup("Side")
    assert [member.fix["version"] for member in found] == ["4.4"]


def test_an_unknown_field_is_answered_with_none(registry: FixtureRegistry) -> None:
    """`field()` answers what it holds; `scalar()` is the one that insists."""
    registry.fields("4.4")
    assert registry.field("NoSuchField") is None
    with pytest.raises(KeyError, match="NoSuchField"):
        registry.scalar("NoSuchField")


def test_the_builtin_registry_is_cached_offline_and_versioned() -> None:
    registry = FixRegistry.from_builtin()
    assert registry is FixRegistry.from_builtin()
    assert registry.offline
    assert registry.versions[:2] == ("5.0.SP2", "5.0.SP1")
    assert registry.versions[-1] == "FIXT1.1"


def test_the_builtin_registry_carries_quote_and_translation_controls() -> None:
    registry = FixRegistry.from_builtin()
    expected = {
        "OrdType": 40,
        "ExecType": 150,
        "QuoteID": 117,
        "QuoteReqID": 131,
        "QuoteType": 537,
        "QuoteStatus": 297,
        "QuoteRejectReason": 300,
        "QuoteRespType": 694,
        "QuoteCancelType": 298,
        "BidPx": 132,
        "OfferPx": 133,
        "BidSize": 134,
        "OfferSize": 135,
        "DefBidSize": 293,
        "DefOfferSize": 294,
        "ValidUntilTime": 62,
        "NoQuoteSets": 296,
        "NoQuoteEntries": 295,
        "QuoteSetID": 302,
        "QuoteEntryID": 299,
    }
    assert {name: int(registry.scalar(name).fix["tag"]) for name in expected} == expected


def test_a_standard_message_name_encodes_to_the_msgtype_that_spells_it() -> None:
    """The one direction the dictionary keeps, and the one market dispatch uses.

    There is no reverse: nothing asks the registry what `8` is called, because
    the caller that has `8` already has the fact it needed."""
    msg_type = FixRegistry.from_builtin().resolve("MsgType")

    assert {
        name: msg_type.fix.encode(name)
        for name in ("executionreport", "newordersingle", "marketdatasnapshotfullrefresh")
    } == {
        "executionreport": "8",
        "newordersingle": "D",
        "marketdatasnapshotfullrefresh": "W",
    }
    assert msg_type.fix.encode("TradeCaptureReport") == "AE", "however the caller spells it"
    assert not hasattr(FixRegistry, "msg_type_handlers")


def test_the_builtin_registry_classifies_msg_types_before_transcription() -> None:
    registry = FixRegistry.from_builtin()
    classified = registry.msg_type_event_types()

    assert {key: classified[key] for key in ("D", "F", "G")} == {
        "D": EventType.ORDER,
        "F": EventType.ORDER,
        "G": EventType.ORDER,
    }
    assert {classified[key] for key in ("8", "AE")} == {EventType.EXECUTION}
    assert classified["9"] is EventType.ORDER
    assert {classified[key] for key in ("W", "X")} == {EventType.BOOK}
    assert {classified[key] for key in ("AG", "AH", "AI", "AJ", "R", "S", "Z", "a", "b", "i")} == {
        EventType.QUOTE
    }
    assert classified["d"] is EventType.INSTRUMENT
    assert classified["0"] is EventType.MISC, "known operational FIX traffic"
    assert "U1" not in classified, "a registry-unknown private type stays UNKNOWN"

    metadata = json.loads(registry.scalar("MsgType").fix["event_types"])
    assert metadata["D"] == EventType.ORDER.name, "stored by name, read back as the member"
    assert registry.msg_type_event_types() is classified


def test_a_builtin_scalar_is_one_record_and_every_version_that_declares_it() -> None:
    registry = FixRegistry.from_builtin()
    begin = registry.scalar("BeginString")
    assert begin.fix["name"] == "BeginString"
    assert begin.fix["tag"] == "8"
    assert json.loads(begin.fix["versions"]) == [
        member.fix["version"] for member in registry.lookup(8)
    ]
    # One reading, from the newest application version: 4.0 called tag 8 a
    # `char`, and the collapse says so in `data/fix-conflicts.json` rather
    # than in a per-version map here.
    assert begin.fix["type"] == "String"
    assert begin.fix["version"] == "5.0.SP2"

    side = registry.scalar("Side")
    assert side.fix.value_of("1").meaning == "Buy"
    assert side.fix.value_of("H").aliases == ("SELL_UNDISCLOSED",)
    assert "Quote" in json.loads(side.fix["msgtypes"])


def test_a_scalar_is_fresh_and_an_explicit_version_stays_exact() -> None:
    registry = FixRegistry.from_builtin()
    first = registry.scalar("Price", name="px", dtype=None)
    second = registry.scalar("Price", name="px", dtype=None)
    assert first == second and first is not second
    assert first.name == "px" and first.dtype is None and first.nullable is None
    first.fix["tag"] = "999"
    assert second.fix["tag"] == "44"

    exact = registry.scalar("Side", version="4.4")
    assert exact.fix["version"] == "4.4"
    assert "versions" not in exact.fix, "an exact version is one version's reading"


# -- search ------------------------------------------------------------------


def test_search_matches_name_tag_and_description_case_insensitively(
    registry: FixtureRegistry,
) -> None:
    registry.fields("4.4")
    assert [f.name for f in registry.search("side")] == ["Side"]
    assert [f.name for f in registry.search(54)] == ["Side"]
    assert "OrdRejReason" in [f.name for f in registry.search("REJECTION")]


def test_a_query_answered_by_an_identity_is_not_padded_with_prose() -> None:
    """`54` names `Side`; nine fields whose descriptions mention 54 followed it."""
    registry = FixRegistry.from_builtin()

    assert [field.name for field in registry.search(54)] == ["Side"]
    assert [field.name for field in registry.search("side")][0] == "Side"
    # A name tier still keeps its neighbours -- they are the same question.
    assert "AdvSide" in [field.name for field in registry.search("side", limit=20)]
    # Prose is the answer only when nothing else is.
    assert "OrdRejReason" in [field.name for field in registry.search("REJECTION")]


def test_a_query_of_several_words_is_every_one_of_them() -> None:
    """So a spelling nobody writes without a space still reaches its name."""
    registry = FixRegistry.from_builtin()

    assert [field.name for field in registry.search("order qty", limit=3)][0] == "OrderQty"
    assert [field.name for field in registry.search("cl ord id", limit=3)][0] == "ClOrdID"


def test_search_limits_distinct_identities_across_versions() -> None:
    registry = FixRegistry.from_builtin()
    found = registry.search("Side", limit=20)
    assert sum(member.name == "Side" for member in found) == 1


def test_search_ranks_exact_before_prefix_before_substring(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    found = [f.name for f in registry.search("PartyID")]
    assert found[0] == "PartyID"


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


# -- source merging -----------------------------------------------------------


def test_a_scrape_takes_the_symbol_from_the_spec_and_the_prose_from_the_site(
    tmp_path: Path,
) -> None:
    """Meaning and symbol survive in their distinct slots.

    `Side <54>` value `1` is `BUY` in the spec and "Buy" in the dictionary, and
    a merge that wrote the first where the second goes would replace prose with
    shouting -- which is the whole reason they are two keys.
    """
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    side = next(field for field in registry.fields("4.4") if field.fix["tag"] == "54")
    assert side.fix.value_of("1").meaning == "Buy"
    assert side.fix.value_of("1").aliases == ("BUY",)
    assert side.description, "and the description the site alone has is still there"


def test_a_field_only_the_spec_knows_is_still_a_field(tmp_path: Path) -> None:
    """The by-tag page lists four; the spec names one of them and one more."""
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    by_tag = {field.fix["tag"]: field for field in registry.fields("4.4")}
    assert "828" not in {"43", "54", "103", "205"}, "the fixture's extra tag"
    assert by_tag["828"].name == "TrdType"
    assert by_tag["828"].dtype == pyarrow.int32(), "typed from the spec"
    assert not by_tag["828"].description, "and with no prose, because there is none"


def test_a_field_without_source_symbols_gains_no_enumeration(tmp_path: Path) -> None:
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    listed = {field.fix["tag"]: field for field in registry.fields("4.4")}
    assert not listed["103"].fix.enumerated


def test_a_refused_spec_fails_the_version(tmp_path: Path) -> None:
    """A refused source cannot become a trusted partial version."""

    class NoSpec(FixtureRegistry):
        def _fetch(self, url: str) -> str:
            if url.endswith(".xml"):
                raise _refused(url, 503)
            return super()._fetch(url)

    with pytest.raises(urllib.error.HTTPError, match="503"):
        NoSpec(cache_dir=tmp_path / "fix").fields("4.4")


# -- reusable components ----------------------------------------------------


def test_the_spec_components_travel_with_a_scraped_version(tmp_path: Path) -> None:
    """The reusable blocks, and only those: a message is not read through."""
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    registry.fields("4.4")
    components = registry.components("4.4")
    assert [component.name for component in components] == ["Parties", "PtysSubGrp"]
    assert registry.component("PARTIES", "4.4") == components[0]
    opener = members_of(components[0])[0]
    assert opener.name == "NoPartyIDs"
    assert opener.fix.tag == 453
    assert is_group(opener), "a count tag opens the group it counts"


def test_a_message_is_declared_and_found_by_its_msgtype(tmp_path: Path) -> None:
    """One record, one folder, two ways in: the name, and the wire code."""
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    registry.fields("4.4")
    report = registry.merged_component("AE")
    assert report.name == "TradeCaptureReport" and report.msg_type == "AE"
    assert registry.merged_component("tradecapturereport") is report
    assert registry.message_records() == {"AE": report}
    assert [member.name for member in report.members] == ["TrdType", "Parties"]
    # And the block it carries knows it is carried by it.
    assert registry.merged_component("Parties").msgtypes == ("TradeCaptureReport",)
    assert registry.merged_component("PtysSubGrp").msgtypes == ("TradeCaptureReport",)


def test_group_delimiters_come_off_the_declared_components() -> None:
    """What the market translator segments side and quote-set entries by."""
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip", offline=True)
    assert registry.group_delimiters("TrdCapRptSideGrp", ("NoSides",), "4.4") == ("Side",)
    assert registry.group_delimiters("QuotSetGrp", ("NoQuoteSets", "NoQuoteEntries")) == (
        "QuoteSetID",
        "QuoteEntryID",
    )


def test_an_undeclared_group_chain_has_no_delimiters() -> None:
    """None and not a partial answer: the caller falls back whole."""
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip", offline=True)
    assert registry.group_delimiters("NoSuchGrp", ("NoQuoteSets",), "4.4") is None
    assert registry.group_delimiters("QuotSetGrp", ("NoQuoteSets", "NoSuchGrp"), "4.4") is None


def test_a_store_without_components_reports_them_unavailable(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    registry._store_fields("4.4", [fix_field("Side", 54, "char", version="4.4")])
    offline = OfflineRegistry(cache_dir=tmp_path / "fix", offline=True)
    assert not offline.components_available("4.4")
    assert offline.components("4.4") == []
    with pytest.raises(KeyError, match="Parties"):
        offline.component("Parties", "4.4")


def test_a_declared_empty_component_list_is_available(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry._store_fields("4.4", [fix_field("Side", 54, "char")], components=[])
    assert registry._stored_components("4.4") == []


def test_the_session_layer_travels_with_the_dictionary(tmp_path: Path) -> None:
    """The one fact a stored dictionary could not otherwise answer."""
    registry = FixtureRegistry(cache_dir=tmp_path / "fix")
    registry.fields("4.4")
    stored = OfflineRegistry(cache_dir=tmp_path / "fix")
    assert [name for name, _ in stored.session("4.4")] == [
        "BeginString",
        "MsgSeqNum",
        "PossDupFlag",
        "CheckSum",
        "Signature",
    ]
    assert [name for name, required in stored.session("4.4") if required] == [
        "BeginString",
        "MsgSeqNum",
        "CheckSum",
    ]


def test_a_version_without_a_session_layer_is_empty(
    tmp_path: Path,
) -> None:
    """An omitted session layer is an empty sequence."""
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry._store_fields("4.4", [fix_field("Side", 54, "char")])
    assert registry.session("4.4") == ()
