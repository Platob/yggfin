"""`FixRegistry` stored lookup, publication and adapter refresh contracts."""

import dataclasses
import json
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep.enums import EventType, State
from rekep.fields import Field
from rekep.fix import (
    Alias,
    FixFieldValue,
    FixRegistry,
)
from rekep.fix import registry as registry_source
from rekep.fix.entries import record_copy
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import (
    SPEC_VERSIONS,
    is_group,
    members_of,
    parse_declarations,
    parse_session,
    parse_spec,
)
from rekep.fix.registry import (
    _levenshtein,
)
from rekep.fix.store import SOURCES_FILE, VERSIONS_FILE, DirectoryDocuments

from .conftest import FIXTURES

PUBLISHED = Path(__file__).resolve().parents[3] / "data"
EXPECTED_FIELDS = 11
EXPECTED_DOCUMENTS = 7
SIDE_SHARD = "fields/000000.json"


class FixtureRegistry(FixRegistry):
    """A small stored dictionary built from the QuickFIX fixture."""


class OfflineRegistry(FixRegistry):
    """A stored fixture whose reads never have a network path."""


def _fixture_registry(
    cache: str | Path,
    filesystem: pyarrow.fs.FileSystem | None = None,
) -> FixtureRegistry:
    """Write the small QuickFIX fixture through the normal store layout."""
    from rekep.fix.store import collapse, documents_of

    document = (FIXTURES / "FIX44.xml").read_text()
    fields: list[Field] = []
    descriptions = {
        43: "Whether this transmission may duplicate an earlier message.",
        54: "Side of the order.",
        103: "Order rejection reason.",
    }
    for tag, known in parse_spec(document).items():
        built = fix_field(
            known.name,
            tag,
            known.datatype,
            description=descriptions.get(tag),
            version="4.4",
            values=tuple(
                FixFieldValue(
                    value=value,
                    meaning=symbol.replace("_", " ").title(),
                    aliases=(symbol,),
                )
                for value, symbol in known.values.items()
            ),
        )
        built.fix.source = "quickfix"
        built.fix.sources = ("quickfix",)
        fields.append(built)

    declarations: list[Field] = []
    for entry in parse_declarations(document).values():
        declared = record_copy(entry)
        declared.fix.source = "quickfix"
        declared.fix.sources = ("quickfix",)
        declarations.append(declared)
    entries, components, _ = collapse(("4.4",), {"4.4": fields}, {"4.4": declarations})
    registry = FixtureRegistry(cache_dir=cache, filesystem=filesystem)
    registry._write(
        documents_of(
            SPEC_VERSIONS,
            entries,
            components,
            {"4.4": parse_session(document)},
            {"4.4": declarations},
        )
    )
    return registry


@pytest.fixture
def registry(tmp_path: Path) -> FixtureRegistry:
    return _fixture_registry(tmp_path / "fix")


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
        "namespaces",
        "repgroup",
        "sources.json",
        "versions.json",
    }, "standard and namespaced identities beside version and source indexes"
    unpacked = FixRegistry(cache_dir=folder)
    zipped = FixRegistry(cache_dir=archive)
    assert unpacked.fields_available("4.4") and zipped.fields_available("4.4")
    assert unpacked.versions == zipped.versions
    assert unpacked.field("Side", "4.4") == zipped.field("Side", "4.4")
    assert unpacked.component("Parties", "4.4") == zipped.component("Parties", "4.4")


def test_a_file_url_reads_the_original_archive_without_materializing() -> None:
    archive = (PUBLISHED / "fix.zip").resolve()
    registry = FixRegistry(cache_dir=archive.as_uri())

    assert Path(registry._cache_path) == archive
    assert registry.field("Side", "4.4").fix["tag"] == "54"


def test_an_arrow_filesystem_directory_is_a_registry_store() -> None:
    filesystem = pyarrow.fs._MockFileSystem()
    folder = PUBLISHED / "fix"
    for name in ("registry", "registry/fields", "registry/components", "registry/repgroup"):
        filesystem.create_dir(name)
    for source in folder.rglob("*.json"):
        name = source.relative_to(folder).as_posix()
        filesystem.create_dir(f"registry/{name}".rsplit("/", 1)[0], recursive=True)
        with filesystem.open_output_stream(f"registry/{name}") as stream:
            stream.write(source.read_bytes())

    registry = FixRegistry(cache_dir="registry", filesystem=filesystem)

    assert registry.versions
    assert registry.field("Side", "4.4").fix["tag"] == "54"


def test_a_store_can_be_written_to_an_arrow_filesystem_directory() -> None:
    filesystem = pyarrow.fs._MockFileSystem()
    registry = _fixture_registry("registry", filesystem)

    assert registry.field("Side", "4.4").fix["tag"] == "54"

    offline = OfflineRegistry(cache_dir="registry", filesystem=filesystem)
    assert offline.field("Side", "4.4").fix["tag"] == "54"


def test_a_remote_archive_is_fetched_once_and_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A zip is read by seeking, and seeking over an object store reads it
    whole every lookup -- so it is copied down once and read from there.

    Kept beside the default store rather than in a temporary directory, so the
    next process reuses this one's copy: pointing `cache_dir` at a bucket is
    worth it only if the fetch happens once.
    """
    monkeypatch.setattr(registry_source, "CACHE_DIRECTORY", tmp_path / "config-fix")
    filesystem = pyarrow.fs._MockFileSystem()
    filesystem.create_dir("registry")
    with filesystem.open_output_stream("registry/fix.zip") as stream:
        stream.write((PUBLISHED / "fix.zip").read_bytes())
    registry = FixRegistry(cache_dir="registry/fix.zip", filesystem=filesystem)

    first = registry._cache_path
    assert Path(first).parent == registry_source.remote_cache()
    assert registry.field("Side", "4.4").fix["tag"] == "54"
    assert registry._cache_path == first
    assert registry.field("Side", "4.4").fix["tag"] == "54"

    # A second registry over the same remote finds the copy rather than the
    # remote: same path, and no new file beside it.
    held = sorted(registry_source.remote_cache().iterdir())
    again = FixRegistry(cache_dir="registry/fix.zip", filesystem=filesystem)
    assert again._cache_path == first
    assert sorted(registry_source.remote_cache().iterdir()) == held


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
    # Eleven fields in one tag shard, three blocks and the version index.
    # Not a size claim -- five documents of a few hundred bytes each cost more
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


def test_a_store_lands_in_the_archive_it_was_pointed_at(tmp_path: Path) -> None:
    """A zip is a store, not only a way to publish one: it is written to."""
    archived = _fixture_registry(tmp_path / "fix.zip")
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
    assert "fix/fields/000009.json" in names, "tag 9999 shards into 9999 // 1000"
    assert not [name for name in names if name.startswith("fields/")], "never at the root"
    reopened = OfflineRegistry(cache_dir=rooted)
    assert [member.name for member in reopened.fields("9.9")] == ["Marvellous"]


def test_a_torn_archive_is_never_refreshed_by_a_read(tmp_path: Path) -> None:
    torn = tmp_path / "fix.zip"
    torn.write_bytes(b"PK\x03\x04 and then nothing that follows a zip's rules")
    registry = FixtureRegistry(cache_dir=torn)
    assert registry.versions == ()
    assert registry.fields("4.4") == []


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
    assert registry.fields("4.4") == registry.fields("4.4")
    assert (Path(registry.cache_dir) / SIDE_SHARD).exists()


def test_the_cache_survives_offline(registry: FixtureRegistry) -> None:
    assert registry.versions  # touched, so versions.json lands in the cache
    registry.fields("4.4")
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    assert offline.versions == registry.versions
    assert offline.field("Side").fix["tag"] == "54"
    assert offline.component("parties", "4.4").name == "Parties"


def test_offline_with_only_field_caches_still_knows_its_versions(tmp_path: Path) -> None:
    source = FixtureRegistry(cache_dir=tmp_path / "fix")
    source._store_fields("4.4", [fix_field("Side", 54, "char", version="4.4")])
    offline = OfflineRegistry(cache_dir=source.cache_dir)
    assert offline.versions == ("4.4",)
    assert offline.field("Side").fix["version"] == "4.4"


def test_a_torn_cache_file_requires_explicit_scrape(registry: FixtureRegistry) -> None:
    registry.fields("4.4")
    (Path(registry.cache_dir) / SIDE_SHARD).write_text("{ torn")
    fresh = FixtureRegistry(cache_dir=registry.cache_dir)
    with (
        pytest.warns(RuntimeWarning, match="cannot read"),
        pytest.raises(OSError, match="FixRegistry.scrape"),
    ):
        fresh.fields("4.4")


def test_a_torn_cache_file_offline_is_reported_and_not_hidden(
    registry: FixtureRegistry,
) -> None:
    """Offline there is nothing to write over it with, so saying so is all there is."""
    registry.fields("4.4")
    (Path(registry.cache_dir) / SIDE_SHARD).write_text("{ torn")
    offline = OfflineRegistry(cache_dir=registry.cache_dir)
    with (
        pytest.warns(RuntimeWarning, match="cannot read"),
        pytest.raises(OSError, match="FixRegistry.scrape"),
    ):
        offline.fields("4.4")


def test_fields_never_refresh_the_source(registry: FixtureRegistry) -> None:
    assert registry.fields("4.4")


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


def test_versioned_lookups_read_the_index_once_and_keep_shard_only_fallback(
    registry: FixtureRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry.fields("4.4")
    assert "4.4" in registry.versions
    assert registry.field(54, "4.4") is not None
    read = registry._documents.read
    opened: list[str] = []

    def counted(name: str) -> dict[str, object] | None:
        opened.append(name)
        return read(name)

    monkeypatch.setattr(registry._documents, "read", counted)
    assert registry.field(54, "4.4") is not None
    assert registry.field(54, "4.4") is not None
    assert "versions.json" not in opened

    assert "9.9" not in registry.versions
    registry._store_fields("9.9", [fix_field("FutureField", 9000, "String", version="9.9")])
    assert "versions" not in registry.__dict__
    opened.clear()
    assert registry.field(9000, "9.9") is not None
    assert "versions.json" in opened
    assert "versions" not in registry.__dict__
    with pytest.raises(KeyError, match="not a FIX version"):
        registry.field(9000, "8.8")


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


def test_tags_for_an_explicit_unstored_version_never_fetches(
    registry: FixtureRegistry,
) -> None:
    assert registry.tags("FIXT1.1") == {}


def test_registry_coalesces_equivalent_tags_in_declared_priority(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    lastpx = fix_field("LastPx", 31, "Price", version="4.4")
    lastpx.fix.tags = (44, 270)
    registry._store_fields("4.4", (lastpx,))

    assert registry.field_tags("LastPx", "4.4") == (31, 44, 270)
    assert registry.coalesce_tags("LastPx", {31: None, "44": 12.5, 270: 9.0}) == 12.5
    assert registry.coalesce_tags("LastPx", {}, default=-1.0) == -1.0

    values = {
        31: pyarrow.array([None, "2.5", None]),
        44: pyarrow.array(["1.25", "9", None]),
        270: pyarrow.array(["3", "4", "5"]),
    }
    actual = registry.arrow_coalesce_tags("LastPx", values, 3, version="4.4")
    assert actual.type == pyarrow.float64()
    assert actual.to_pylist() == [1.25, 2.5, 5.0]


def test_arrow_tag_coalescing_returns_typed_nulls_and_refuses_bad_lengths(
    tmp_path: Path,
) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    lastpx = fix_field("LastPx", 31, "Price", version="4.4")
    lastpx.fix.tags = (44,)
    registry._store_fields("4.4", (lastpx,))

    missing = registry.arrow_coalesce_tags("LastPx", {}, 2, version="4.4")
    assert missing.type == pyarrow.float64() and missing.null_count == 2
    with pytest.raises(ValueError, match="expected 2"):
        registry.arrow_coalesce_tags(
            "LastPx", {44: pyarrow.array(["1", "2", "3"])}, 2, version="4.4"
        )


def test_a_version_that_would_be_a_path_is_refused(registry: FixtureRegistry) -> None:
    """The version lands in a cache file name, so it must never carry a path."""
    for hostile in ("../evil", "a/b", "..", ""):
        with pytest.raises(ValueError, match="does not name a FIX version"):
            registry.fields(hostile)


def test_duplicate_case_spellings_in_the_cache_are_one_version(tmp_path: Path) -> None:
    source = FixtureRegistry(cache_dir=tmp_path / "fix")
    stored = [fix_field("Side", 54, "char", version="4.4")]
    source._store_fields("4.4", stored)
    source._store_fields("FIXT1.1", stored)
    source._store_fields("fixt1.1", stored)
    offline = OfflineRegistry(cache_dir=source.cache_dir)
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
    assert registry.versions[:2] == ("FIX.Latest", "5.0.SP2")
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
    categories = {
        EventType.SESSION: {"0", "1", "2", "3", "4", "5", "A"},
        EventType.INDICATION: {"6", "7", "C"},
        EventType.ALLOCATION: {"J", "P", "AS", "AT", "BM", "DU", "DV"},
        EventType.SETTLEMENT: {"T", "AV", "BQ"},
        EventType.CONFIRMATION: {"AK", "AU", "BH"},
        EventType.NEWS: {"B"},
        EventType.POSITION: {"AL", "AM", "AN", "AO", "AP", "BL", "DL", "DM", "DN"},
        EventType.COLLATERAL: {"AX", "AY", "AZ", "BA", "BB", "BG", "DQ"},
        EventType.PARTY: {
            "CF",
            "CG",
            "CK",
            "CL",
            "CM",
            "CR",
            "CS",
            "CT",
            "CU",
            "CV",
            "CX",
            "CY",
            "CZ",
            "DA",
            "DB",
            "DE",
            "DF",
            "DG",
            "DH",
            "DI",
        },
    }
    for category, msgtypes in categories.items():
        assert {key for key in msgtypes if classified[key] is category} == msgtypes
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
    # One reading, from the newest application version. Older versions called
    # tag 8 `char`; that is the same stored string contract, not a conflict.
    assert begin.fix["type"] == "String"
    assert begin.fix["version"] == "FIX.Latest"

    side = registry.scalar("Side")
    assert side.fix.value_of("1").meaning == "Buy"
    assert side.fix.value_of("H").aliases == ("SellUndisclosed", "SELL_UNDISCLOSED")
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
    assert "AdvSide" in [field.name for field in registry.search("side", limit=100)]
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


# -- stored declarations ------------------------------------------------------


def test_value_symbols_and_meanings_keep_their_distinct_slots(
    tmp_path: Path,
) -> None:
    """Meaning and symbol survive in their distinct slots.

    `Side <54>` value `1` uses `BUY` as its symbol and `Buy` as its meaning.
    """
    registry = _fixture_registry(tmp_path / "fix")
    side = next(field for field in registry.fields("4.4") if field.fix["tag"] == "54")
    assert side.fix.value_of("1").meaning == "Buy"
    assert side.fix.value_of("1").aliases == ("BUY",)
    assert side.description


def test_a_field_only_the_spec_knows_is_still_a_field(tmp_path: Path) -> None:
    """The by-tag page lists four; the spec names one of them and one more."""
    registry = _fixture_registry(tmp_path / "fix")
    by_tag = {field.fix["tag"]: field for field in registry.fields("4.4")}
    assert "828" not in {"43", "54", "103", "205"}, "the fixture's extra tag"
    assert by_tag["828"].name == "TrdType"
    assert by_tag["828"].dtype == pyarrow.int32(), "typed from the spec"
    assert not by_tag["828"].description, "and with no prose, because there is none"


def test_a_field_without_source_symbols_gains_no_enumeration(tmp_path: Path) -> None:
    registry = _fixture_registry(tmp_path / "fix")
    listed = {field.fix["tag"]: field for field in registry.fields("4.4")}
    assert not listed["103"].fix.enumerated


# -- reusable components ----------------------------------------------------


def test_the_spec_components_travel_with_a_scraped_version(tmp_path: Path) -> None:
    """The reusable blocks, and only those: a message is not read through."""
    registry = _fixture_registry(tmp_path / "fix")
    components = registry.components("4.4")
    assert [component.name for component in components] == ["Parties", "PtysSubGrp"]
    assert registry.component("PARTIES", "4.4") == components[0]
    opener = members_of(components[0])[0]
    assert opener.name == "NoPartyIDs"
    assert opener.fix.tag == 453
    assert is_group(opener), "a count tag opens the group it counts"


def test_a_message_is_declared_and_found_by_its_msgtype(tmp_path: Path) -> None:
    """One record, one folder, two ways in: the name, and the wire code."""
    registry = _fixture_registry(tmp_path / "fix")
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
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip")
    assert registry.group_delimiters("TrdCapRptSideGrp", ("NoSides",), "4.4") == ("Side",)
    assert registry.group_delimiters("QuotSetGrp", ("NoQuoteSets", "NoQuoteEntries")) == (
        "QuoteSetID",
        "QuoteEntryID",
    )


def test_an_undeclared_group_chain_has_no_delimiters() -> None:
    """None and not a partial answer: the caller falls back whole."""
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip")
    assert registry.group_delimiters("NoSuchGrp", ("NoQuoteSets",), "4.4") is None
    assert registry.group_delimiters("QuotSetGrp", ("NoQuoteSets", "NoSuchGrp"), "4.4") is None


def test_component_group_field_returns_the_expanded_terminal_group() -> None:
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip")
    group = registry.component_group_field("QuotSetGrp", ("NoQuoteSets", "NoQuoteEntries"), "4.4")

    assert group is not None and group.fix.name == "NoQuoteEntries"
    assert group.nullable and not group.item.nullable
    assert [(field.fix.name, field.nullable) for field in group.item.fields[:2]] == [
        ("QuoteEntryID", False),
        ("Symbol", True),
    ]
    sides = registry.component_group_field("AE", "NoSides", "4.4")
    newest_sides = registry.component_group_field("AE", "NoSides")
    assert sides is not None and sides.fix.name == "NoSides"
    assert newest_sides is not None and newest_sides.fix.name == "NoSides"


def test_component_group_field_distinguishes_absent_and_non_group_paths() -> None:
    registry = FixRegistry(cache_dir=PUBLISHED / "fix.zip")

    assert registry.component_group_field("Absent", "NoSides", "4.4") is None
    assert registry.component_group_field("AE", "Absent", "4.4") is None
    assert registry.component_group_field("AE", "NoSides", "4.0") is None
    assert registry.component_group_field("AE", "NoSides", "9.9") is None
    with pytest.raises(ValueError, match="not a group"):
        registry.component_group_field("AE", "BeginString", "4.4")
    with pytest.raises(ValueError, match="cannot be empty"):
        registry.component_group_field("AE", (), "4.4")


def test_a_store_without_components_reports_them_unavailable(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry._store_fields("4.4", [fix_field("Side", 54, "char", version="4.4")])
    offline = OfflineRegistry(cache_dir=tmp_path / "fix")
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
    _fixture_registry(tmp_path / "fix")
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


def _definition(
    name: str,
    tag: int,
    datatype: str,
    source: str,
    *,
    aliases: tuple[str, ...] = (),
) -> Field:
    """One attributed extension definition for namespace tests."""
    field = fix_field(name, tag, datatype)
    field.fix.versions = ("FIX.Latest",)
    field.fix.source = source
    field.fix.sources = (source,)
    field.fix.named_aliases = aliases
    return field


def _source(source_id: str, namespace: str, priority: int) -> dict[str, object]:
    """One complete-file provenance manifest entry."""
    digest = source_id.encode().hex().ljust(64, "0")[:64]
    return {
        "source_id": source_id,
        "namespace": namespace,
        "url": f"https://example.test/{source_id}.xml",
        "version": "FIX.Latest",
        "format": "orchestra",
        "checksum": f"sha256:{digest}",
        "license_url": "https://example.test/terms",
        "priority": priority,
    }


def test_definitions_keep_standard_udf_and_venue_tags_separate(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix", namespace_priority=("clear-street",))
    registry.add_definition(_definition("MaxShow", 210, "Qty", "fix-latest"), "standard")
    registry.add_definition(
        _definition("MaxShow1", 9001, "Qty", "fixtrading-udf", aliases=("MaxShow",)),
        "fixtrading-udf",
    )
    registry.add_definition(
        _definition("VenueMaximumShow", 9001, "String", "clear-street"), "clear-street"
    )

    assert registry.field(210).fix.canonical == "MaxShow"
    assert registry.field(9001).fix.canonical == "MaxShow1"
    assert registry.lookup(9001)[0].fix.canonical == "MaxShow1"
    assert registry.definition(9001, "clear-street").fix.canonical == "VenueMaximumShow"
    assert [field.fix.get("namespace") for field in registry.definitions(9001)] == [
        "fixtrading-udf",
        "clear-street",
    ]
    assert [field.fix.tag for field in registry.definitions("MaxShow")] == [210, 9001]
    assert (tmp_path / "fix/namespaces/fixtrading-udf/fields/000009.json").is_file()
    assert (tmp_path / "fix/namespaces/clear-street/fields/000009.json").is_file()


def test_configured_vendor_priority_survives_reopen(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    second = {**_source("second", "venue-second", 10), "lookup_order": 1}
    first = {**_source("first", "venue-first", 10), "lookup_order": 0}
    registry.store_source_manifest((second, first))
    registry.add_definition(_definition("SecondCode", 9005, "String", "second"), "venue-second")
    registry.add_definition(_definition("FirstCode", 9005, "String", "first"), "venue-first")

    reopened = FixRegistry(cache_dir=tmp_path / "fix")

    assert reopened.namespaces() == ("standard", "venue-first", "venue-second")
    assert reopened.field(9005).fix.canonical == "FirstCode"
    assert [field.fix.canonical for field in reopened.definitions(9005)] == [
        "FirstCode",
        "SecondCode",
    ]


def test_same_namespace_type_conflicts_fall_back_to_string(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest(
        (
            _source("official", "fixtrading-udf", 0),
            _source("enrichment", "fixtrading-udf", 10),
        )
    )
    registry.add_definition(
        _definition("UDFSupportIndicator", 9003, "Int", "official"), "fixtrading-udf"
    )
    merged = registry.add_definition(
        _definition("UDFSupportIndicator", 9003, "String", "enrichment"),
        "fixtrading-udf",
    )

    assert merged.dtype == pyarrow.string()
    assert merged.fix.type == "String"
    assert json.loads(merged.fix["disputed_types"]) == ["Int", "String"]
    assert registry.conflicts.counts()["type"] == 1
    assert FixRegistry(cache_dir=tmp_path / "fix").field(9003).dtype == pyarrow.string()


def test_adapter_status_attributes_conflicts_created_during_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters
    from rekep.fix.orchestra import SourceField, SourceProvenance, SourceRegistry

    class Adapter:
        def __init__(self, registry: SourceRegistry, priority: int) -> None:
            self.registry = registry
            self.priority = priority

        def load(self, *_args, **_kwargs) -> SourceRegistry:
            return self.registry

    def source(source_id: str, datatype: str, dtype: pyarrow.DataType) -> SourceRegistry:
        provenance = SourceProvenance.for_bytes(
            source_id.encode(),
            source_id=source_id,
            namespace="fixtrading-udf",
            version="1.0",
            protocol_version="FIX.Latest",
        )
        field = SourceField(
            tag=9003,
            name="UDFSupportIndicator",
            original_datatype=datatype,
            datatype=datatype,
            arrow_type=dtype,
            provenance=provenance,
        )
        return SourceRegistry(provenance, "UDF", "1.0", fields=(field,))

    monkeypatch.setattr(
        adapters,
        "ADAPTERS_BY_ID",
        {
            "official": Adapter(source("official", "Int", pyarrow.int64()), 0),
            "vendor": Adapter(source("vendor", "String", pyarrow.string()), 100),
        },
    )
    registry = FixRegistry(cache_dir=tmp_path / "fix")

    registry._ingest_adapters(("official", "vendor"), tmp_path / "sources", offline=True)

    assert [status["conflicts"] for status in registry.source_status] == [0, 1]
    assert registry.conflicts.counts()["type"] == 1


def test_official_orchestra_precedes_legacy_enrichment_in_standard(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest((_source("fix-latest", "standard", 0),))
    legacy = _definition("OldMaximumShow", 210, "Qty", "nanoconda")
    registry.add_field(legacy)

    merged = registry.add_definition(_definition("MaxShow", 210, "Qty", "fix-latest"), "standard")

    assert merged.fix.canonical == "MaxShow"
    assert merged.fix.source == "fix-latest"
    assert "OldMaximumShow" in merged.fix.spellings()


def test_latest_merge_preserves_the_legacy_fut_sett_date_alias(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest((_source("fix-latest", "standard", 0),))
    legacy = _definition("SettlDate", 64, "LocalMktDate", "nanoconda")
    legacy.fix.named_aliases = (Alias("FutSettDate", source="4.3"),)
    registry.add_field(legacy)

    merged = registry.add_definition(
        _definition("SettlDate", 64, "LocalMktDate", "fix-latest"), "standard"
    )

    assert merged.fix.source == "fix-latest"
    assert "FutSettDate" in merged.fix.spellings()


def test_new_canonical_name_shadows_only_the_legacy_alias(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest((_source("fix-latest", "standard", 0),))
    legacy = _definition("BidTradeType", 418, "Int", "nanoconda")
    legacy.fix.named_aliases = (
        Alias("TradeType", source="FIX.4.4"),
        Alias("BidType", source="onixs"),
    )
    registry.add_field(legacy)

    changes = registry.add_definitions(
        (_definition("TradeType", 828, "Int", "fix-latest"),), "standard"
    )

    assert changes == {"additions": 1, "updates": 1}
    assert registry.definition("TradeType", "standard").fix.tag == 828
    assert registry.definition(418, "standard").fix.named_aliases == (
        Alias("BidType", source="onixs"),
    )
    conflict = registry.conflicts.collapses[-1]
    assert conflict.part == "aliases"
    assert conflict.name == "TradeType"
    assert conflict.dropped[0].reading == "TradeType"
    assert conflict.dropped[0].source == "FIX.4.4"


def test_same_name_different_tags_follow_source_priority_in_either_order(
    tmp_path: Path,
) -> None:
    sources = (
        _source("official", "fixtrading-udf", 0),
        _source("vendor", "fixtrading-udf", 100),
    )

    def built(path: Path, order: tuple[str, ...]) -> Field:
        registry = FixRegistry(cache_dir=path)
        registry.store_source_manifest(sources)
        definitions = {
            "official": _definition("Collision", 10_001, "String", "official"),
            "vendor": _definition("Collision", 9_001, "String", "vendor"),
        }
        for source in order:
            registry.add_definition(definitions[source], "fixtrading-udf")
        assert registry.definition(9_001, "fixtrading-udf") is None
        winner = registry.definition(10_001, "fixtrading-udf")
        assert winner is not None
        assert json.loads(winner.fix["disputed_keys"]) == [
            {"key": "10001", "source": "official"},
            {"key": "9001", "source": "vendor"},
        ]
        assert registry.conflicts.collapses[-1].keptsource == "official"
        assert registry.conflicts.collapses[-1].dropped[0].source == "vendor"
        return winner

    low_first = built(tmp_path / "low-first", ("vendor", "official"))
    high_first = built(tmp_path / "high-first", ("official", "vendor"))

    assert low_first == high_first


def test_same_value_conflicts_keep_authoritative_meaning_and_all_aliases(
    tmp_path: Path,
) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest(
        (
            _source("official", "fixtrading-udf", 0),
            _source("vendor", "fixtrading-udf", 100),
        )
    )
    official = _definition("UDFSupportIndicator", 9003, "Int", "official")
    official.description = "Official description."
    official.fix.enumerated = (FixFieldValue("1", "Supports UDFs", ("Supports",)),)
    vendor = _definition("UDFSupportIndicator", 9003, "Int", "vendor")
    vendor.description = "Vendor description."
    vendor.fix.enumerated = (FixFieldValue("1", "Enabled", ("Yes",)),)
    registry.add_definition(vendor, "fixtrading-udf")

    merged = registry.add_definition(official, "fixtrading-udf")

    assert merged.description == "Official description."
    assert merged.fix.enumerated == (FixFieldValue("1", "Supports UDFs", ("Supports", "Yes")),)
    conflicts = registry.conflicts.collapses
    assert [(conflict.part, conflict.keptsource) for conflict in conflicts] == [
        ("values", "official"),
        ("note", "official"),
    ]
    assert conflicts[0].dropped[0].key == "1"
    assert conflicts[0].dropped[0].source == "vendor"


def test_latest_merge_preserves_local_field_overlays_and_utc_refinement(
    tmp_path: Path,
) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest((_source("fix-latest", "standard", 0),))
    msgtype = _definition("MsgType", 35, "String", "nanoconda")
    msgtype.fix.event_types = {"D": EventType.ORDER}
    msgtype.fix.states = {"D": State.OPEN}
    msgtype.fix.msgtypes = ("D",)
    msgtype.fix.components = ("StandardHeader",)
    msgtype.fix.column = "bodytype"
    registry.add_field(msgtype)
    origtime = _definition("OrigTime", 42, "UTCTimestamp", "nanoconda")
    origtime.dtype = pyarrow.timestamp("us", tz="UTC")
    registry.add_field(origtime)

    merged_msgtype = registry.add_definition(
        _definition("MsgType", 35, "String", "fix-latest"), "standard"
    )
    merged_origtime = registry.add_definition(
        _definition("OrigTime", 42, "UTCTimestamp", "fix-latest"), "standard"
    )

    assert merged_msgtype.fix.event_types == {"D": EventType.ORDER}
    assert merged_msgtype.fix.states == {"D": State.OPEN}
    assert merged_msgtype.fix.msgtypes == ("D",)
    assert merged_msgtype.fix.components == ("StandardHeader",)
    assert merged_msgtype.fix.column == "bodytype"
    assert merged_origtime.dtype == pyarrow.timestamp("us", tz="UTC")


def test_authoritative_enum_merge_is_stable_on_replay(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry.store_source_manifest((_source("fix-latest", "standard", 0),))
    legacy = _definition("Side", 54, "Char", "nanoconda")
    legacy.fix.enumerated = (
        FixFieldValue("2", aliases=("Sell",)),
        FixFieldValue("1", aliases=("Buy",)),
        FixFieldValue("Z", aliases=("Legacy",)),
    )
    official = _definition("Side", 54, "Char", "fix-latest")
    official.fix.enumerated = (
        FixFieldValue("1", aliases=("Buy",)),
        FixFieldValue("2", aliases=("Sell",)),
    )
    registry.add_field(legacy)

    first = registry.add_definitions((official,), "standard")
    record = registry.definition(54, "standard")
    second = registry.add_definitions((official,), "standard")

    assert first == {"additions": 0, "updates": 1}
    assert second == {"additions": 0, "updates": 0}
    assert registry.definition(54, "standard") == record
    assert [value.value for value in record.fix.enumerated] == ["1", "2", "Z"]


def test_source_manifest_and_namespaced_archives_are_deterministic(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    registry._store_versions(())
    registry.store_source_manifest((_source("udf", "fixtrading-udf", 0),))
    registry.add_definition(_definition("CrossSeqNum", 9002, "SeqNum", "udf"), "fixtrading-udf")

    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    registry.into_zip(first)
    registry.into_zip(second)

    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as archive:
        names = archive.namelist()
    assert "namespaces/fixtrading-udf/fields/000009.json" in names
    assert "sources.json" in names
    reopened = FixRegistry(cache_dir=first)
    assert reopened.field(9002).fix.canonical == "CrossSeqNum"
    assert reopened.source_manifest()[0]["source_id"] == "udf"


def test_cached_orchestra_refresh_populates_fields_components_and_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    fixture_source = dataclasses.replace(adapters.FIX_LATEST, checksum="")
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fix-latest": fixture_source})

    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())

    registry = FixRegistry.scrape(
        tmp_path / "fix",
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    assert len(registry.field_records()) == 12
    assert len(registry.component_records()) == 2
    assert len(registry.repeating_group_records()) == 1
    assert registry.component_records()["Instrument"].msgtypes == ("NewOrderSingle",)
    manifest = registry.source_manifest()[0]
    assert manifest["source_id"] == "fix-latest"
    assert manifest["projection"] == "rekep-fix-registry-v1"
    assert str(manifest["definitions_checksum"]).startswith("sha256:")
    assert registry.source_status[0]["additions"] == 12
    assert registry.field(9001) is not None
    assert registry.versions[0] == "FIX.Latest"
    index = registry._documents.read(VERSIONS_FILE)
    assert index is not None
    assert index["versions"] == ["FIX.Latest"]
    assert index["stored"] == ["FIX.Latest"]
    assert index["declared"] == ["FIX.Latest"]
    first = {
        path.relative_to(tmp_path / "fix").as_posix(): path.read_bytes()
        for path in (tmp_path / "fix").rglob("*.json")
    }

    def no_revalidation(*_args, **_kwargs):
        raise AssertionError("an unchanged complete-source replay does not republish the store")

    monkeypatch.setattr(FixRegistry, "_validate_registry_store", no_revalidation)

    replayed = FixRegistry.scrape(
        tmp_path / "fix",
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )
    assert replayed.source_status[0]["additions"] == 0
    assert replayed.source_status[0]["updates"] == 0
    second = {
        path.relative_to(tmp_path / "fix").as_posix(): path.read_bytes()
        for path in (tmp_path / "fix").rglob("*.json")
    }
    assert second == first


def test_cached_replay_repairs_standard_version_indexes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    fixture_source = dataclasses.replace(adapters.FIX_LATEST, checksum="")
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fix-latest": fixture_source})
    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())
    target = tmp_path / "fix"
    registry = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )
    index = registry._documents.read(VERSIONS_FILE)
    assert index is not None
    index.pop("stored")
    index.pop("declared")
    registry._documents.write(VERSIONS_FILE, index)

    repaired = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    assert repaired.source_status[0]["additions"] == 0
    assert repaired.source_status[0]["updates"] == 0
    index = repaired._documents.read(VERSIONS_FILE)
    assert index is not None
    assert index["versions"] == ["FIX.Latest"]
    assert index["stored"] == ["FIX.Latest"]
    assert index["declared"] == ["FIX.Latest"]


def test_cached_refresh_upgrades_a_manifest_without_projection_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    fixture_source = dataclasses.replace(adapters.FIX_LATEST, checksum="")
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fix-latest": fixture_source})
    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())
    target = tmp_path / "fix"
    registry = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )
    document = registry._documents.read(SOURCES_FILE)
    assert document is not None
    for source in document["sources"]:
        source.pop("projection")
        source.pop("definitions_checksum")
    registry._documents.write(SOURCES_FILE, document)

    upgraded = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    assert upgraded.source_status[0]["additions"] == 0
    assert upgraded.source_status[0]["updates"] == 0
    assert upgraded.source_manifest()[0]["projection"] == "rekep-fix-registry-v1"
    assert str(upgraded.source_manifest()[0]["definitions_checksum"]).startswith("sha256:")


def test_projection_upgrade_repairs_and_preserves_component_carriage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    fixture_source = dataclasses.replace(adapters.FIX_LATEST, checksum="")
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fix-latest": fixture_source})
    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())
    target = tmp_path / "fix"
    registry = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    held = registry.component_records()["Instrument"]
    declared = record_copy(held.declaration)
    declared.fix.pop("msgtypes", None)
    registry._layout._store_component(dataclasses.replace(held, declaration=declared))
    manifest = registry._documents.read(SOURCES_FILE)
    assert manifest is not None
    manifest["sources"][0]["projection"] = "stale-projection"
    registry._documents.write(SOURCES_FILE, manifest)

    upgraded = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    repaired = upgraded.component_records()["Instrument"]
    assert repaired.msgtypes == ("NewOrderSingle",)

    declared = record_copy(repaired.declaration)
    declared.fix.msgtypes = ["LegacyOrder"]
    upgraded._layout._store_component(dataclasses.replace(repaired, declaration=declared))
    manifest = upgraded._documents.read(SOURCES_FILE)
    assert manifest is not None
    manifest["sources"][0]["projection"] = "stale-projection"
    upgraded._documents.write(SOURCES_FILE, manifest)

    preserved = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )
    assert preserved.component_records()["Instrument"].msgtypes == (
        "LegacyOrder",
        "NewOrderSingle",
    )


def test_cached_refresh_reconciles_when_source_priority_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    fixture_source = dataclasses.replace(adapters.FIX_LATEST, checksum="", priority=10)
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fix-latest": fixture_source})
    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())
    target = tmp_path / "fix"
    FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )
    monkeypatch.setattr(
        adapters,
        "ADAPTERS_BY_ID",
        {"fix-latest": dataclasses.replace(fixture_source, priority=0)},
    )
    reconciled: list[str] = []
    add_definitions = FixRegistry.add_definitions

    def tracked(self: FixRegistry, entries: tuple[Field, ...], namespace: str) -> Mapping[str, int]:
        reconciled.append(namespace)
        return add_definitions(self, entries, namespace)

    monkeypatch.setattr(FixRegistry, "add_definitions", tracked)

    refreshed = FixRegistry.scrape(
        target,
        source_ids=("fix-latest",),
        offline=True,
        source_cache=cache,
    )

    assert reconciled == ["standard"]
    assert refreshed.source_manifest()[0]["priority"] == 0


def test_bulk_namespace_ingestion_writes_once_per_tag_shard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    written: list[str] = []
    write = registry._documents.write

    def counted(name: str, document: dict[str, object]) -> None:
        written.append(name)
        write(name, document)

    monkeypatch.setattr(registry._documents, "write", counted)
    definitions = tuple(
        _definition(f"RegisteredUDF{tag}", tag, "String", "fixtrading-udf")
        for tag in range(5_000, 7_500)
    )

    changes = registry.add_definitions(definitions, "fixtrading-udf")

    assert changes == {"additions": 2_500, "updates": 0}
    assert written == [
        "namespaces/fixtrading-udf/fields/000005.json",
        "namespaces/fixtrading-udf/fields/000006.json",
        "namespaces/fixtrading-udf/fields/000007.json",
    ]


def test_partial_offline_refresh_copies_the_store_without_parsing_every_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.fix import adapters

    target = tmp_path / "fix"
    standard = FixRegistry(cache_dir=target)
    standard._store_versions(("4.4",))
    standard._store_fields("4.4", [fix_field("Side", 54, "char", version="4.4")])
    fixture_source = dataclasses.replace(
        adapters.FIX_LATEST,
        source_id="fixtrading-udf",
        namespace="fixtrading-udf",
        checksum="",
    )
    monkeypatch.setattr(adapters, "ADAPTERS_BY_ID", {"fixtrading-udf": fixture_source})
    cache = tmp_path / "sources"
    cache.mkdir()
    (cache / fixture_source.cache_name).write_bytes((FIXTURES / "orchestra.xml").read_bytes())

    def no_bulk_parse(*_args, **_kwargs):
        raise AssertionError("partial refresh must copy unchanged documents as bytes")

    monkeypatch.setattr(DirectoryDocuments, "read_many", no_bulk_parse)
    refreshed = FixRegistry.scrape(
        target,
        source_ids=("fixtrading-udf",),
        offline=True,
        source_cache=cache,
    )
    monkeypatch.undo()

    assert refreshed.versions == ("4.4",)
    assert refreshed.field(54).fix.canonical == "Side"
    assert len(refreshed.field_records("fixtrading-udf")) == 12
    assert len(refreshed.component_records("fixtrading-udf")) == 2
    assert len(refreshed.repeating_group_records("fixtrading-udf")) == 1
