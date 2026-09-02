"""The FIX dictionary in `data/fix.zip` is checked here, so a bad scrape cannot ship.

A dump nobody verifies is a directory of files that merely *look* like a
    dictionary. The first scrape of this one came back a fifth empty -- the sites
    throttle a fourteen-thousand-page walk, and every page refused became a field
with no type, no description and no enumeration -- and every file it wrote
still parsed, still loaded, still answered a lookup. So the checks here are the
ones that would have failed then: what a version file holds is `Field`s, the
tags are its own, and the parts the pages carry are carried through.
"""

import json
import urllib.request
import zipfile
from functools import cache
from pathlib import Path

import pyarrow
import pytest

from rekep.fix import FIX_SCALARS, FixRegistry
from rekep.fix.classify import NAMESPACE, NEAR, KeyCount, KeyCounts, classify
from rekep.fix.columns import _ORDER
from rekep.fix.entries import ANY_VERSION
from rekep.fix.fields import fix_field
from rekep.fix.publish import (
    CONFLICT_BASELINE,
    FIXMSG_FIELDS,
    MARKET_FIELDS,
    REQUIRED_FIELDS,
    beyond_baseline,
    missing_from,
    publish_full,
)
from rekep.fix.quickfix import members_of
from rekep.fix.rekep import (
    REKEP_COMPONENT_NAMES,
    REKEP_FIELD_DECLARATIONS,
    REKEP_TAGS,
    rekep_is_registered,
)
from rekep.fix.store import NAMED_SHARD, SHARD_SPAN, ConflictReport, field_document
from rekep.market.fix import CARRIED_FIELDS, market_tags

#: The dictionary is at the repo root, beside `python/` -- published data,
#: not something shipped in the wheel. One archive: a registry pointed at it
#: reads the JSON inside, and the extension is what tells it to.
DATA = Path(__file__).resolve().parents[2] / "data" / "fix.zip"


@cache
def member(name: str) -> dict[str, object]:
    """One document out of the published archive, read without a registry.

    The tests below have to be able to fail when the registry is wrong, so
    what they read the archive with is `zipfile`, not the code under test.
    """
    with zipfile.ZipFile(DATA) as archive:
        return json.loads(archive.read(name).decode("utf-8"))


@cache
def members(folder: str) -> dict[str, dict[str, object]]:
    """Every document under one folder of the archive, by its file name."""
    with zipfile.ZipFile(DATA) as archive:
        return {
            name[len(folder) + 1 : -len(".json")]: json.loads(archive.read(name).decode("utf-8"))
            for name in sorted(archive.namelist())
            if name.startswith(f"{folder}/")
        }


@cache
def records() -> dict[str, dict[str, object]]:
    """Every field record in the archive, by the identity it states.

    A shard is a list of records, so the key here is read *off* each record --
    its tag, or its canonical name where FIX never numbered it -- rather than
    from a mapping above it that could disagree with what it keys.
    """
    found: dict[str, dict[str, object]] = {}
    for shard in members("fields").values():
        for record in shard:
            fix = record["fix"]
            found[str(fix["tag"]) if "tag" in fix else str(record["name"])] = record
    return found


#: Every key one stored field record carries at its top level: the Arrow
#: reading, and the protocol's own nested under `fix`. The same document a
#: component declaration and `schemas/rekep/*.yaml` are written in, which is
#: the whole reason there is no second codec for a field record.
FIELD_KEYS = frozenset(
    {"name", "type", "nullable", "description", "fix", "fields", "item", "key", "value"}
)

#: The `fix:` keys whose value is itself a document in a readable field shard.
NESTED = (
    "versions",
    "values",
    "aliases",
    "msgtypes",
    "components",
    "event_types",
    "states",
    "sources",
    "origins",
)


def stored_fix(record: dict[str, object]) -> dict[str, object]:
    """One record's readable FIX metadata."""
    fix = dict(record.get("fix") or {})
    assert all(not isinstance(fix[key], str) for key in NESTED if key in fix)
    return fix


def stored_value(record: dict[str, object], value: str) -> dict[str, object]:
    """One enumerated value out of a stored record, by what the wire carries."""
    for one in stored_fix(record).get("values") or ():
        if one["value"] == value:
            return one
    raise AssertionError(f"{record.get('name')} declares no value {value!r}")


INDEX: dict[str, object] = member("versions.json")
VERSIONS: list[str] = INDEX["versions"]

#: Derived from the archive, then pinned. Counts rather than a bare "more than
#: zero", so a rebuild that lost half the dictionary and still produced a
#: readable store fails here.
#:
#: Eleven shards under one naming rule: the standard occupies nine
#: sparse ranges, rekep's frozen 30000 range occupies one more, and the fields
#: FIX never numbered share `NAMED_SHARD`, which is an index no tag reaches
#: rather than a document of another kind.
EXPECTED_FIELD_DOCUMENTS = 11
EXPECTED_FIELD_RECORDS = 6283
EXPECTED_COMPONENT_FILES = 927
EXPECTED_REPEATING_GROUP_FILES = 525
#: Of which these are messages: a message is a component that arrives under a MsgType.
EXPECTED_MESSAGE_FILES = 193

REKEP_TAG_VALUES = frozenset(REKEP_TAGS.values())
REKEP_FIELD_NAMES = {
    REKEP_TAGS[column]: name for column, name, _datatype, _description in REKEP_FIELD_DECLARATIONS
}
REKEP_COMPONENTS = frozenset(REKEP_COMPONENT_NAMES)

#: The collapse report, committed beside the dictionary it describes.
CONFLICTS = DATA.parent / "fix-conflicts.json"

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


def test_the_archive_holds_one_file_per_registry_identity() -> None:
    """The layout itself: field shards, components, repeating groups, and an index."""
    assert len(VERSIONS) == EXPECTED_VERSIONS
    with zipfile.ZipFile(DATA) as archive:
        names = archive.namelist()
    assert len(names) == len(set(names)), "one member per name, never a shadowed one"
    folders = {name.split("/")[0] if "/" in name else name for name in names}
    assert folders == {
        "fields",
        "components",
        "namespaces",
        "repgroup",
        "sources.json",
        "versions.json",
    }
    assert len(members("fields")) == EXPECTED_FIELD_DOCUMENTS
    assert len(records()) == EXPECTED_FIELD_RECORDS
    assert len(members("components")) == EXPECTED_COMPONENT_FILES
    assert len(members("repgroup")) == EXPECTED_REPEATING_GROUP_FILES
    assert VERSIONS[0] == "5.0.SP2", "newest first"
    assert VERSIONS[-1] == "FIXT1.1", "and the transport last"
    assert set(INDEX["sessions"]) <= set(VERSIONS)
    assert set(INDEX["declared"]) == set(VERSIONS), "every version's spec was read"


def test_every_field_is_in_the_document_the_arithmetic_names() -> None:
    """`tag // 1000`: no index in `versions.json`, no lookup table, no scan.

    And one rule for every document, including the one the fields FIX never
    numbered share: they key by name, so they land in `NAMED_SHARD` rather
    than in a document with a different kind of name.
    """
    with zipfile.ZipFile(DATA) as archive:
        shards = {
            name: json.loads(archive.read(name).decode("utf-8"))
            for name in sorted(archive.namelist())
            if name.startswith("fields/")
        }
    for name, shard in shards.items():
        assert isinstance(shard, list), name
        for record in shard:
            fix = record["fix"]
            key = int(fix["tag"]) if "tag" in fix else record["name"]
            assert field_document(key) == name, key
    populated = {int(name[len("fields/") : -len(".json")]) for name in shards}
    assert len(populated) == EXPECTED_FIELD_DOCUMENTS
    assert NAMED_SHARD in populated, "the fields with no tag are one shard among them"
    tagged = populated - {NAMED_SHARD}
    assert max(tagged) * SHARD_SPAN >= 50000, "the extension packs, up at 50002"


def test_a_field_record_is_one_reading_and_the_versions_that_declare_it() -> None:
    """The whole point of the shape: tag 64 is one record, not eight readings.

    `SettlDate` is what 4.4 and after call it; the four versions before spelled
    it `FutSettDate`, which the collapse kept as an alias so a capture still
    writing it resolves.
    """
    held = records()
    settl = stored_fix(held["64"])
    assert held["64"]["name"] == "SettlDate" and settl["tag"] == "64"
    assert settl["versions"] == [
        "4.0",
        "4.1",
        "4.2",
        "4.3",
        "4.4",
        "5.0",
        "5.0.SP1",
        "5.0.SP2",
    ]
    assert [alias["name"] for alias in settl["aliases"]] == ["FutSettDate"]

    tags = set()
    vendor = 0
    for key, record in held.items():
        assert record["name"], key
        assert set(record) <= FIELD_KEYS, key
        fix = stored_fix(record)
        assert fix["versions"], key
        if "tag" not in fix:
            # A field FIX never numbered: no tag, keyed by name, and holding
            # for whichever version the session negotiated. Having no tag is
            # the whole of being namespaced -- nothing states it twice.
            vendor += 1
            assert fix["versions"] == [ANY_VERSION], key
            continue
        assert fix["tag"] not in tags, f"{key} repeats a tag"
        assert fix["tag"] == key, "a record is filed under its own tag"
        tags.add(fix["tag"])
        if int(key) in REKEP_TAG_VALUES:
            assert fix["versions"] == [ANY_VERSION], key
            assert record["name"] == REKEP_FIELD_NAMES[int(key)], key
        else:
            assert set(fix["versions"]) <= set(VERSIONS), key
    assert vendor == 3, "ISINCODE and the parent identities the log gives columns"
    assert stored_fix(held["ISINCODE"])["column"] == "isincode", (
        "a declared column is stored folded, the way the lift names it"
    )
    assert [alias["name"] for alias in stored_fix(held["ISINCODE"])["aliases"]] == ["AMON.ISINCODE"]


def test_scraped_protocol_names_are_identifiers_not_page_labels() -> None:
    held = records()
    for key, record in held.items():
        fix = stored_fix(record)
        if "tag" in fix:
            name = str(record["name"])
            assert name.isalnum(), key
            assert all(str(alias["name"]).isalnum() for alias in fix.get("aliases", ())), key
        assert all(str(name).isalnum() for name in fix.get("msgtypes", ())), key
        assert all(str(name).isalnum() for name in fix.get("components", ())), key

    msg_type = held["35"]
    assert stored_value(msg_type, "8")["meaning"] == "ExecutionReport"
    assert stored_value(msg_type, "i")["meaning"] == "MassQuote"
    assert stored_fix(msg_type)["event_types"]["8"] == "EXECUTION", (
        "by name, because a packed ASCII code is unreadable in a file people edit"
    )
    assert "handlers" not in msg_type
    assert "encoded" not in stored_fix(msg_type) and "decoded" not in stored_fix(msg_type), (
        "a lookup derived from the values is not stored beside them"
    )
    assert stored_fix(held["32"])["aliases"] == [
        {"name": "LastShares", "source": "4.2", "occurrences": 0}
    ], "tag 32's pre-4.3 name is a spelling of it, sourced to the version that used it"


def test_every_stored_document_is_a_field_document() -> None:
    """One serialization for the whole dictionary.

    A shard is a list of field records and a component is the `Field` it
    declares, so nothing here needs a second codec, and a key above a record
    is that record's own identity written twice.
    """
    with zipfile.ZipFile(DATA) as archive:
        for name in sorted(archive.namelist()):
            document = json.loads(archive.read(name).decode("utf-8"))
            if name.startswith("fields/") or "/fields/" in name:
                assert isinstance(document, list), name
                for record in document:
                    assert set(record) <= FIELD_KEYS, name
                    assert record["name"] and record["fix"]["versions"], name
            elif name.startswith(("components/", "repgroup/")) or "/components/" in name:
                assert isinstance(document, dict), name
                assert set(document) <= FIELD_KEYS, name
                assert document["fix"]["versions"], "the versions ride in its own fix"
                assert "declaration" not in document, "a component is its declaration"


def test_a_component_record_is_one_declaration_and_its_versions() -> None:
    """The same for a component, and its declaration is a Field document: a
    struct of members, a list where one of them repeats, and `fix` at every
    level -- the shape every other declaration in this package is stored as."""
    declared = members("components")["parties"]
    assert declared["name"] == "Parties"
    assert declared["fix"]["versions"] == ["4.3", "4.4", "5.0", "5.0.SP1", "5.0.SP2"], (
        "the versions declaring it ride in its own `fix`, as a field record's do"
    )
    assert declared["type"] == "struct" and declared["fix"]["component"] == "Parties"
    assert "msgtype" not in declared["fix"], "a reusable block is not a message definition"
    carried = declared["fix"]["msgtypes"]
    assert {"NewOrderSingle", "ExecutionReport"} <= set(carried), "which messages carry it"
    assert carried == sorted(carried)
    group = declared["fields"][0]
    assert group["name"] == "NoPartyIDs"
    assert group["type"] == "list" and group["fix"]["tag"] == "453"
    assert group["item"]["fields"][0]["name"] == "PartyID"


def test_a_repeating_group_is_the_common_list_field_its_components_use() -> None:
    declared = members("repgroup")["no_party_i_ds"]

    assert declared["name"] == "NoPartyIDs"
    assert declared["type"] == "list" and declared["fix"]["tag"] == "453"
    assert declared["item"]["type"] == "struct"
    assert [member["name"] for member in declared["item"]["fields"][:3]] == [
        "PartyID",
        "PartyIDSource",
        "PartyRole",
    ]
    embedded = members("components")["parties"]["fields"][0]
    assert (embedded["name"], embedded["fix"]["tag"]) == ("NoPartyIDs", "453")
    assert embedded["item"]["fields"][-1] == {
        "name": "PtysSubGrp",
        "type": "struct",
        "nullable": True,
        "fix": {"component": "PtysSubGrp"},
        "fields": [],
    }, "the owning component keeps its nested reference"
    assert declared["item"]["fields"][-1]["name"] == "NoPartySubIDs", (
        "the group index exposes the recursively resolved list"
    )


def test_a_message_is_stored_as_the_component_it_is() -> None:
    """One folder, one record shape: the MsgType is the whole difference."""
    declared = members("components")["new_order_single"]
    assert declared["name"] == "NewOrderSingle"
    assert declared["fix"]["msgtype"] == "D"
    assert "msgtypes" not in declared["fix"], "a message is not carried by a message"
    assert declared["fields"][0] == {
        "name": "StandardHeader",
        "type": "struct",
        "fix": {"component": "StandardHeader"},
        "fields": [],
    }
    assert [member["name"] for member in declared["fields"][1:5]] == [
        "ClOrdID",
        "OrderRequestID",
        "SecondaryClOrdID",
        "ClOrdLinkID",
    ], "the newest tree, as every record here keeps"
    stored = members("components")
    messages = [one for one in stored.values() if one["fix"].get("msgtype")]
    assert len(messages) == EXPECTED_MESSAGE_FILES
    assert len(stored) == EXPECTED_COMPONENT_FILES


def test_a_value_resolves_from_its_prose_its_symbol_or_itself(registry: FixRegistry) -> None:
    """The real dictionary uses one codec path for prose, symbols and values."""
    stamps = registry.resolve("TrdRegTimestampType").fix
    assert stamps.encode("Order Submission Time") == "10"
    assert stamps.encode("ORDER_SUBMISSION_TIME") == "10"
    assert stamps.encode("ordersubmissiontime") == "10"
    assert stamps.encode("10") == "10"
    assert stored_value(records()["770"], "10")["aliases"] == [
        "OrderSubmissionTime",
        "ORDER_SUBMISSION_TIME",
    ]


def test_the_collapse_report_is_committed_and_is_what_the_build_makes() -> None:
    """Every source and version disagreement is committed with its attribution.

    A reviewable list rather than a silent drop -- and a baseline, so a
    dictionary refresh cannot quietly introduce conflicts nobody looked at.
    """
    report = ConflictReport.from_dict(json.loads(CONFLICTS.read_text(encoding="utf-8")))
    assert report.counts() == dict(CONFLICT_BASELINE)
    assert beyond_baseline(report) == []
    values = [one for one in report.collapses if one.part == "values"]
    assert len(values) == CONFLICT_BASELINE["values"] == 949
    assert {"AccountType", "AcctIDSource", "AllocStatus", "AllocTransType"} <= {
        one.name for one in values
    }
    assert all(one.dropped for one in report.collapses), "an entry with no loss is not one"
    assert all(one.keptsource for one in report.collapses)
    assert all(dropped.source for one in report.collapses for dropped in one.dropped)
    assert all(one.name.isalnum() for one in report.collapses)


#: The prose the sources wrote up, per version, derived then pinned.
#: A count and not a ratio, because the sources cover different amounts:
#: the prose sites write up the standard's own tags, and the QuickFIX spec numbers
#: every field an extension pack added -- five thousand of them in 5.0.SP2,
#: arriving typed and undocumented. A ratio over that set would say the
#: dictionary got worse for having grown.
EXPECTED_DESCRIBED: dict[str, int] = {
    "4.0": 142,
    "4.1": 213,
    "4.2": 408,
    "4.3": 660,
    "4.4": 956,
    "5.0": 1133,
    "5.0.SP1": 1418,
    "5.0.SP2": 6248,
    "FIXT1.1": 77,
}


@pytest.mark.parametrize("version", VERSIONS)
def test_a_version_carries_what_its_pages_say(version: str, registry: FixRegistry) -> None:
    """Typed, described, and enumerated where the field is an enumeration.

    Every field carries a type, so that is a ratio.
    Prose is an exact published count: see `EXPECTED_DESCRIBED`.
    """
    fields = [
        member for member in registry.fields(version) if member.fix.tag not in REKEP_TAG_VALUES
    ]
    typed = sum(1 for member in fields if member.fix.get("type"))
    described = sum(1 for member in fields if member.description)
    enumerated = sum(1 for member in fields if member.fix.get("values"))
    assert typed / len(fields) > 0.95
    assert described == EXPECTED_DESCRIBED[version]
    assert enumerated > EXPECTED_DESCRIBED[version] // 10, "a tenth of FIX is enumerations"


def test_the_dump_answers_a_lookup_offline(registry: FixRegistry) -> None:
    """What the directory is for: a registry that never fetches anything."""
    side = registry.field("Side")
    assert side.fix["tag"] == "54"
    assert side.fix["version"] == "5.0.SP2", "the newest version that has it"
    assert side.description == 'Side of order (see Volume : "Glossary" for value definitions)'
    assert side.fix.value_of("1").meaning == "Buy"
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
    rebuilt = publish_full(DATA, tmp_path / "fix.zip")
    assert rebuilt.read_bytes() == DATA.read_bytes()


def test_full_publication_registers_rekep_in_a_clean_store(tmp_path: Path) -> None:
    source = tmp_path / "source"
    registry = FixRegistry(cache_dir=source)
    venue = fix_field("VenueField", 49999, "String")
    venue.fix.versions = ("9.1",)
    registry.add_field(venue)

    target = publish_full(source, tmp_path / "full.zip")
    stored = FixRegistry(cache_dir=source)
    archived = FixRegistry(cache_dir=target)

    assert stored.field(49999).name == archived.field(49999).name == "VenueField"
    assert rekep_is_registered(stored)
    assert rekep_is_registered(archived)


def test_a_projection_is_a_small_exact_offline_registry(
    registry: FixRegistry, tmp_path: Path
) -> None:
    target = registry.into_projection(tmp_path / "projected.zip", ["Side", "QuoteID"])
    projected = FixRegistry(cache_dir=target)
    assert projected.versions == registry.versions
    assert set(projected.tags()) == {"side", "quoteid"}
    for version in projected.versions:
        expected = [
            member for member in registry.fields(version) if member.name in {"Side", "QuoteID"}
        ]
        assert projected.fields(version) == expected
    # Three quarters of the published dictionary for two fields, and nearly all
    # of the remainder is declarations: those travel whole rather than being
    # selected with the fields, because a component says where a repeating
    # group starts and ends and a tree missing members would end it elsewhere.
    # The messages are the bulk of them, and travel for the same reason: a
    # projection that could not say what a `D` is would be one every reader
    # had to fetch the whole dictionary to get past.
    assert target.stat().st_size < DATA.stat().st_size * 80 // 100
    with zipfile.ZipFile(target) as opened:
        fields = [name for name in opened.namelist() if name.startswith("fields/")]
    assert sorted(fields) == ["fields/000000.json"], "both tags share one shard"
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


def test_the_default_directory_matches_the_published_archive(
    registry: FixRegistry,
) -> None:
    """`data/fix` is what every unconfigured lookup reads, `data/fix.zip` is it
    packed; the two disagreeing would make a lookup depend on which was open."""
    builtin = FixRegistry.from_builtin()
    assert builtin.versions == registry.versions
    assert builtin.namespaces() == registry.namespaces()
    assert builtin.resolve("ISINCODE").fix.tag is None, "and is still resolvable by name"
    assert builtin.field(9001, namespace="fixtrading-udf").name == "MaxShow"
    assert {field.fix.get("namespace") for field in builtin.definitions(9001)} == {
        "fixtrading-udf",
        "clear-street",
    }


def test_archive_and_default_registries_match_the_rekep_declarations(
    registry: FixRegistry,
) -> None:
    assert rekep_is_registered(registry)
    assert rekep_is_registered(FixRegistry.from_builtin())


def test_the_default_registry_answers_every_key_the_package_looks_up(
    registry: FixRegistry,
) -> None:
    """`publish.REQUIRED_FIELDS` is a hand-written contract coverage list.

    A field added to the log schema or to market translation but not to the
    list would ship a registry that cannot answer for it -- which is the same
    silence as a name nobody has ever seen, and reads as one downstream.
    """
    assert not missing_from(registry, REQUIRED_FIELDS), "the dictionary answers for every key"
    assert set(FIXMSG_FIELDS) == set(_ORDER)
    assert set(REQUIRED_FIELDS) >= set(CARRIED_FIELDS)
    assert set(MARKET_FIELDS).isdisjoint(FIXMSG_FIELDS), "each name declared once"
    builtin = FixRegistry.from_builtin()
    assert not missing_from(builtin, REQUIRED_FIELDS)
    assert not missing_from(builtin, tuple(market_tags())), "every tag translation reads"


def test_the_default_registry_carries_the_component_declarations(
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
    assert [member.name for member in members_of(parties)] == ["NoPartyIDs"]
    assert members_of(parties)[0].fix.tag == 453
    package_components = set(REKEP_COMPONENT_NAMES[:2])
    for version in registry.versions:
        assert builtin.components_available(version), version
        declared = builtin.components(version)
        assert [one for one in declared if one.name not in package_components] == (
            [one for one in registry.components(version) if one.name not in package_components]
        ), version
        assert {one.name for one in declared if one.name in package_components} == (
            package_components
        ), version
    declared = {
        version
        for version in registry.versions
        if any(
            component.name not in package_components for component in builtin.components(version)
        )
    }
    assert declared == {
        "4.3",
        "4.4",
        "5.0",
        "5.0.SP1",
        "5.0.SP2",
        "FIXT1.1",
    }
    assert {"4.0", "4.1", "4.2"}.isdisjoint(declared), "no standard component existed before 4.3"


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
    """Derived, then pinned: the dictionary compresses to under a third.

    Under a third, where one document per identity managed two thirds: seven
    hundred members give deflate whole shards of repeated keys to work with,
    and the zip's own directory stops being a quarter of the file. The bound is
    here to catch a *stored* archive, not to chase the last percent.
    """
    with zipfile.ZipFile(DATA) as archive:
        stored = sum(entry.file_size for entry in archive.infolist())
    assert stored > 2_500_000, "the JSON inside is the whole dictionary"
    assert DATA.stat().st_size < stored // 3


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
    assert guessed == [], "every spelling a record keeps projects to a type"
    # The dictionary's own slips (`Quantity`, `Day`, `Stirng`) are older
    # versions' spellings, and a record keeps the newest -- which is the
    # correct one in each case, and never a string standing in for a number.
    assert registry.field("RatioQty", "4.3").dtype == pyarrow.float64()
    assert registry.field("MaturityDay", "4.1").dtype == pyarrow.int64()
    assert registry.field("LegFutSettDate", "4.3").dtype == pyarrow.timestamp("us")


def test_fields_whose_descriptions_fix_utc_store_a_zoned_timestamp() -> None:
    documented = {
        record["name"]: record["type"]
        for record in records().values()
        if "expressed in utc" in str(record.get("description", "")).casefold()
    }

    assert documented == {
        "ContraTradeTime": "timestamp[us, tz=UTC]",
        "CreationTime": "timestamp[us, tz=UTC]",
        "EffectiveTime": "timestamp[us, tz=UTC]",
        "ExpireTime": "timestamp[us, tz=UTC]",
        "OnBehalfOfSendingTime": "timestamp[us, tz=UTC]",
        "OrigSendingTime": "timestamp[us, tz=UTC]",
        "OrigTime": "timestamp[us, tz=UTC]",
        "QuoteSetValidUntilTime": "timestamp[us, tz=UTC]",
        "ResponseTime": "timestamp[us, tz=UTC]",
        "SendingTime": "timestamp[us, tz=UTC]",
        "ValidUntilTime": "timestamp[us, tz=UTC]",
    }


def test_published_versions_keep_the_promoted_field_boundary(registry: FixRegistry) -> None:
    assert registry.field("CFICode", "4.2") is None
    assert registry.field("CFICode", "4.4").fix.tag == 461


def test_published_names_keep_the_classification_examples(registry: FixRegistry) -> None:
    """The small unit registry stays representative of the published vocabulary."""
    names = ("PARTYROLLE", "SIDDE", "TECH.CLIENTID", "VENDOR.SOURCE")
    counts = KeyCounts(counts={name: KeyCount(name, bare=1) for name in names})
    rows = {row.name: row for row in classify(counts, registry).rows}

    assert (rows["PARTYROLLE"].kind, rows["PARTYROLLE"].resolved) == (NEAR, "PartyRole")
    assert (rows["SIDDE"].kind, rows["SIDDE"].resolved) == (NEAR, "Side")
    assert rows["TECH.CLIENTID"].kind == NAMESPACE
    assert rows["VENDOR.SOURCE"].kind == NAMESPACE


# -- source merging -----------------------------------------------------------


def test_the_published_dictionary_carries_the_symbol_beside_the_description(
    registry: OfflineRegistry,
) -> None:
    """Both halves of a value remain distinct after source merging.

    Nanoconda writes `Buy` as both the name and the short description. Its name
    leads the aliases, and the case-only QuickFIX spelling is not stored twice.
    """
    side = registry.field(54, "4.4")
    assert side.fix.value_of("1").meaning == "Buy"
    assert side.fix.value_of("1").aliases == ("Buy", "BUY")
    assert side.fix.value_of("3").aliases == ("BuyMinus", "BUY_MINUS")


def test_the_published_dictionary_pins_source_coverage_and_provenance(
    registry: OfflineRegistry,
) -> None:
    assert registry.source_coverage() == {
        "clear-street": {"primary": 11, "fields": 11},
        "fix-latest": {"primary": 6203, "fields": 6203},
        "fixtrading-udf": {"primary": 4028, "fields": 4028, "fallbacks": 5},
        "nanoconda": {"primary": 0, "fields": 1487},
        "onixs": {"primary": 43, "fields": 1495},
        "quickfix": {"primary": 0, "fields": 6066},
    }
    standard = [
        entry
        for entry in registry.field_records().values()
        if entry.fix.tag is not None and entry.fix.tag not in REKEP_TAG_VALUES
    ]
    assert all(entry.fix.source == entry.fix.sources[0] for entry in standard)


def test_the_published_dictionary_pins_complete_source_artifacts(
    registry: OfflineRegistry,
) -> None:
    manifest = {source["source_id"]: dict(source) for source in registry.source_manifest()}
    required = {"source_id", "namespace", "url", "version", "format", "checksum", "license_url"}

    assert set(manifest) == {"fix-latest", "fixtrading-udf", "quickfix", "clear-street"}
    assert all(required <= source.keys() for source in manifest.values())
    assert {source["projection"] for source in manifest.values()} == {"rekep-fix-registry-v1"}
    assert {
        source: (record["namespace"], record["version"], record["format"], record["checksum"])
        for source, record in manifest.items()
    } == {
        "fix-latest": (
            "standard",
            "FIX.5.0SP2_EP309",
            "orchestra",
            "sha256:9ea5ee01a90019eb2d307cdd91e3fbec0b4a9249bc196da62d08417c9df3da07",
        ),
        "fixtrading-udf": (
            "fixtrading-udf",
            "1.0",
            "orchestra",
            "sha256:dbc1dfbd9deea8180303edd28145e7494ffeae273e0ccee3cc72ecdeefe9afe5",
        ),
        "quickfix": (
            "standard",
            "FIX.5.0SP2_EP280",
            "quickfix",
            "sha256:7d34e565586dd4096a08691d10e415b5a2fd531a8dadfcfc831daea419d3c3f3",
        ),
        "clear-street": (
            "clear-street",
            "FIX.4.2",
            "markdown",
            "sha256:cbcff58055c619f4f93cea9ee43124bde59b0a90b5bd4059180079f53e0846d3",
        ),
    }


def test_the_published_dictionary_pins_registered_udfs_and_venue_alternatives(
    registry: OfflineRegistry,
) -> None:
    udf = tuple(registry.field_records("fixtrading-udf").values())

    assert len(udf) == 4028
    assert len({field.fix.tag for field in udf}) == 4028
    assert len({field.fix.folded for field in udf}) == 4028
    assert all(5000 <= field.fix.tag <= 9999 for field in udf)

    max_show = registry.definition(9001, "fixtrading-udf")
    cross = registry.definition(9002, "fixtrading-udf")
    support = registry.definition(9003, "fixtrading-udf")
    venue = registry.definition(9001, "clear-street")
    assert max_show is not None and max_show.fix.canonical == "MaxShow"
    assert max_show.fix["source-name"] == "MaxShow1"
    assert (max_show.fix["replacement-tag"], max_show.fix["replacement-name"]) == (
        "210",
        "MaxShow",
    )
    assert cross is not None and cross.dtype == pyarrow.string()
    assert cross.fix["type-fallback"] == "prose permits alphanumeric values"
    assert support is not None
    assert [(value.value, value.meaning) for value in support.fix.enumerated] == [
        ("1", "Supports UDFs in the message"),
        ("2", "Supports UDFs in repeating groups"),
    ]
    assert venue is not None and venue.fix.canonical == "TradeType"
    assert [field.fix.canonical for field in registry.definitions(9001)] == [
        "MaxShow",
        "TradeType",
    ]

    components = registry.component_records("fixtrading-udf")
    assert len(components) == 126
    assert all(
        component.declaration.fix["namespace"] == "fixtrading-udf"
        for component in components.values()
    )
    assert registry.repeating_group_records("fixtrading-udf") == {}


def test_every_published_value_keeps_its_wire_value_and_meaning(
    registry: OfflineRegistry,
) -> None:
    """A source symbol is optional; the wire value and meaning are not."""
    values = [
        (entry, value)
        for entry in registry.field_records().values()
        for value in entry.fix.enumerated
    ]
    assert values
    assert all(value.value and value.meaning for _entry, value in values)
    without_symbol = [(entry, value) for entry, value in values if not value.aliases]
    assert without_symbol, "a symbol identical to its wire value is not stored twice"
    assert all(entry.fix.decode(value.value) == value.value for entry, value in without_symbol)
    side = registry.field("Side")
    assert side is not None
    assert side.fix.decode("future-code") == "future-code", "unknown values remain raw"


def test_every_version_published_here_has_its_symbols(registry: OfflineRegistry) -> None:
    """Derived from the archive, then pinned: a version that lost them fails here."""
    counted = {
        version: sum(
            1
            for member in registry.fields(version)
            if any(one.aliases for one in member.fix.enumerated)
        )
        for version in registry.versions
    }
    assert counted == {
        "4.0": 43,
        "4.1": 59,
        "4.2": 122,
        "4.3": 195,
        "4.4": 308,
        "5.0": 373,
        "5.0.SP1": 446,
        "5.0.SP2": 2026,
        "FIXT1.1": 16,
    }


def test_a_symbol_is_never_written_where_a_description_goes(registry: OfflineRegistry) -> None:
    """The failure this shape exists to prevent, checked across the archive.

    `description=` in the spec holds the value's *name*, so a merge that wrote
    it into `values` would replace every prose reading with SHOUTING_SNAKE.
    """
    shouted = 0
    for version in registry.versions:
        for member in registry.fields(version):
            for text in (one.meaning for one in member.fix.enumerated):
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
