"""A whole store: the shards it is written in, editing it, bootstrapping it.

`test_entries.py` holds one record to its schema and `test_data.py` reads the
published dictionary. These are about the store around them -- which document a
tag lands in and how few are read to answer for it, what a change to one record
is allowed to do, what a name resolves to, what a cold registry does about
being cold, and which explicit operation may read source dictionaries.

Every identity is synthetic. The one real name any of this uses is a FIX
version, which is a schema fact and not data.
"""

from __future__ import annotations

import io
import json
import re
import socket
import urllib.request
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow
import pytest

import rekep
from rekep.enums import EventType, State
from rekep.fields import Field, FixFieldValue
from rekep.fields.metadata import values_of
from rekep.fix import registry as registry_module
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    Alias,
    ComponentRecord,
    record_copy,
    record_kind,
)
from rekep.fix.fields import fix_field, namespaced_field
from rekep.fix.quickfix import block, field_member, group_member, members_of
from rekep.fix.registry import FixRegistry, QuickFixSource, _problems
from rekep.fix.store import (
    SHARD_SPAN,
    ConflictReport,
    collapse,
    field_document,
)
from rekep.fix.transcribe import FixCodec

#: The published dictionary, for the reads that have to answer over the real one.
PUBLISHED = Path(__file__).resolve().parents[3] / "data" / "fix.zip"


def _record(
    name: str,
    tag: int | None = None,
    datatype: str = "String",
    *,
    description: str | None = None,
    versions: Sequence[str] = ("9.1",),
    **fixed: Any,
) -> Field:
    """A record as a caller declares one: numbered when it has a tag, named when not.

    Which of the two it is decides everything downstream -- the shard it lands
    in, whether a tag lookup can reach it -- so it is the only thing this has
    to say; the rest of the FIX metadata a case cares about is passed through
    by its own name (`column=`, `enumerated=`, `states=`).
    """
    if tag is None:
        record = namespaced_field(name, datatype, description=description)
    else:
        record = fix_field(name, tag, datatype, description=description)
        record.fix.versions = versions
    for key, value in fixed.items():
        setattr(record.fix, key, value)
    return record


def _registry_documents() -> dict[str, dict[str, Any]]:
    field = _record("FakeRole", 90001, "int")
    component = ComponentRecord(name="FakeParties", versions=("9.1",))
    return {
        "versions.json": {
            "versions": ["9.1"],
            "stored": ["9.1"],
            "declared": ["9.1"],
            "sessions": {"9.1": [["FakeRole", True]]},
        },
        field_document(90001): {"90001": field.into_dict()},
        "components/fake_parties.json": component.into_dict(),
    }


def _registry_archive(documents: dict[str, dict[str, Any]]) -> bytes:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, document in documents.items():
            archive.writestr(name, json.dumps(document))
    return payload.getvalue()


class _Response(io.BytesIO):
    """A context-managed HTTP response fixture."""

    def __init__(self, payload: bytes, length: bool = True) -> None:
        super().__init__(payload)
        self.headers = {"Content-Length": str(len(payload))} if length else {}

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class Offline(FixRegistry):
    """A registry that must answer from the store alone."""

    def _fetch(self, url: str) -> str:
        raise OSError(f"offline: {url}")


def _pairs(array: pyarrow.Array, row: int = 0) -> list[tuple[object, str]]:
    """One `entries` or map cell in the pair form the assertions read."""
    return [
        (entry["key"], entry["value"]) if isinstance(entry, dict) else tuple(entry)
        for entry in array.to_pylist()[row] or ()
    ]


def _field(name: str, tag: int | None, version: str, datatype: str = "String") -> Field:
    if tag is None:
        member = Field(name=name, metadata={"fix:type": datatype})
        member.fix["version"] = version
        return member
    return fix_field(name, tag, datatype, version=version)


@pytest.fixture
def store(tmp_path: Path) -> Offline:
    """A store holding two synthetic versions of two synthetic fields."""
    registry = Offline(cache_dir=tmp_path / "fix")
    registry._store_versions(("9.1", "9.0"))
    registry._store_fields(
        "9.0",
        [_field("FakeRoleCode", 90001, "9.0", "char"), _field("FakeCode", 90002, "9.0")],
        components=[],
    )
    registry._store_fields(
        "9.1",
        [_field("FakeRole", 90001, "9.1", "int"), _field("FakeCode", 90002, "9.1")],
        session=(("FakeRole", True),),
        components=[
            block(
                "FakeParties",
                [
                    group_member(
                        "NoFakeParties", 90003, [field_member("FakeRole", 90001, required=True)]
                    )
                ],
            )
        ],
    )
    return registry


# -- the one layout ----------------------------------------------------------


def test_a_cold_store_is_written_as_tag_shards(store: Offline) -> None:
    folder = Path(store.cache_dir)
    assert sorted(path.name for path in folder.iterdir()) == [
        "components",
        "fields",
        "repgroup",
        "versions.json",
    ]
    # Tags 90001 and 90002 share the 90000--90999 shard.
    assert [path.name for path in (folder / "fields").iterdir()] == ["000090.json"]
    assert sorted(json.loads((folder / "fields" / "000090.json").read_text())) == [
        "90001",
        "90002",
    ]


def test_incremental_folding_keeps_every_contributing_source(tmp_path: Path) -> None:
    registry = Offline(cache_dir=tmp_path / "fix")
    older = fix_field(
        "Side",
        54,
        "char",
        version="4.2",
        values=(FixFieldValue("1", "Buy", ("Buy",)),),
    )
    older.fix.source = "onixs"
    older.fix.sources = ("onixs",)
    older.fix.added = "2.7"
    older.fix.origins = {
        "added": "onixs",
        "values": {"1": "onixs"},
        "aliases": {"1": "onixs"},
    }
    newer = fix_field(
        "Side",
        54,
        "char",
        version="4.4",
        values=(FixFieldValue("2", "Sell", ("Sell",)),),
    )
    newer.fix.source = "nanoconda"
    newer.fix.sources = ("nanoconda",)
    newer.fix.origins = {
        "values": {"2": "nanoconda"},
        "aliases": {"2": "nanoconda"},
    }

    registry._store_fields("4.2", [older])
    registry._store_fields("4.4", [newer])

    stored = registry.field(54)
    assert stored.fix.source == "nanoconda"
    assert stored.fix.sources == ("nanoconda", "onixs")
    assert stored.fix.added == "2.7"
    assert stored.fix.origins == {
        "added": "onixs",
        "values": {"1": "onixs", "2": "nanoconda"},
        "aliases": {"1": "onixs", "2": "nanoconda"},
    }

    document = json.loads((tmp_path / "fix" / field_document(54)).read_text())["54"]
    assert document["fix"]["sources"] == ["nanoconda", "onixs"]
    assert document["fix"]["origins"]["values"] == {
        "1": "onixs",
        "2": "nanoconda",
    }
    assert [value["value"] for value in document["fix"]["values"]] == ["1", "2"]
    assert document["fix"]["type"] == "char", "plain metadata remains plain text"

    reopened = Offline(cache_dir=tmp_path / "fix").field(54)
    assert reopened is not None and reopened.metadata == stored.metadata


@pytest.mark.parametrize(
    ("tag", "document"),
    [
        (0, "fields/000000.json"),
        (54, "fields/000000.json"),
        (499, "fields/000000.json"),
        (500, "fields/000000.json"),
        (999, "fields/000000.json"),
        (1000, "fields/000001.json"),
        (40000, "fields/000040.json"),
        (50002, "fields/000050.json"),
        ("isincode", "fields/999999.json"),
        ("FAKE.VENDOR.CODE", "fields/999999.json"),
    ],
)
def test_which_document_holds_a_field_is_arithmetic(tag: int | str, document: str) -> None:
    """No index, no lookup table, no scan: `tag // 1000`, zero-padded.

    A field FIX never numbered keys by its name instead, and lands in the one
    shard index no tag can reach -- so every field document is named the same
    way and nothing asks whether a record has a tag to find it.
    """
    assert field_document(tag) == document
    assert SHARD_SPAN == 1_000


class Counting:
    """A `Documents` that records every name it was asked to read."""

    def __init__(self, documents: Any) -> None:
        self.documents = documents
        self.read_names: list[str] = []

    def read(self, name: str) -> dict[str, Any] | None:
        self.read_names.append(name)
        return self.documents.read(name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.documents, name)


def test_a_single_tag_lookup_deserializes_one_shard() -> None:
    """Over the published dictionary: ten shards, and a tag reads one."""
    registry = FixRegistry(cache_dir=PUBLISHED)
    counted = Counting(registry._documents)
    registry.__dict__["_documents"] = counted

    assert registry.field(54).name == "Side"

    opened = [name for name in counted.read_names if name.startswith("fields/")]
    assert opened == ["fields/000000.json"], "the shard tag 54 is in, and no other"
    assert "_fields" not in registry._layout.__dict__, "the dictionary was never read whole"


def test_one_layout_is_all_that_is_left_of_the_store() -> None:
    """Grep, as a test: a reader nothing writes is what this shape removed.

    A store is sharded, and there is no second spelling to sniff for -- so the
    detection, the two layout classes and the component extractor's legacy tags
    are gone from the package rather than left behind unused.
    """
    source = Path(rekep.__file__).parent
    gone = re.compile(
        r"\b(VERSIONED|EXPLODED|LAYOUTS|layout_of|ExplodedLayout|VersionedLayout|into_fallback)\b"
    )
    found = {
        f"{path.relative_to(source)}:{number}"
        for path in sorted(source.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if gone.search(line)
    }
    assert found == set()


def test_a_field_fix_never_numbered_is_kept_where_a_name_can_be_found(store: Offline) -> None:
    """It has no tag to shard on, so it shares the one shard those live in."""
    named = field_document("FAKE.VENDOR.CODE")
    store.add_field(_record("FAKE.VENDOR.CODE", column="fakevendorcode"))
    assert (Path(store.cache_dir) / named).exists()
    assert list(json.loads((Path(store.cache_dir) / named).read_text())) == ["FAKE.VENDOR.CODE"]
    assert Offline(cache_dir=store.cache_dir).resolve("fake.vendor.code") is not None


# -- what a rename does ------------------------------------------------------


def test_a_renamed_tag_is_one_identity_and_the_older_spelling_an_alias(store: Offline) -> None:
    """One tag, one record, and the name 9.0 used still resolves to it."""
    entry = store.resolve("FakeRole")
    assert entry.fix.tag == 90001 and entry.fix.canonical == "FakeRole"
    assert entry.fix.versions == ("9.0", "9.1")
    assert [alias.name for alias in entry.fix.named_aliases] == ["FakeRoleCode"]
    assert store.resolve("FakeRoleCode") is entry
    stored = json.loads((Path(store.cache_dir) / field_document(90001)).read_text())
    assert stored["90001"]["fix"]["aliases"] == [
        {"name": "FakeRoleCode", "source": "9.0", "occurrences": 0}
    ]


def test_every_version_reads_back_the_one_reading(store: Offline) -> None:
    """The collapse, seen from a lookup: the newest name and type, whichever was asked."""
    assert store.field(90001, "9.1").name == "FakeRole"
    assert store.field(90001, "9.0").name == "FakeRole"
    assert store.field(90001, "9.0").fix["type"] == "int", "9.0 said char, and 9.1 won"
    assert store.field(90001, "9.0").fix["version"] == "9.0", "and it says which was asked"


def test_storing_a_version_says_what_that_version_has(store: Offline) -> None:
    """A field a rewritten version no longer names has lost that version."""
    store._store_fields("9.0", [_field("FakeCode", 90002, "9.0")])
    assert [member.name for member in store.fields("9.0")] == ["FakeCode"]
    assert store.resolve("FakeRole").fix.versions == ("9.1",)
    store._store_fields("9.1", [_field("FakeCode", 90002, "9.1")])
    assert store.resolve("FakeRole") is None, "its last version went, and so did the record"
    assert "90001" not in json.loads((Path(store.cache_dir) / field_document(90001)).read_text())


# -- resolving a name --------------------------------------------------------


def test_a_name_resolves_canonical_then_alias(store: Offline) -> None:
    """Two tiers, in the order a rendered key is tried against them."""
    store.alias_field("FakeRole", Alias(name="FakeRolle", source="brk", occurrences=9))
    assert store.resolve("FakeRole").fix.tag == 90001, "tier one: an identity's own name"
    assert store.resolve("FakeCode").fix.tag == 90002
    assert store.resolve("FakeRoleCode").fix.tag == 90001, "tier two: what 9.0 called that tag"
    assert store.resolve("FakeRolle").fix.tag == 90001, "and a spelling somebody recorded"
    assert store.resolve("FAKEROLLE").fix.tag == 90001
    assert store.resolve("fake_rolle").fix.tag == 90001, "separators are not identity"
    assert store.resolve("FakeNothing") is None, "and a name nothing here has is unknown"
    assert store.alias_conflicts() == {}


def test_an_alias_an_earlier_tier_already_answers_for_is_refused(store: Offline) -> None:
    """Recording a spelling nothing will ever reach is a mistake, not precedence."""
    with pytest.raises(ValueError, match="already FakeRole's"):
        store.alias_field("FakeCode", Alias(name="FakeRole"))
    assert store.resolve("FakeRole").fix.tag == 90001, "unchanged, because it was refused"


def test_two_fields_claiming_one_name_in_one_tier_fails_the_check(store: Offline) -> None:
    """Nothing decides between them, so the store says so rather than picking."""
    store.alias_field("FakeRole", Alias(name="FakeSpelling"))
    with pytest.raises(ValueError, match="'fakespelling' is claimed by"):
        store.alias_field("FakeCode", Alias(name="FakeSpelling"))
    assert store.check() == [], "and the refused change was not written"

    # Written past the API, so the check has something to find.
    entry = record_copy(store.resolve("FakeCode"))
    entry.fix.named_aliases = (Alias(name="FakeSpelling"),)
    store._layout.store_field(entry)
    store._forget()
    assert store.check() == ["'fakespelling' is claimed by ['FakeRole', 'FakeCode']"]


def test_an_alias_is_data_and_carries_where_it_came_from(store: Offline) -> None:
    """A near miss counted in a capture is evidence; a name typed in is not."""
    entry = store.alias_field("FakeRole", Alias(name="FakeRolle", source="brk", occurrences=41))
    assert store.resolve("FakeRolle").fix.tag == 90001
    stored = json.loads((Path(store.cache_dir) / field_document(90001)).read_text())
    assert {"name": "FakeRolle", "source": "brk", "occurrences": 41} in stored["90001"]["fix"][
        "aliases"
    ]
    assert entry.fix.named_aliases[-1].occurrences == 41

    again = store.alias_field("FakeRole", "FakeRolle")
    assert len(again.fix.named_aliases) == len(entry.fix.named_aliases), (
        "a spelling already recorded is not twice"
    )


def test_aliasing_a_field_nothing_resolves_is_refused(store: Offline) -> None:
    with pytest.raises(KeyError, match="FakeAbsent"):
        store.alias_field("FakeAbsent", "FakeSomething")


# -- editing the store -------------------------------------------------------


def test_a_field_identity_is_created_updated_and_removed(store: Offline) -> None:
    entry = _record(
        "FAKE.VENDOR.CODE",
        description="A vendor's own.",
        column="fakevendorcode",
    )
    store.add_field(entry)
    assert store.resolve("FAKE.VENDOR.CODE").fix.column == "fakevendorcode"
    assert record_kind(store.field("FAKE.VENDOR.CODE", "9.1")) == NAMESPACE

    renamed = record_copy(entry)
    renamed.fix.column = "renamed"
    store.update_field(renamed)
    assert store.resolve("FAKE.VENDOR.CODE").fix.column == "renamed"

    assert store.remove_field("FAKE.VENDOR.CODE")
    assert store.resolve("FAKE.VENDOR.CODE") is None
    assert not store.remove_field("FAKE.VENDOR.CODE"), "and says so the second time"


def test_promoting_registers_a_rendered_name_and_its_column_in_one_call(store: Offline) -> None:
    """The one-step path: a name never seen becomes a namespaced entry with a column."""
    entry = store.promote_field(
        "FAKE.VENDOR.TS",
        "fakevendorts",
        type="UTCTimestamp",
        description="A vendor's own stamp.",
        aliases=("FAKEVENDORTS",),
    )
    assert record_kind(entry) == NAMESPACE and entry.fix.tag is None
    assert entry.fix.versions == (ANY_VERSION,)
    assert entry.fix.type == "UTCTimestamp"
    assert entry.fix.column == "fakevendorts"
    assert store.resolve("FAKE.VENDOR.TS").fix.column == "fakevendorts"
    assert store.resolve("FAKEVENDORTS").fix.canonical == "FAKE.VENDOR.TS"
    assert store.check() == []


def test_promoting_completes_a_half_registered_name(store: Offline) -> None:
    """A declared name with no column -- what `apply --namespace` leaves -- is
    finished in place, keeping the aliases and counts the run recorded."""
    store.add_field(
        _record(
            "FAKE.VENDOR.CODE",
            named_aliases=[Alias(name="FAKEVENDORCODE", source="brk", occurrences=7)],
        )
    )
    entry = store.promote_field(
        "FAKE.VENDOR.CODE",
        "fakevendorcode",
        description="A vendor's own code.",
        aliases=("FAKEVENDORCODE", "FAKE_VENDOR_CODE"),
    )
    assert entry.fix.column == "fakevendorcode"
    assert entry.description == "A vendor's own code."
    assert [alias.name for alias in entry.fix.named_aliases] == ["FAKEVENDORCODE"]
    assert entry.fix.named_aliases[0].occurrences == 7, "the recorded count survived the promotion"

    again = store.promote_field("FAKE.VENDOR.CODE", "fakevendorcode")
    assert again.fix.column == "fakevendorcode", "the same answer twice is not a conflict"
    assert again.fix.type == "String", "a type left unsaid keeps what the entry holds"
    aliased = store.promote_field("FAKEVENDORCODE", "fakevendorcode")
    assert aliased.fix.canonical == "FAKE.VENDOR.CODE", (
        "any name the entry answers to names it here too"
    )
    retyped = store.promote_field("FAKE.VENDOR.CODE", "fakevendorcode", type="UTCTimestamp")
    assert retyped.fix.type == "UTCTimestamp", "and a type said here is the newest reading"
    reworded = store.promote_field(
        "FAKE.VENDOR.CODE", "fakevendorcode", description="A vendor's code, reconfirmed."
    )
    assert reworded.description == "A vendor's code, reconfirmed.", "so is a said description"


def test_promoting_normalizes_the_column_and_folds_repeated_aliases(store: Offline) -> None:
    """What is stored is what a later call compares against, so it is cleaned
    on the way in: padding would lock the entry against its own spelling, and
    a spelling said twice in one call would be dropped silently on reload."""
    entry = store.promote_field(
        "FAKE.VENDOR.CODE",
        "  fake_vendor_code  ",
        aliases=("FAKEVENDORCODE", "FakeVendorCode"),
    )
    assert entry.fix.column == "fakevendorcode"
    assert [alias.name for alias in entry.fix.named_aliases] == ["FAKEVENDORCODE"]
    assert store.promote_field("FAKE.VENDOR.CODE", "fakevendorcode").fix.column == (
        "fakevendorcode"
    ), "and the unpadded spelling names the same column"


def test_promoting_a_standard_field_is_refused(store: Offline) -> None:
    """A tagged field's column is the dictionary's to declare, not this verb's."""
    with pytest.raises(KeyError, match="standard, with tag 90001"):
        store.promote_field("FakeRole", "fakerole")
    assert store.resolve("FakeRole").fix.column == "", "unchanged, because it was refused"


def test_promoting_refuses_moving_an_assigned_column(store: Offline) -> None:
    """Two runs disagreeing about where a field lands is a conflict, not an update."""
    store.promote_field("FAKE.VENDOR.CODE", "fakevendorcode")
    with pytest.raises(ValueError, match="already lifted into 'fakevendorcode'"):
        store.promote_field("FAKE.VENDOR.CODE", "elsewhere")
    assert store.resolve("FAKE.VENDOR.CODE").fix.column == "fakevendorcode"


def test_promoting_refuses_a_column_another_field_landed_in(store: Offline) -> None:
    """One parsed-log column holds one field; a second claimant is a conflict."""
    store.promote_field("FAKE.VENDOR.CODE", "fakevendorcode")
    with pytest.raises(ValueError, match="two fields cannot land in one column"):
        store.promote_field("FAKE.OTHER.CODE", "fakevendorcode")
    assert store.resolve("FAKE.OTHER.CODE") is None, "nothing was written"


def test_promoting_requires_a_name_and_a_column(store: Offline) -> None:
    with pytest.raises(ValueError, match="requires the column"):
        store.promote_field("FAKE.VENDOR.CODE", "   ")
    with pytest.raises(ValueError, match="requires its name"):
        store.promote_field(" ", "fakevendorcode")
    assert store.resolve("FAKE.VENDOR.CODE") is None, "nothing was written"


def test_msg_type_event_kinds_are_configurable_store_data(store: Offline) -> None:
    entry = _record("MsgType", 35, enumerated={"0": "Heartbeat", "D": "NewOrderSingle"})
    store.add_field(entry)
    before = store.msg_type_event_types()
    classified = record_copy(entry)
    classified.fix.event_types = {"D": EventType.ORDER}
    store.update_field(classified)

    assert before == {"0": EventType.MISC, "D": EventType.MISC}
    assert store.msg_type_event_types() == {"0": EventType.MISC, "D": EventType.ORDER}

    reopened = Offline(cache_dir=store.cache_dir)
    assert reopened.msg_type_event_types() == {
        "0": EventType.MISC,
        "D": EventType.ORDER,
    }
    document = json.loads((Path(store.cache_dir) / field_document(35)).read_text())
    assert document["35"]["fix"]["event_types"] == {"D": "ORDER"}, (
        "by name, because this file is read and edited by hand and the packed "
        "code is a nineteen-digit integer"
    )


def test_market_dispatch_states_and_codecs_are_cached_store_data(
    store: Offline,
) -> None:
    msg_type = _record(
        "MsgType",
        35,
        enumerated={"0": "Heartbeat", "1": "TestRequest", "D": "NewOrderSingle"},
        event_types={"D": EventType.ORDER},
        states={"D": State.PENDING_NEW},
    )
    status = _record(
        "OrdStatus",
        39,
        "char",
        states={"0": State.NEW, "1": State.PARTIALLY_FILLED},
    )
    store.add_field(msg_type)
    store.add_field(status)

    cached = store.state_values("OrdStatus")
    assert cached == {"0": State.NEW, "1": State.PARTIALLY_FILLED}
    assert store.state_values("OrdStatus") is cached
    assert store.state_values("MsgType") == {"D": State.PENDING_NEW}
    assert store.field(35).fix.encode("NewOrderSingle") == "D"

    restated = record_copy(status)
    restated.fix.states = {"0": State.ACCEPTED}
    store.update_field(restated)
    assert store.state_values("OrdStatus") == {"0": State.ACCEPTED}
    assert store.state_values("OrdStatus") is not cached

    reopened = Offline(cache_dir=store.cache_dir)
    assert reopened.state_values("OrdStatus") == {"0": State.ACCEPTED}
    assert reopened.state_values("MsgType") == {"D": State.PENDING_NEW}
    assert reopened.field(35).fix.encode("NewOrderSingle") == "D"


def test_creating_one_that_is_already_there_and_updating_one_that_is_not_are_refused(
    store: Offline,
) -> None:
    with pytest.raises(KeyError, match="already stored"):
        store.add_field(_record("FakeRole", 90001, "int"))
    with pytest.raises(KeyError, match="no FIX field stored"):
        store.update_field(_record("FakeAbsent", 90099))


def test_a_change_is_validated_against_the_whole_store_before_it_is_written(
    store: Offline,
) -> None:
    """Beside what is already there, because an alias alone is never a collision."""
    clashing = _record("FakeOther", 90004, named_aliases=[Alias(name="FakeCode")])
    # `FakeCode` is another field's canonical name, so this alias could never
    # resolve. Precedence is the rule; recording a spelling nothing will ever
    # reach is the mistake, and it is refused rather than written and ignored.
    with pytest.raises(ValueError, match="already FakeCode's"):
        store.add_field(clashing)
    assert store.resolve("FakeOther") is None, "refused whole, never written half"
    assert "90004" not in json.loads((Path(store.cache_dir) / field_document(90004)).read_text())


def test_a_component_identity_is_created_updated_and_removed(store: Offline) -> None:
    entry = ComponentRecord.from_components(
        [block("FakeInstrument", [field_member("FakeCode", 90002)])], ["9.1"]
    )
    store.add_component(entry)
    assert members_of(store.component("FakeInstrument", "9.1"))[0].fix.tag == 90002
    with pytest.raises(KeyError, match="already stored"):
        store.add_component(entry)

    store.update_component(
        ComponentRecord.from_components(
            [block("FakeInstrument", [field_member("FakeRole", 90001)])], ["9.1"]
        )
    )
    assert members_of(store.component("FakeInstrument", "9.1"))[0].fix.tag == 90001

    assert store.remove_component("FakeInstrument")
    assert not store.remove_component("FakeInstrument")
    with pytest.raises(KeyError, match="FakeInstrument"):
        store.component("FakeInstrument", "9.1")


# -- the merged views --------------------------------------------------------


def test_the_whole_unified_table_comes_back_in_one_call(store: Offline) -> None:
    """What a classification run pulls, where `scalar()` answered one key at a time."""
    merged = store.merged_fields()
    assert sorted(merged) == ["FakeCode", "FakeRole"]
    scalar = store.scalar("FakeRole")
    assert merged["FakeRole"].name == scalar.name
    assert merged["FakeRole"].dtype == scalar.dtype
    assert merged["FakeRole"].metadata == scalar.metadata, "the same declaration, in bulk"
    assert json.loads(merged["FakeRole"].fix["versions"]) == ["9.1", "9.0"]


def test_a_merged_field_carries_the_spellings_it_answers_to(store: Offline) -> None:
    store.alias_field("FakeRole", Alias(name="FakeRolle", source="pco", occurrences=7))
    merged = store.merged_fields()["FakeRole"]
    assert {alias["name"] for alias in json.loads(merged.fix["aliases"])} == {
        "FakeRoleCode",
        "FakeRolle",
    }


def test_a_component_is_one_queryable_object_across_every_version(store: Offline) -> None:
    """Not "pick the newest match and hope", which is what `component()` does."""
    entry = store.merged_component("fakeparties")
    assert entry.name == "FakeParties" and entry.versions == ("9.1",)
    assert entry.delimiters() == {("NoFakeParties",): "FakeRole"}
    assert store.component_records()["FakeParties"] is entry
    with pytest.raises(KeyError, match="FakeAbsent"):
        store.merged_component("FakeAbsent")


def test_component_member_metadata_is_readable_json_and_round_trips(store: Offline) -> None:
    """Group entries use the same readable metadata shape as field shards."""
    member = field_member("FakeRole", 90001)
    member.fix["values"] = json.dumps(
        [{"value": "1", "meaning": "one", "aliases": ["ONE"]}], separators=(",", ":")
    )
    entry = ComponentRecord(
        name="ReadableComponent",
        versions=("9.1",),
        declaration=block("ReadableComponent", [group_member("NoFakeRoles", 90003, [member])]),
    )
    store.add_component(entry)

    document = json.loads(
        (Path(store.cache_dir) / "components" / "readable_component.json").read_text()
    )
    values = document["declaration"]["fields"][0]["item"]["fields"][0]["fix"]["values"]
    assert values == [{"value": "1", "meaning": "one", "aliases": ["ONE"]}]

    reopened = Offline(cache_dir=store.cache_dir).merged_component("ReadableComponent")
    assert reopened.declaration.into_dict() == entry.declaration.into_dict()


# -- collapsing, and what it costs -------------------------------------------


def test_a_collapse_keeps_the_newest_reading_and_reports_what_it_dropped() -> None:
    """The judgement the shape asks for, written down rather than inferred."""
    older = fix_field("FakeRole", 90001, "char", version="9.0", values={"1": "Was", "2": "Gone"})
    newer = fix_field("FakeRole", 90001, "int", version="9.1", values={"1": "Is"})
    older.fix.source = "nanoconda"
    older.fix.sources = ("nanoconda", "onixs")
    older.fix.origins = {"type": "onixs", "values": {"1": "onixs", "2": "onixs"}}
    newer.fix.source = "nanoconda"
    newer.fix.sources = ("nanoconda", "quickfix")
    newer.fix.origins = {"type": "quickfix", "values": {"1": "nanoconda"}}
    entries, _, report = collapse(("9.1", "9.0"), {"9.1": [newer], "9.0": [older]}, {})

    entry = entries[90001]
    assert entry.fix.type == "int" and entry.fix.versions == ("9.0", "9.1")
    assert entry.fix.enumerated == values_of({"1": "Is", "2": "Gone"}), (
        "the union, newest winning per key"
    )

    counts = report.counts()
    assert counts["type"] == 1 and counts["values"] == 1
    (values,) = [one for one in report.collapses if one.part == "values"]
    assert values.tag == 90001 and values.kept == "9.1"
    assert values.keptsource == "nanoconda"
    assert [(one.version, one.key, one.reading, one.source) for one in values.dropped] == [
        ("9.0", "1", "Was", "onixs")
    ]
    (typed,) = [one for one in report.collapses if one.part == "type"]
    assert typed.keptsource == "quickfix"
    assert typed.dropped[0].source == "onixs"


def test_a_component_member_collapse_attributes_both_source_readings() -> None:
    older = block(
        "FakeParties",
        (field_member("FakeRole", 90001), field_member("FakeCode", 90002)),
    )
    newer = block("FakeParties", (field_member("FakeRole", 90001),))
    for declared in (older, newer):
        declared.fix.source = "quickfix"
        declared.fix.sources = ("quickfix",)

    _, _, report = collapse(
        ("9.1", "9.0"),
        {},
        {"9.1": (newer,), "9.0": (older,)},
    )

    restored = ConflictReport.from_dict(report.into_dict())
    (members,) = restored.collapses
    assert members.keptsource == "quickfix"
    assert [(one.reading, one.source) for one in members.dropped] == [("FakeCode", "quickfix")]


def test_an_unstated_added_version_is_not_a_conflict() -> None:
    older = fix_field("FakeRole", 90001, "int", version="4.2")
    older.fix.added = "2.7"
    older.fix.source = "nanoconda"
    older.fix.sources = ("nanoconda",)
    older.fix.origins = {"added": "nanoconda"}
    newer = fix_field("FakeRole", 90001, "int", version="4.4")
    newer.fix.source = "quickfix"
    newer.fix.sources = ("quickfix",)

    _, _, report = collapse(("4.4", "4.2"), {"4.4": [newer], "4.2": [older]}, {})

    assert not [entry for entry in report.collapses if entry.part == "added"]


def test_a_clean_rebuild_persists_the_cached_state_enum_mapping(tmp_path: Path) -> None:
    class Building(Offline):
        def _spec_document(self, version: str) -> str:
            return "<fix><header/><trailer/><messages/><components/><fields/></fix>"

        def _scrape_version(self, version: str) -> list[Field]:
            return [
                fix_field(
                    "ExecType",
                    150,
                    "char",
                    version=version,
                    values={
                        "0": "New",
                        "1": "Partial fill",
                        "F": "Trade",
                        "G": "Trade correct",
                        "H": "Trade cancel",
                    },
                )
            ]

    registry = Building(cache_dir=tmp_path / "fix")
    registry._rebuild_sources("9.1")
    mapping = State.fix_mapping()

    assert mapping is State.fix_mapping()
    assert mapping[150]["1"] is State.PARTIALLY_FILLED
    assert registry.state_values("ExecType") == {
        "0": State.NEW,
        "1": State.PARTIALLY_FILLED,
        "F": State.PARTIALLY_FILLED,
        "G": State.REPLACED,
        "H": State.CANCELLED,
    }
    stored = json.loads((tmp_path / "fix" / field_document(150)).read_text())
    assert stored["150"]["fix"]["states"]["G"] == "REPLACED"
    assert Offline(cache_dir=registry.cache_dir).state_values("ExecType") == (
        registry.state_values("ExecType")
    )


def test_a_collapse_reports_every_encoding_two_values_share() -> None:
    member = fix_field(
        "FakeRole", 90001, "char", version="9.1", values={"1": "Cross", "2": "cross"}
    )
    member.fix.source = "nanoconda"
    member.fix.sources = ("nanoconda",)
    _, _, report = collapse(("9.1",), {"9.1": [member]}, {})
    assert report.counts()["encoded"] == 1
    assert report.collisions[0].key == "cross" and report.collisions[0].values == ("1", "2")
    assert report.collisions[0].sources == ("nanoconda", "nanoconda")


def test_a_local_collision_keeps_empty_source_slots() -> None:
    member = fix_field(
        "FakeRole", 90001, "char", version="9.1", values={"1": "Same Name", "2": "same-name"}
    )
    _, _, report = collapse(("9.1",), {"9.1": [member]}, {})

    assert report.collisions[0].sources == ("", "")


def test_a_report_round_trips_and_says_which_counts_grew() -> None:
    older = fix_field("FakeRole", 90001, "char", version="9.0")
    newer = fix_field("FakeRole", 90001, "int", version="9.1")
    _, _, report = collapse(("9.1", "9.0"), {"9.1": [newer], "9.0": [older]}, {})
    assert ConflictReport.from_dict(report.into_dict()) == report
    assert report.exceeds({"type": 1}) == []
    assert report.exceeds({"type": 0}) == ["type: 1 conflicts against a baseline of 0"]


def test_char_and_string_are_one_fix_datatype_but_integer_is_not() -> None:
    char = fix_field("FakeRole", 90001, "char", version="9.0")
    string = fix_field("FakeRole", 90001, "String", version="9.1")
    _, _, report = collapse(("9.1", "9.0"), {"9.1": [string], "9.0": [char]}, {})
    assert not [one for one in report.collapses if one.part == "type"]

    integer = fix_field("FakeRole", 90001, "int", version="9.2")
    _, _, report = collapse(("9.2", "9.1"), {"9.2": [integer], "9.1": [string]}, {})
    assert [one.part for one in report.collapses] == ["type"]


def test_two_identities_claiming_one_name_are_refused_when_a_store_is_built() -> None:
    """One name is one identity: a store holding two answers whichever it read first.

    Two *tags* cannot reach here -- a tag is what an identity is, so a second
    reading of one folds into the record that already owns it -- but two tags
    spelled alike are two identities, and that is the collision.
    """
    with pytest.raises(ValueError, match="FIX field name 'fakerole' is claimed by"):
        collapse(
            ("9.1",),
            {
                "9.1": [
                    _field("FakeRole", 90001, "9.1"),
                    _field("FAKE_ROLE", 90002, "9.1"),
                ]
            },
            {},
        )


def test_a_write_that_would_duplicate_a_tag_is_refused_whole(store: Offline) -> None:
    """Checked against what the store would hold after, and refused before writing."""
    with pytest.raises(KeyError, match="tag 90001 is already claimed by"):
        store.add_field(_record("FakeOther", 90001))
    assert store.resolve("FakeOther") is None, "and nothing was written"


def test_check_reports_a_duplicate_tag_the_same_way_a_write_refuses_it(store: Offline) -> None:
    """One authority for both, so `check` and a write never disagree."""
    entry = _record("FakeOther", 90001)
    problems = _problems(({**store._entries[0], "spare": entry}, store._entries[1]))
    assert any("FIX tag 90001 is claimed by" in problem for problem in problems)


# -- copying a store ---------------------------------------------------------


def test_a_store_travels_as_a_directory_or_as_a_zip(store: Offline, tmp_path: Path) -> None:
    archived = FixRegistry(cache_dir=store.into_zip(tmp_path / "copy.zip"))
    assert not store.verify(archived)
    with zipfile.ZipFile(tmp_path / "copy.zip") as opened:
        names = opened.namelist()
    assert field_document(90001) in names
    assert "components/fake_parties.json" in names


def test_the_published_dictionary_answers_what_a_copy_of_it_answers(tmp_path: Path) -> None:
    """Zero regressions, checked rather than asserted: every version, every field."""
    published = FixRegistry(cache_dir=PUBLISHED)
    copied = FixRegistry(cache_dir=published.into_zip(tmp_path / "copy.zip"))
    assert published.verify(copied) == []


# -- bootstrapping the default store -----------------------------------------


class Refused(FixRegistry):
    """A registry whose bootstrap cannot read the repository archive."""

    def _registry_archive_payload(self) -> bytes:
        raise OSError("the repository host is down")


@pytest.fixture
def default_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """`CACHE_DIRECTORY`, pointed somewhere a test may write."""
    target = tmp_path / "config-fix"
    monkeypatch.setattr(registry_module, "CACHE_DIRECTORY", target)
    return target


def test_a_cold_default_store_is_fetched_once_and_says_so_both_times(
    default_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Announced before, announced after, and the next process finds a store."""
    payload = _registry_archive(_registry_documents())
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(payload),
    )
    said: list[str] = []
    with pytest.warns(RuntimeWarning, match="no FIX registry at"):
        first = FixRegistry(announce=said.append)
    assert first.installed
    assert len(said) == 2, "one line before the fetch and one after it"
    assert "downloading the main repository archive" in said[0]
    assert "is installed at" in said[1]
    assert (default_store / "fields" / "000090.json").exists(), "the sharded layout, cold"

    second = FixRegistry(announce=said.append)
    assert not second.installed, "the repository archive is installed once"
    assert len(said) == 2, "and a store that is there is served silently"
    assert second.field(90001, "9.1").name == "FakeRole"


def test_a_cold_default_store_installs_the_configured_full_archive(
    default_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    said: list[str] = []
    monkeypatch.setenv("REKEP_FIX_REGISTRY_URL", str(PUBLISHED))

    with pytest.warns(RuntimeWarning, match="downloading the main repository archive"):
        registry = FixRegistry(announce=said.append)

    assert registry.installed
    assert len(list(default_store.rglob("*.json"))) > 700
    assert registry.field("Side", "4.4").fix["tag"] == "54"
    assert "downloading the main repository archive" in said[0]
    assert "is installed" in said[-1]


def test_an_https_registry_receives_the_private_bearer_token(
    default_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _registry_archive(_registry_documents())
    seen: dict[str, object] = {}

    class Opener:
        def open(self, request: urllib.request.Request, timeout: float) -> _Response:
            seen.update(request=request, timeout=timeout)
            return _Response(payload)

    monkeypatch.setattr(urllib.request, "build_opener", lambda *_handlers: Opener())
    with pytest.warns(RuntimeWarning, match="downloading the main repository archive"):
        registry = FixRegistry(
            registry_url="https://artifactory.example/fix-registry.zip",
            registry_token="secret",
            announce=lambda _line: None,
        )

    request = seen["request"]
    assert isinstance(request, urllib.request.Request)
    assert request.get_header("Authorization") == "Bearer secret"
    assert request.get_header("User-agent") == "rekep-fix-registry"
    assert registry.installed and registry.field(90001, "9.1").name == "FakeRole"


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://artifactory.example/fix-registry.zip", "requires an HTTPS"),
        ("https://user:secret@artifactory.example/fix-registry.zip", "cannot contain credentials"),
        ("https://artifactory.example/fix-registry.zip?token=secret", "cannot contain a query"),
    ],
)
def test_registry_credentials_cannot_use_an_unsafe_url(
    tmp_path: Path,
    url: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        FixRegistry(
            cache_dir=tmp_path / "named",
            registry_url=url,
            registry_token="secret",
        )


def test_registry_archive_download_stops_at_the_compressed_limit(
    default_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry_module, "_REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES", 8)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(b"123456789", length=False),
    )
    said: list[str] = []

    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = FixRegistry(
            registry_url="https://artifactory.example/fix-registry.zip",
            announce=said.append,
        )

    assert not registry.installed
    assert any("exceeds 8 compressed bytes" in line for line in said)


def test_a_failed_registry_archive_stream_serves_the_projection(
    default_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenResponse(_Response):
        def read(self, _size: int = -1) -> bytes:
            raise RuntimeError("connection ended")

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: BrokenResponse(b"", length=False),
    )
    said: list[str] = []

    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = FixRegistry(
            registry_url="https://artifactory.example/fix-registry.zip",
            announce=said.append,
        )

    assert not registry.installed
    assert any("archive download failed: connection ended" in line for line in said)


def test_a_refused_registry_archive_serves_the_projection(
    default_store: Path,
    tmp_path: Path,
) -> None:
    broken = tmp_path / "broken.zip"
    broken.write_bytes(b"not a zip")
    said: list[str] = []

    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = FixRegistry(registry_url=str(broken), announce=said.append)

    assert not registry.installed
    assert registry.field("Side").fix.tag == 54
    assert any("could not be installed" in line for line in said)


def test_an_undecodable_registry_member_serves_the_projection(
    default_store: Path,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "undecodable.zip"
    with zipfile.ZipFile(archive, "w") as opened:
        opened.writestr("versions.json", b"\xff")
        opened.writestr("fields/000000.json", "{}")
    said: list[str] = []

    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = FixRegistry(registry_url=str(archive), announce=said.append)

    assert not registry.installed
    assert any("cannot be decoded" in line for line in said)


def test_a_registry_archive_token_is_not_serialised(tmp_path: Path) -> None:
    registry = FixRegistry(
        cache_dir=tmp_path / "named",
        registry_url="https://artifactory.example/fix-registry.zip",
        registry_token="secret-token",
    )

    dumped = registry.into_dict()
    assert "registry_token" not in dumped
    assert "secret-token" not in repr(registry)
    assert "secret-token" not in json.dumps(dumped)


def test_an_explicit_empty_registry_token_suppresses_the_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REKEP_FIX_REGISTRY_TOKEN", "another-registry-token")
    registry = FixRegistry(
        cache_dir=tmp_path / "named",
        registry_url="http://artifactory.example/fix-registry.zip",
        registry_token="",
    )

    assert registry.__dict__["_registry_token"] == ""


@pytest.mark.parametrize("malformed", ["index", "field", "component"])
def test_malformed_registry_documents_never_leave_the_staging_directory(
    tmp_path: Path,
    malformed: str,
) -> None:
    documents = _registry_documents()
    if malformed == "index":
        documents["versions.json"]["sessions"] = {"9.1": [["FakeRole", "yes"]]}
    elif malformed == "field":
        documents[field_document(90001)]["90001"]["fix"]["versions"] = "9.1"
    else:
        documents["components/fake_parties.json"]["declaration"] = "not-a-document"
    target = tmp_path / malformed
    registry = FixRegistry(cache_dir=target)

    with pytest.raises(ValueError, match="FIX"):
        registry._install_registry_documents(documents)

    assert not target.exists()


def test_a_complete_concurrent_registry_wins_without_a_scrape(
    default_store: Path,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "incoming.zip"
    archive.write_bytes(_registry_archive(_registry_documents()))
    winner = _registry_documents()

    class Racing(FixRegistry):
        def _install_registry_documents(self, documents: Mapping[str, Mapping[str, Any]]) -> int:
            place = registry_module.DirectoryDocuments(
                pyarrow.fs.LocalFileSystem(), Path(self.cache_dir).as_posix()
            )
            for name, document in winner.items():
                place.write(name, document)
            return super()._install_registry_documents(documents)

    with pytest.warns(RuntimeWarning, match="downloading the main repository archive"):
        registry = Racing(registry_url=str(archive), announce=lambda _line: None)

    assert registry.installed
    assert registry.field(90001, "9.1").name == "FakeRole"


def test_an_interrupted_registry_install_leaves_no_partial_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "fix"
    registry = FixRegistry(cache_dir=target)
    documents = {
        "versions.json": {"versions": ["9.1"]},
        "fields/000000.json": {},
    }
    write = registry_module.DirectoryDocuments.write
    written = 0

    def interrupt(place: object, name: str, document: dict[str, object]) -> None:
        nonlocal written
        written += 1
        if written == 2:
            raise OSError("interrupted")
        write(place, name, document)

    monkeypatch.setattr(registry_module.DirectoryDocuments, "write", interrupt)
    with pytest.raises(OSError, match="interrupted"):
        registry._install_registry_documents(documents)

    assert not target.exists()


def test_a_whole_store_rebuild_drops_the_documents_it_no_longer_declares(
    tmp_path: Path,
) -> None:
    """A rebuild is a replacement: a shard that emptied is gone, not stale."""
    place = registry_module.DirectoryDocuments(
        pyarrow.fs.LocalFileSystem(), (tmp_path / "fix").as_posix()
    )
    place.write("fields/000000.json", {"old": True})
    place.write("fields/000001.json", {"stale": True})

    place.write_all({"fields/000000.json": {"new": True}})

    assert place.names() == ("fields/000000.json",)
    assert place.read("fields/000000.json") == {"new": True}


def test_a_registry_archive_cannot_write_outside_the_default_store(tmp_path: Path) -> None:
    archive = tmp_path / "hostile.zip"
    with zipfile.ZipFile(archive, "w") as opened:
        opened.writestr("versions.json", '{"versions": ["9.1"]}')
        opened.writestr("fields/000000.json", "{}")
        opened.writestr("../outside.json", "{}")

    with pytest.raises(ValueError, match="unsafe FIX registry archive member"):
        FixRegistry._registry_archive_documents(archive.read_bytes())

    assert not (tmp_path / "outside.json").exists()


def test_a_bootstrap_the_network_refuses_serves_the_projection_and_says_it_is_reduced(
    default_store: Path,
) -> None:
    said: list[str] = []
    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = Refused(announce=said.append)
    assert not default_store.exists(), "nothing was installed over a failed fetch"
    assert "misses the long tail" in said[-1]
    assert "rekep fix registry scrape" in said[-1]
    assert registry.field("Side").fix["tag"] == "54", "and the projection answers"


def test_a_store_somebody_named_opens_no_socket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Naming a store is the whole of saying where the answers come from.

    Asserted at the socket, not inferred from a counter: a registry pointed at
    a store serves it, cold or warm, and nothing it is asked turns into a
    fourteen-thousand-page scrape. This is the property `offline=True` used to
    buy, and every caller had to remember to buy it.
    """

    def refuse(*_: object, **__: object) -> None:
        raise AssertionError("a registry serving a store opened a socket")

    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    registry = FixRegistry(cache_dir=tmp_path / "named")

    assert registry.offline, "it was pointed at a store"
    assert registry.versions == () and registry._stored_versions() == ()
    assert registry.lookup("Side") == [], "no packaged projection stood in for it"
    assert not registry.fields_available("4.4")


def test_a_cold_default_store_is_the_one_that_may_fill_itself(default_store: Path) -> None:
    """Nobody chose `~/.config/fix`, so it is the only store that fetches.

    Refused here, which is the same path a machine with no route out takes:
    it serves the packaged projection and says the registry is reduced.
    """
    said: list[str] = []
    with pytest.warns(RuntimeWarning, match="a reduced one is served"):
        registry = Refused(announce=said.append)
    assert not registry.offline, "nothing was found where it looked"
    assert "rekep fix registry scrape" in said[-1]
    assert registry.field("Side").fix["tag"] == "54", "served reduced, and never silently"


def test_the_packaged_projection_is_bootstrapped_by_being_what_it_is() -> None:
    """`from_builtin` names its store, so it is served rather than announced."""
    builtin = FixRegistry.from_builtin()
    assert builtin.offline and builtin.field("Side").fix["tag"] == "54"


# -- explicit scraping ------------------------------------------------------


def test_registry_reads_never_refresh_configured_sources(store: Offline) -> None:
    class NoRead(FixRegistry):
        def _read(self, _request: urllib.request.Request) -> str:
            raise AssertionError("a registry read reached a source")

    registry = NoRead(cache_dir=store.cache_dir, sources=(QuickFixSource(),))
    assert registry.fields("9.1")
    assert registry.components("9.1")
    assert registry.field(90001, "9.1").name == "FakeRole"


# -- a field FIX never numbered, end to end ----------------------------------


def test_a_declared_vendor_field_is_lifted_into_a_log_column(
    store: Offline, tmp_path: Path
) -> None:
    """Declaration, storage, lookup, and a parsed column -- with no code change.

    The gap this closes: a rendered name with no FIX tag had exactly one home,
    a hand-written Python literal, and adding a second meant editing the
    package. Here a synthetic one is declared into the store, projected into a
    registry a codec reads, and lifted out of a bridge line into the column its
    record names.
    """
    store.add_field(
        _record(
            "FAKE.VENDOR.CODE",
            named_aliases=[Alias(name="FAKEVENDORCODE", source="brk", occurrences=5)],
            description="A vendor's own code.",
            column="fakevendorcode",
        )
    )

    # Declared, so the registry answers for it by every spelling it has.
    assert store.resolve("FAKE.VENDOR.CODE").fix.column == "fakevendorcode"
    assert store.resolve("fakevendorcode").fix.canonical == "FAKE.VENDOR.CODE"
    assert record_kind(store.field("FAKE.VENDOR.CODE", "9.1")) == NAMESPACE
    assert "FAKE.VENDOR.CODE" not in store.tags(), "it has no tag to be mapped to"

    # Stored, in the one document the fields with no tag share, as the same
    # field document every other declaration in this repository is written in:
    # the Arrow reading at the top, the protocol's own keys under `fix`.
    stored = json.loads((Path(store.cache_dir) / field_document("FAKE.VENDOR.CODE")).read_text())
    assert stored == {
        "FAKE.VENDOR.CODE": {
            "name": "FAKE.VENDOR.CODE",
            "type": "string",
            "nullable": True,
            "description": "A vendor's own code.",
            "fix": {
                "type": "String",
                "versions": ["*"],
                "column": "fakevendorcode",
                "aliases": [{"name": "FAKEVENDORCODE", "source": "brk", "occurrences": 5}],
            },
        }
    }
    assert "kind" not in stored["FAKE.VENDOR.CODE"]["fix"], (
        "having no tag is what makes it namespaced; storing the answer beside "
        "the question is a second place for the two to disagree"
    )

    # Projected, whole rather than per version, into a registry of its own.
    projected = FixRegistry(
        cache_dir=store.into_projection(
            tmp_path / "projected.zip", ["FakeRole", "FAKE.VENDOR.CODE"]
        ),
    )
    entry = projected.resolve("FAKE.VENDOR.CODE")
    assert entry.fix.versions == (ANY_VERSION,), "it holds for every version, not for 9.1"
    assert entry.fix.column == "fakevendorcode"

    merged = projected.merged_fields()["FAKE.VENDOR.CODE"]
    assert merged.fix["column"] == "fakevendorcode"
    assert json.loads(merged.fix["aliases"])[0]["name"] == "FAKEVENDORCODE"

    # And lifted, by a codec reading that registry, out of a rendered line --
    # under both spellings, because a bridge writes whichever it feels like.
    codec = FixCodec(registry=projected)
    assert set(codec.named_fields()) == {"fakevendorcode"}
    line = "toBridge " + "|".join(
        ["#FAKEVENDORCODE=FAKE-CODE-0001", "#FAKEROLE=1", "#UNRESOLVED=x"]
    )
    columns, rest = codec.into_lifted_columns(
        codec.into_entries(codec.into_pairs(pyarrow.array([line]), "FIXML"), "9.1"), "9.1"
    )
    assert columns["fakevendorcode"].to_pylist() == ["FAKE-CODE-0001"]
    assert [key for key, _ in _pairs(rest)] == ["FAKEROLE", "UNRESOLVED"], "and nothing else moved"

    dotted = codec.into_entries(
        codec.into_pairs(
            pyarrow.array(["toBridge #FAKE.VENDOR.CODE=FAKE-CODE-0002|#X=1"]), "FIXML"
        ),
        "9.1",
    )
    assert codec.into_lifted_columns(dotted, "9.1")[0]["fakevendorcode"].to_pylist() == [
        "FAKE-CODE-0002"
    ]


def test_namespaced_aliases_are_ordered_fallbacks(store: Offline) -> None:
    store.add_field(
        _record(
            "FAKE.VENDOR.CODE",
            named_aliases=[Alias(name="LEGACYCODE"), Alias(name="OLDER.CODE")],
            column="fakevendorcode",
        )
    )
    codec = FixCodec(registry=store)
    declared = codec.named_fields()

    assert [alias["name"] for alias in declared["fakevendorcode"].fix.aliases] == [
        "LEGACYCODE",
        "OLDER.CODE",
    ]
    pairs = codec.into_pairs(
        pyarrow.array(
            [
                "toBridge #FAKE.VENDOR.CODE=canonical|#LEGACYCODE=legacy|#Y=1",
                "toBridge #LEGACYCODE=legacy|#Y=1",
                "toBridge #LEGACYCODE=legacy|#OLDER.CODE=older|#Y=1",
                "toBridge #OLDER.CODE=older|#Y=1",
            ]
        ),
        "FIXML",
    )
    entries = codec.into_entries(pairs, "9.1")
    columns, residual = codec.into_lifted_columns(entries, "9.1")

    assert columns["fakevendorcode"].to_pylist() == [
        "canonical",
        "legacy",
        "legacy",
        "older",
    ]
    assert [_pairs(residual, row) for row in range(4)] == [
        [("LEGACYCODE", "legacy"), ("Y", "1")],
        [("Y", "1")],
        [("OLDER.CODE", "older"), ("Y", "1")],
        [("Y", "1")],
    ]


def test_a_codec_over_a_dictionary_that_declares_none_lifts_none(store: Offline) -> None:
    """The column exists in the parsed shape either way; only the value is absent."""
    codec = FixCodec(registry=store)
    assert set(codec.named_fields()) == set()
    entries = codec.into_entries(
        codec.into_pairs(pyarrow.array(["toBridge #FAKEVENDORCODE=x|#Y=1"]), "FIXML"), "9.1"
    )
    columns, rest = codec.into_lifted_columns(entries, "9.1")
    assert not any(column.to_pylist()[0] is not None for column in columns.values())
    assert [key for key, _ in _pairs(rest)] == ["FAKEVENDORCODE", "Y"]


def test_two_vendor_namespaces_of_one_name_stay_two_fields(store: Offline) -> None:
    """A namespace is part of the name, and matching on the tail would merge them.

    Two vendors each render a `CLIENTID`. Reducing a rendered key to its last
    dotted segment -- which is what the parser does for a component path --
    would make one of them answer for the other's values.
    """
    for vendor, column in (("FAKEA", "fakeaclient"), ("FAKEB", "fakebclient")):
        store.add_field(_record(f"{vendor}.CLIENTID", column=column))
    codec = FixCodec(registry=store)
    assert set(codec.named_fields()) == {"fakeaclientid", "fakebclientid"}
    assert "clientid" not in codec.named_fields(), "two fields claim it, so it is a guess"

    line = (
        "toBridge #FAKEA.CLIENTID=ACCT-TEST-01|#FAKEB.CLIENTID=ACCT-TEST-02|#CLIENTID=ACCT-TEST-03"
    )
    columns, rest = codec.into_lifted_columns(
        codec.into_entries(codec.into_pairs(pyarrow.array([line]), "FIXML"), "9.1"), "9.1"
    )
    assert columns["fakeaclient"].to_pylist() == ["ACCT-TEST-01"]
    assert columns["fakebclient"].to_pylist() == ["ACCT-TEST-02"]
    assert [key for key, _ in _pairs(rest)] == ["CLIENTID"], "the bare one is nobody's"


# -- what a component declaration says a member must carry -------------------


def test_a_component_projects_with_the_nullability_its_spec_declares(store: Offline) -> None:
    """`required` is the whole rule: a member a message must carry is NOT NULL."""
    field = store.component_field("FakeParties", "9.1")
    (group,) = field.fields
    assert group.name == "nofakeparties"
    assert group.nullable, "the group itself is optional in this declaration"
    member = group.item.field("fakerole")
    assert not member.nullable, "and its one member is required"
    assert member.dtype == pyarrow.int32(), "typed from the dictionary, not guessed"


def test_a_component_a_version_does_not_declare_projects_nothing(store: Offline) -> None:
    assert store.component_field("FakeParties", "9.0") is None
    assert store.component_scalar("FakeParties", "9.0") is None


def test_a_component_materialises_as_a_class_the_declaration_wrote(store: Offline) -> None:
    """No hand-written row class: the declaration already says every member."""
    built = store.component_scalar("FakeParties", "9.1")
    entry = built.FakeParty

    assert built.__name__ == "FakeParties"
    assert entry.__name__ == "FakeParty", "`NoFakeParties` repeats one `FakeParty`"
    assert built(nofakeparties=[entry(fakerole=7)]).into_dict() == {
        "nofakeparties": [{"fakerole": 7}]
    }
    assert built.into_field().dtype == store.component_field("FakeParties", "9.1").dtype
