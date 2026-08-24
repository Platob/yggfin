"""A whole store: which layout it is, editing it, keeping it fresh, migrating it.

`test_entries.py` holds one identity to its schema and `test_data.py` reads the
published dictionary. These are about the store around them -- how a directory
or a zip says which layout it is in, what a change to one entry is allowed to
do, what a name resolves to and in what order, and what a TTL does and does not
refetch.

Every identity is synthetic. The one real name any of this uses is a FIX
version, which is a schema fact and not data.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
import zipfile
from pathlib import Path

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix.entries import ANY_VERSION, NAMESPACE, Alias, ComponentEntry, FieldEntry
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import SpecComponent, SpecFieldRef, SpecGroup
from rekep.fix.registry import FixRegistry, _problems
from rekep.fix.store import (
    EXPLODED,
    VERSIONED,
    ExplodedLayout,
    VersionedLayout,
    explode,
    layout_of,
)
from rekep.fix.transcribe import FixCodec

#: The published dictionary, for the migration that has to lose nothing.
PUBLISHED = Path(__file__).resolve().parents[3] / "data" / "fix.zip"


class Offline(FixRegistry):
    """A registry that must answer from the store alone."""

    def _fetch(self, url: str) -> str:
        raise OSError(f"offline: {url}")


def _pairs(array: pyarrow.Array, row: int = 0) -> list[tuple[object, str]]:
    """One `kwargs` or map cell in the pair form the assertions read."""
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
    registry = Offline(cache_dir=tmp_path / "fix", offline=True)
    registry._store_versions(("9.1", "9.0"))
    registry._store_fields(
        "9.1",
        [_field("FakeRole", 90001, "9.1", "int"), _field("FakeCode", 90002, "9.1")],
        session=(("FakeRole", True),),
        components=[
            SpecComponent(
                "FakeParties",
                (
                    SpecGroup(
                        "NoFakeParties", False, 90003, (SpecFieldRef("FakeRole", True, 90001),)
                    ),
                ),
            )
        ],
    )
    registry._store_fields(
        "9.0",
        [_field("FakeRoleCode", 90001, "9.0", "char"), _field("FakeCode", 90002, "9.0")],
        components=[],
    )
    return registry


# -- which layout a store is in ----------------------------------------------


def test_a_cold_store_is_written_one_file_per_identity(store: Offline) -> None:
    folder = Path(store.cache_dir)
    assert sorted(path.name for path in folder.iterdir()) == [
        "components",
        "fields",
        "versions.json",
    ]
    assert sorted(path.name for path in (folder / "fields").iterdir()) == [
        "fake_code.json",
        "fake_role.json",
    ]


def test_a_layout_is_read_off_what_a_store_holds_never_off_a_setting(
    store: Offline, tmp_path: Path
) -> None:
    """A copied-in dictionary brings no setting with it, so it has to say."""
    assert isinstance(store._layout, ExplodedLayout)
    versioned = FixRegistry(cache_dir=tmp_path / "old", offline=True, layout=VERSIONED)
    versioned._store_fields("9.1", [_field("FakeRole", 90001, "9.1")])
    assert (Path(versioned.cache_dir) / "9.1.json").exists()
    assert isinstance(FixRegistry(cache_dir=versioned.cache_dir)._layout, VersionedLayout)
    assert isinstance(FixRegistry(cache_dir=store.cache_dir)._layout, ExplodedLayout)


def test_a_store_that_holds_nothing_is_written_in_the_layout_it_was_asked_for(
    tmp_path: Path,
) -> None:
    documents = FixRegistry(cache_dir=tmp_path / "cold", offline=True)._documents
    assert isinstance(layout_of(documents, EXPLODED), ExplodedLayout)
    assert isinstance(layout_of(documents, VERSIONED), VersionedLayout)
    with pytest.raises(ValueError, match="unknown FIX registry layout"):
        layout_of(documents, "invented")
    with pytest.raises(ValueError, match="unknown FIX registry layout"):
        FixRegistry(cache_dir=tmp_path / "cold", layout="invented")


def test_a_versioned_store_still_answers_everything_an_exploded_one_does(
    store: Offline, tmp_path: Path
) -> None:
    """An old `~/.config/fix` keeps working; it is not rewritten by being read."""
    versioned = FixRegistry(cache_dir=tmp_path / "old", offline=True, layout=VERSIONED)
    versioned._store_versions(store.versions)
    for version in store.versions:
        versioned._store_fields(
            version, store.fields(version), store.session(version), store.components(version)
        )
    assert not versioned.verify(store)
    assert versioned.resolve("FakeRoleCode").name == "FakeRole", "and merges the same"


# -- what a rename does ------------------------------------------------------


def test_a_renamed_tag_is_one_identity_and_not_two(store: Offline) -> None:
    """The whole point of storing by identity: one file, saying what each version called it."""
    entry = store.resolve("FakeRole")
    assert entry.tag == 90001
    assert entry.names() == {"9.1": "FakeRole", "9.0": "FakeRoleCode"}
    assert not (Path(store.cache_dir) / "fields" / "fake_role_code.json").exists()
    stored = json.loads((Path(store.cache_dir) / "fields" / "fake_role.json").read_text())
    assert stored["versions"]["9.0"]["name"] == "FakeRoleCode"
    assert "name" not in stored["versions"]["9.1"], "the newest version is the identity"


def test_each_version_still_reads_back_its_own_name(store: Offline) -> None:
    assert store.field(90001, "9.1").name == "FakeRole"
    assert store.field(90001, "9.0").name == "FakeRoleCode"
    assert store.lookup("FakeRoleCode", "9.1") == [], "9.1 does not spell it that way"
    assert store.lookup("FakeRoleCode", "9.0")[0].name == "FakeRoleCode"


def test_storing_a_version_says_what_that_version_has(store: Offline) -> None:
    """A field a rewritten version no longer names has lost that version."""
    store._store_fields("9.0", [_field("FakeCode", 90002, "9.0")])
    assert [member.name for member in store.fields("9.0")] == ["FakeCode"]
    assert store.resolve("FakeRole").versions == ("9.1",)
    store._store_fields("9.1", [_field("FakeCode", 90002, "9.1")])
    assert store.resolve("FakeRole") is None, "its last version went, and so did the file"
    assert not (Path(store.cache_dir) / "fields" / "fake_role.json").exists()


# -- resolving a name --------------------------------------------------------


def test_a_name_resolves_canonical_then_per_version_then_alias(store: Offline) -> None:
    """The three tiers, in the order a rendered key is tried against them."""
    store.alias_field("FakeRole", Alias(name="FakeRolle", source="brk", occurrences=9))
    assert store.resolve("FakeRole").tag == 90001, "tier one: an identity's own name"
    assert store.resolve("FakeCode").tag == 90002
    assert store.resolve("FakeRoleCode").tag == 90001, "tier two: what 9.0 calls that tag"
    assert store.resolve("FakeRolle").tag == 90001, "tier three: a spelling somebody recorded"
    assert store.resolve("FAKEROLLE").tag == 90001, "and matching folds case, and only case"
    assert store.resolve("fake_rolle") is None, "a separator is part of a name, not noise"
    assert store.resolve("FakeNothing") is None, "and a name nothing here has is unknown"
    assert store.alias_conflicts() == {}


def test_an_alias_an_earlier_tier_already_answers_for_is_refused(store: Offline) -> None:
    """Recording a spelling nothing will ever reach is a mistake, not precedence."""
    with pytest.raises(ValueError, match="already FakeRole's"):
        store.alias_field("FakeCode", Alias(name="FakeRoleCode"))
    assert store.resolve("FakeRoleCode").tag == 90001, "unchanged, because it was refused"


def test_two_fields_claiming_one_name_in_one_tier_fails_the_check(store: Offline) -> None:
    """Nothing decides between them, so the store says so rather than picking."""
    store.alias_field("FakeRole", Alias(name="FakeSpelling"))
    with pytest.raises(ValueError, match="'fakespelling' is claimed by"):
        store.alias_field("FakeCode", Alias(name="FakeSpelling"))
    assert store.check() == [], "and the refused change was not written"

    # Written past the API, so the check has something to find.
    entry = store.resolve("FakeCode")
    store._editable.store_field(
        FieldEntry(
            name=entry.name,
            tag=entry.tag,
            aliases=(Alias(name="FakeSpelling"),),
            variants=dict(entry.variants),
        )
    )
    store._forget()
    assert store.check() == ["'fakespelling' is claimed by ['FakeCode', 'FakeRole']"]


def test_an_alias_is_data_and_carries_where_it_came_from(store: Offline) -> None:
    """A near miss counted in a capture is evidence; a name typed in is not."""
    entry = store.alias_field("FakeRole", Alias(name="FakeRolle", source="brk", occurrences=41))
    assert store.resolve("FakeRolle").tag == 90001
    stored = json.loads((Path(store.cache_dir) / "fields" / "fake_role.json").read_text())
    assert stored["aliases"] == [{"name": "FakeRolle", "source": "brk", "occurrences": 41}]
    assert entry.aliases[0].occurrences == 41

    again = store.alias_field("FakeRole", "FakeRolle")
    assert len(again.aliases) == 1, "a spelling already recorded is not recorded twice"


def test_aliasing_a_field_nothing_resolves_is_refused(store: Offline) -> None:
    with pytest.raises(KeyError, match="FakeAbsent"):
        store.alias_field("FakeAbsent", "FakeSomething")


# -- editing the store -------------------------------------------------------


def test_a_field_identity_is_created_updated_and_removed(store: Offline) -> None:
    entry = FieldEntry(
        name="FAKE.VENDOR.CODE",
        kind=NAMESPACE,
        variants={ANY_VERSION: {"type": "String", "description": "A vendor's own."}},
        column="fake_vendor_code",
    )
    store.add_field(entry)
    assert (Path(store.cache_dir) / "fields" / "fake_vendor_code.json").exists()
    assert store.resolve("FAKE.VENDOR.CODE").column == "fake_vendor_code"
    assert store.field("FAKE.VENDOR.CODE", "9.1").fix["kind"] == NAMESPACE

    store.update_field(dataclasses_replace(entry, column="renamed"))
    assert store.resolve("FAKE.VENDOR.CODE").column == "renamed"

    assert store.remove_field("FAKE.VENDOR.CODE")
    assert store.resolve("FAKE.VENDOR.CODE") is None
    assert not store.remove_field("FAKE.VENDOR.CODE"), "and says so the second time"


def test_creating_one_that_is_already_there_and_updating_one_that_is_not_are_refused(
    store: Offline,
) -> None:
    entry = FieldEntry(name="FakeRole", tag=90001, variants={"9.1": {"type": "int"}})
    with pytest.raises(KeyError, match="already stored"):
        store.add_field(entry)
    with pytest.raises(KeyError, match="no FIX field stored"):
        store.update_field(FieldEntry(name="FakeAbsent", tag=90099, variants={"9.1": {}}))


def test_a_change_is_validated_against_the_whole_store_before_it_is_written(
    store: Offline,
) -> None:
    """Beside what is already there, because an alias alone is never a collision."""
    clashing = FieldEntry(
        name="FakeOther",
        tag=90004,
        aliases=(Alias(name="FakeCode"),),
        variants={"9.1": {"type": "String"}},
    )
    # `FakeCode` is another field's canonical name, so this alias could never
    # resolve. Precedence is the rule; recording a spelling nothing will ever
    # reach is the mistake, and it is refused rather than written and ignored.
    with pytest.raises(ValueError, match="already FakeCode's"):
        store.add_field(clashing)
    assert store.resolve("FakeOther") is None, "refused whole, never written half"
    assert not (Path(store.cache_dir) / "fields" / "fake_other.json").exists()


def test_a_component_identity_is_created_updated_and_removed(store: Offline) -> None:
    entry = ComponentEntry.from_components(
        [SpecComponent("FakeInstrument", (SpecFieldRef("FakeCode", False, 90002),))], ["9.1"]
    )
    store.add_component(entry)
    assert store.component("FakeInstrument", "9.1").members[0].tag == 90002
    with pytest.raises(KeyError, match="already stored"):
        store.add_component(entry)

    store.update_component(
        ComponentEntry.from_components(
            [SpecComponent("FakeInstrument", (SpecFieldRef("FakeRole", False, 90001),))], ["9.1"]
        )
    )
    assert store.component("FakeInstrument", "9.1").members[0].tag == 90001

    assert store.remove_component("FakeInstrument")
    assert not store.remove_component("FakeInstrument")
    with pytest.raises(KeyError, match="FakeInstrument"):
        store.component("FakeInstrument", "9.1")


def test_a_store_kept_one_file_per_version_cannot_be_edited_one_identity_at_a_time(
    tmp_path: Path,
) -> None:
    """It has no file to write, so it says to migrate rather than inventing one."""
    versioned = FixRegistry(cache_dir=tmp_path / "old", offline=True, layout=VERSIONED)
    versioned._store_fields("9.1", [_field("FakeRole", 90001, "9.1")])
    with pytest.raises(TypeError, match="migrate it first"):
        versioned.alias_field("FakeRole", "FakeRolle")


# -- the merged views --------------------------------------------------------


def test_the_whole_unified_table_comes_back_in_one_call(store: Offline) -> None:
    """What a classification run pulls, where `scalar()` answered one key at a time."""
    merged = store.merged_fields()
    assert sorted(merged) == ["FakeCode", "FakeRole"]
    scalar = store.scalar("FakeRole")
    assert merged["FakeRole"].name == scalar.name
    assert merged["FakeRole"].arrow_type == scalar.arrow_type
    assert merged["FakeRole"].metadata == scalar.metadata, "the same declaration, in bulk"
    assert json.loads(merged["FakeRole"].fix["names"]) == {
        "9.1": "FakeRole",
        "9.0": "FakeRoleCode",
    }


def test_a_merged_field_carries_the_spellings_it_answers_to(store: Offline) -> None:
    store.alias_field("FakeRole", Alias(name="FakeRolle", source="pco", occurrences=7))
    merged = store.merged_fields()["FakeRole"]
    assert json.loads(merged.fix["aliases"]) == [
        {"name": "FakeRolle", "source": "pco", "occurrences": 7}
    ]


def test_a_component_is_one_queryable_object_across_every_version(store: Offline) -> None:
    """Not "pick the newest match and hope", which is what `component()` does."""
    entry = store.merged_component("fakeparties")
    assert entry.name == "FakeParties" and entry.versions == ("9.1",)
    assert entry.delimiters("9.1") == {("NoFakeParties",): "FakeRole"}
    assert store.merged_components()["FakeParties"] is entry
    with pytest.raises(KeyError, match="FakeAbsent"):
        store.merged_component("FakeAbsent")


# -- migrating ---------------------------------------------------------------


def test_migrating_the_published_dictionary_changes_no_answer(tmp_path: Path) -> None:
    """Zero regressions, checked rather than asserted: every version, every field."""
    published = FixRegistry(cache_dir=PUBLISHED, offline=True)
    migrated = published.migrate(tmp_path / "migrated")
    assert published.verify(migrated) == []
    assert (tmp_path / "migrated" / "fields").is_dir()
    assert (tmp_path / "migrated" / "components" / "parties.json").exists()


def test_a_migration_that_changed_an_answer_is_refused(store: Offline, tmp_path: Path) -> None:
    """The check is the migration, not a thing run after it."""

    class Lossy(Offline):
        def verify(self, other: FixRegistry) -> list[str]:
            return ["a difference this test invented"]

    lossy = Lossy(cache_dir=store.cache_dir, offline=True)
    with pytest.raises(ValueError, match="changed what it answers"):
        lossy.migrate(tmp_path / "migrated")


def test_a_migrated_store_travels_as_a_directory_or_as_a_zip(
    store: Offline, tmp_path: Path
) -> None:
    archived = store.migrate(tmp_path / "migrated.zip")
    assert not store.verify(archived)
    with zipfile.ZipFile(tmp_path / "migrated.zip") as opened:
        names = opened.namelist()
    assert "fields/fake_role.json" in names
    assert "components/fake_parties.json" in names


# -- keeping it fresh --------------------------------------------------------


class Refetching(FixRegistry):
    """A registry whose spec fetches are counted and served from a fixture."""

    fetched: list[str]

    def _read(self, request: urllib.request.Request) -> str:
        self.__dict__.setdefault("fetched", []).append(request.full_url)
        return _SPEC


class Refusing(Refetching):
    """One whose upstream is down, which must not stop it serving."""

    def _read(self, request: urllib.request.Request) -> str:
        self.__dict__.setdefault("fetched", []).append(request.full_url)
        raise OSError("the spec host is down")


#: A spec naming the two tags the fixture store holds, one of them enumerated.
_SPEC = """<fix major='9' minor='1'>
 <header/><trailer/>
 <components>
  <component name='FakeParties'>
   <group name='NoFakeParties'><field name='FakeRole' required='N'/></group>
  </component>
 </components>
 <fields>
  <field number='90001' name='FakeRole' type='INT'>
   <value enum='1' description='FAKE_ONE'/>
  </field>
  <field number='90002' name='FakeCode' type='STRING'/>
  <field number='90003' name='NoFakeParties' type='NUMINGROUP'/>
 </fields>
</fix>
"""


def test_a_ttl_of_zero_never_reaches_upstream(store: Offline, tmp_path: Path) -> None:
    """The default, and what every pipeline reading a packaged dictionary wants."""
    registry = Refetching(cache_dir=store.cache_dir, cache_ttl=0.0)
    assert registry.fields("9.1")
    assert registry.refresh_if_stale() is False
    assert not registry.__dict__.get("fetched"), "no fetch was attempted at all"


def test_a_store_younger_than_its_ttl_is_served_untouched(store: Offline) -> None:
    registry = Refetching(cache_dir=store.cache_dir, cache_ttl=3600.0)
    assert "value_names" not in registry.field(90001, "9.1").fix
    assert registry.refresh_if_stale() is False
    assert not registry.__dict__.get("fetched")


def _aged(store: Offline, seconds: float) -> None:
    """Backdate every document, so a TTL test does not race the clock."""
    when = time.time() - seconds
    for path in Path(store.cache_dir).rglob("*.json"):
        os.utime(path, (when, when))


def test_a_store_older_than_its_ttl_is_refetched_and_written(store: Offline) -> None:
    _aged(store, 7200)
    registry = Refetching(cache_dir=store.cache_dir, cache_ttl=3600.0, retries=0)
    assert registry.refresh_if_stale() is True
    assert sorted(url.rsplit("/", 1)[-1] for url in registry.fetched) == [
        "FIX90.xml",
        "FIX91.xml",
    ], "every version the store holds, so none of it goes stale behind the others"
    reopened = Offline(cache_dir=store.cache_dir, offline=True)
    assert json.loads(reopened.field(90001, "9.1").fix["value_names"]) == {"1": "FAKE_ONE"}
    assert reopened.component("FakeParties", "9.1").members[0].name == "NoFakeParties"


def test_a_refetch_that_fails_serves_the_local_copy_and_says_so(store: Offline) -> None:
    """A dictionary a day stale parses every message; one that raises parses none."""
    _aged(store, 7200)
    registry = Refusing(cache_dir=store.cache_dir, cache_ttl=3600.0, retries=0, backoff=0.0)
    with pytest.warns(RuntimeWarning, match="could not be refreshed"):
        assert registry.refresh_if_stale() is False
    assert registry.fetched, "it was attempted"
    assert registry.field(90001, "9.1").name == "FakeRole", "and the store still answers"


def test_a_ttl_is_checked_once_per_registry_and_not_once_per_call(store: Offline) -> None:
    """Otherwise a batch of a hundred versions is a hundred refetches."""
    _aged(store, 7200)
    registry = Refetching(cache_dir=store.cache_dir, cache_ttl=3600.0, retries=0)
    registry.fields("9.1")
    fetched = len(registry.fetched)
    registry.fields("9.0")
    registry.fields("9.1")
    assert len(registry.fetched) == fetched


def test_a_negative_ttl_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        FixRegistry(cache_dir=tmp_path / "fix", cache_ttl=-1.0)


def dataclasses_replace(entry: FieldEntry, **changed: object) -> FieldEntry:
    """`dataclasses.replace`, named so the test reads as what it is doing."""
    import dataclasses

    return dataclasses.replace(entry, **changed)


# -- a field FIX never numbered, end to end ----------------------------------


def test_a_declared_vendor_field_is_lifted_into_a_log_column(
    store: Offline, tmp_path: Path
) -> None:
    """Declaration, storage, lookup, and a parsed column -- with no code change.

    The gap this closes: a rendered name with no FIX tag had exactly one home,
    a hand-written Python literal, and adding a second meant editing the
    package. Here a synthetic one is declared into the store, projected into a
    registry a codec reads, and lifted out of a bridge line into the column its
    entry names.
    """
    store.add_field(
        FieldEntry(
            name="FAKE.VENDOR.CODE",
            kind=NAMESPACE,
            aliases=(Alias(name="FAKEVENDORCODE", source="brk", occurrences=5),),
            variants={ANY_VERSION: {"type": "String", "description": "A vendor's own code."}},
            column="fake_vendor_code",
        )
    )

    # Declared, so the registry answers for it by every spelling it has.
    assert store.resolve("FAKE.VENDOR.CODE").column == "fake_vendor_code"
    assert store.resolve("fakevendorcode").name == "FAKE.VENDOR.CODE"
    assert store.field("FAKE.VENDOR.CODE", "9.1").fix["kind"] == NAMESPACE
    assert "FAKE.VENDOR.CODE" not in store.tags(), "it has no tag to be mapped to"

    # Stored, as one reviewable file that says what it is.
    stored = json.loads((Path(store.cache_dir) / "fields" / "fake_vendor_code.json").read_text())
    assert stored == {
        "name": "FAKE.VENDOR.CODE",
        "kind": "namespace",
        "column": "fake_vendor_code",
        "aliases": [{"name": "FAKEVENDORCODE", "source": "brk", "occurrences": 5}],
        "versions": {"*": {"type": "String", "description": "A vendor's own code."}},
    }

    # Projected, whole rather than per version, into a registry of its own.
    projected = FixRegistry(
        cache_dir=store.into_projection(
            tmp_path / "projected.zip", ["FakeRole", "FAKE.VENDOR.CODE"]
        ),
        offline=True,
    )
    entry = projected.resolve("FAKE.VENDOR.CODE")
    assert entry.versions == (ANY_VERSION,), "it holds for every version, not for 9.1"
    assert entry.column == "fake_vendor_code"

    merged = projected.merged_fields()["FAKE.VENDOR.CODE"]
    assert merged.fix["column"] == "fake_vendor_code"
    assert json.loads(merged.fix["aliases"])[0]["name"] == "FAKEVENDORCODE"

    # And lifted, by a codec reading that registry, out of a rendered line --
    # under both spellings, because a bridge writes whichever it feels like.
    codec = FixCodec(registry=projected)
    assert set(codec.named_fields()) == {
        "fake.vendor.code",
        "fakevendorcode",
        # The last dotted segment as well, because one estate renders the same
        # field both with its vendor namespace and without -- and only because
        # exactly one field here claims it.
        "code",
    }
    line = "toBridge " + "|".join(
        ["#FAKEVENDORCODE=FAKE-CODE-0001", "#FAKEROLE=1", "#UNRESOLVED=x"]
    )
    columns, rest = codec.into_lifted_columns(
        codec.into_kwargs(codec.into_pairs(pyarrow.array([line]), "UL"), "9.1"), "9.1"
    )
    assert columns["fake_vendor_code"].to_pylist() == ["FAKE-CODE-0001"]
    assert [key for key, _ in _pairs(rest)] == ["FAKEROLE", "UNRESOLVED"], "and nothing else moved"

    dotted = codec.into_kwargs(
        codec.into_pairs(pyarrow.array(["toBridge #FAKE.VENDOR.CODE=FAKE-CODE-0002|#X=1"]), "UL"),
        "9.1",
    )
    assert codec.into_lifted_columns(dotted, "9.1")[0]["fake_vendor_code"].to_pylist() == [
        "FAKE-CODE-0002"
    ]


def test_a_codec_over_a_dictionary_that_declares_none_lifts_none(store: Offline) -> None:
    """The column exists in the parsed shape either way; only the value is absent."""
    codec = FixCodec(registry=store)
    assert set(codec.named_fields()) == set()
    kwargs = codec.into_kwargs(
        codec.into_pairs(pyarrow.array(["toBridge #FAKEVENDORCODE=x|#Y=1"]), "UL"), "9.1"
    )
    columns, rest = codec.into_lifted_columns(kwargs, "9.1")
    assert not any(column.to_pylist()[0] is not None for column in columns.values())
    assert [key for key, _ in _pairs(rest)] == ["FAKEVENDORCODE", "Y"]


def test_two_vendor_namespaces_of_one_name_stay_two_fields(store: Offline, tmp_path: Path) -> None:
    """A namespace is part of the name, and matching on the tail would merge them.

    Two vendors each render a `CLIENTID`. Reducing a rendered key to its last
    dotted segment -- which is what the parser does for a component path --
    would make one of them answer for the other's values.
    """
    for vendor, column in (("FAKEA", "fake_a_client"), ("FAKEB", "fake_b_client")):
        store.add_field(
            FieldEntry(
                name=f"{vendor}.CLIENTID",
                kind=NAMESPACE,
                variants={ANY_VERSION: {"type": "String"}},
                column=column,
            )
        )
    codec = FixCodec(registry=store)
    assert set(codec.named_fields()) == {"fakea.clientid", "fakeb.clientid"}
    assert "clientid" not in codec.named_fields(), "two fields claim it, so it is a guess"

    line = (
        "toBridge #FAKEA.CLIENTID=ACCT-TEST-01|#FAKEB.CLIENTID=ACCT-TEST-02|#CLIENTID=ACCT-TEST-03"
    )
    columns, rest = codec.into_lifted_columns(
        codec.into_kwargs(codec.into_pairs(pyarrow.array([line]), "UL"), "9.1"), "9.1"
    )
    assert columns["fake_a_client"].to_pylist() == ["ACCT-TEST-01"]
    assert columns["fake_b_client"].to_pylist() == ["ACCT-TEST-02"]
    assert [key for key, _ in _pairs(rest)] == ["CLIENTID"], "the bare one is nobody's"


# -- what a store refuses to be built with -----------------------------------


def test_two_identities_claiming_one_name_are_refused_when_a_store_is_built() -> None:
    """One name is one identity: a store holding two answers whichever it read first.

    Two *tags* cannot reach here -- a tag is what an identity is, so a second
    reading of one folds into the entry that already owns it -- but two tags
    spelled alike are two identities, and that is the collision.
    """
    with pytest.raises(ValueError, match="FIX field name 'fakerole' is claimed by"):
        explode(
            ("9.1",),
            {"9.1": [_field("FakeRole", 90001, "9.1"), _field("FAKEROLE", 90002, "9.1")]},
            {},
        )


def test_a_write_that_would_duplicate_a_tag_is_refused_whole(store: Offline) -> None:
    """Checked against what the store would hold after, and refused before writing."""
    with pytest.raises(ValueError, match="FIX tag 90001 is claimed by"):
        store.add_field(
            FieldEntry(name="FakeOther", tag=90001, variants={"9.1": {"type": "String"}})
        )
    assert store.resolve("FakeOther") is None, "and nothing was written"


def test_the_refusal_says_how_many_problems_and_what_they_are(store: Offline) -> None:
    with pytest.raises(ValueError, match="nothing was written"):
        store.add_field(
            FieldEntry(name="FakeOther", tag=90001, variants={"9.1": {"type": "String"}})
        )


def test_check_reports_a_duplicate_tag_the_same_way_a_write_refuses_it(
    store: Offline, tmp_path: Path
) -> None:
    """One authority for both, so `check` and a write never disagree."""
    entry = FieldEntry(name="FakeOther", tag=90001, variants={"9.1": {"type": "String"}})
    held = dict(store._entries[0])
    problems = _problems(({**held, entry.slug: entry}, store._entries[1]))
    assert any("FIX tag 90001 is claimed by" in problem for problem in problems)


# -- what a component declaration says a member must carry -------------------


def test_a_component_projects_with_the_nullability_its_spec_declares(
    store: Offline,
) -> None:
    """`required` is the whole rule: a member a message must carry is NOT NULL."""
    field = store.component_field("FakeParties", "9.1")
    (group,) = field.fields
    assert group.name == "no_fake_parties"
    assert group.nullable, "the group itself is optional in this declaration"
    member = group.item.field("fake_role")
    assert not member.nullable, "and its one member is required"
    assert member.arrow_type == pyarrow.int64(), "typed from the dictionary, not guessed"


def test_a_component_a_version_does_not_declare_projects_nothing(store: Offline) -> None:
    assert store.component_field("FakeParties", "9.0") is None
