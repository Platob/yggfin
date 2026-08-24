"""The `rekep` command, run in this process so a failure is a traceback."""

import dataclasses
import json
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixMessage, StructField, cli
from rekep.fix import registry as registry_module
from rekep.fix.entries import Alias, FieldEntry
from rekep.fix.fields import fix_field
from rekep.fix.registry import FixRegistry
from rekep.fix.store import Collapse, ConflictReport, Dropped

#: The contracts this repository publishes, which the CLI has to be able to
#: read -- the same directory `tests/test_schemas.py` pins.
SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


def run(*argv: str) -> int:
    return cli.main(list(argv))


# -- dumping ----------------------------------------------------------------


def test_dump_writes_the_declaration_to_stdout(capsysbinary: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "rekep.text.fixmessage:FixMessage") == 0
    written = capsysbinary.readouterr().out
    assert Field.from_yaml(written) == FixMessage.into_field()


def test_dump_takes_a_dotted_class_too(capsysbinary: pytest.CaptureFixture) -> None:
    """`module:Attribute` is what an entry point writes; the dot is what a docstring does."""
    assert run("fields", "dump", "--pyclass", "rekep.text.fixmessage.FixMessage") == 0
    assert Field.from_yaml(capsysbinary.readouterr().out) == FixMessage.into_field()


@pytest.mark.parametrize(
    ("suffix", "reader"), [(".yaml", Field.from_yaml), (".json", Field.from_json)]
)
def test_dump_infers_the_format_from_the_target(tmp_path: Path, suffix: str, reader) -> None:
    target = tmp_path / f"log{suffix}"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "rekep.text.fixmessage:FixMessage",
            "--target",
            str(target),
        )
        == 0
    )
    assert reader(str(target)) == FixMessage.into_field()


def test_dump_format_wins_over_the_extension(tmp_path: Path) -> None:
    """It was typed; the extension was merely there."""
    target = tmp_path / "fixmessage.yaml"
    argv = ("fields", "dump", "--pyclass", "rekep.text.fixmessage:FixMessage", "--format", "json")
    assert run(*argv, "--target", str(target)) == 0
    assert json.loads(target.read_text())["name"] == "FixMessage"


def test_dump_writes_toml_when_asked(tmp_path: Path) -> None:
    target = tmp_path / "log.toml"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "rekep.text.fixmessage:FixMessage",
            "--target",
            str(target),
        )
        == 0
    )
    assert Field.from_toml(str(target)) == FixMessage.into_field()


def test_only_the_document_reaches_stdout(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """So a dump with no target pipes, and one with a target says where it went."""
    target = tmp_path / "fixmessage.yaml"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "rekep.text.fixmessage:FixMessage",
            "--target",
            str(target),
        )
        == 0
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert str(target) in captured.err


def test_dump_takes_a_plain_dataclass(capsysbinary: pytest.CaptureFixture) -> None:
    """`Field.from_` is the one reading of "a shape", so the CLI inherits all of it."""
    assert run("fields", "dump", "--pyclass", "tests.test_cli:Venue") == 0
    dumped = Field.from_yaml(capsysbinary.readouterr().out)
    assert dumped.names == ["mic", "country"]
    assert dumped.field("country").nullable is True


@dataclasses.dataclass
class Venue:
    """A venue, declared without the decorator."""

    mic: str
    country: str | None = None


def test_a_class_that_is_not_a_shape_is_refused(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "rekep.cli:FORMATS") == 1
    assert "does not name a shape" in capsys.readouterr().err


def test_a_missing_attribute_names_the_module(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "rekep.text.fixmessage:Nothing") == 1
    assert "has no 'Nothing'" in capsys.readouterr().err


def test_a_missing_module_is_reported(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "nowhere.at.all:FixMessage") == 1
    assert "nowhere" in capsys.readouterr().err


def test_a_spec_that_names_no_class_says_how_to_write_one(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "FixMessage") == 1
    assert "module:Attribute" in capsys.readouterr().err


# -- loading ----------------------------------------------------------------


def test_load_builds_what_the_document_declares(capsys: pytest.CaptureFixture) -> None:
    """The count is taken off the declaration and pinned, so a column that left
    the contract cannot take the printed number quietly with it.

    `kwargs` is the one line the renderer has to spell out of a nested type,
    and `check_sum` pins the public naming, so together they cover the shape.
    """
    assert run("fields", "load", "--target", str(SCHEMAS / "rekep" / "fixmessage.yaml")) == 0
    printed = capsys.readouterr().out
    assert len(FixMessage.into_field().names) == 105
    assert "FixMessage: 105 columns, builds" in printed
    assert "unix: int64  [primary key]" in printed
    assert "unix_hour: int64  [partition identity]" in printed
    assert (
        "kwargs: list<item: struct<tag: int32 not null, key: string not null, "
        "value: string, trans: string, namespace: string, comp: string> not null>"
        "  [nullable]"
    ) in printed
    assert "parties: list<item: struct<party_id: string" in printed
    assert "check_sum: string  [nullable]" in printed
    assert "primary keys: ['unix', 'hash']" in printed


@pytest.mark.parametrize(
    "contract",
    sorted(path.name for path in SCHEMAS.rglob("*") if path.suffix in {".yaml", ".json"}),
)
def test_every_published_contract_loads(contract: str, capsys: pytest.CaptureFixture) -> None:
    (path,) = SCHEMAS.rglob(contract)
    assert run("fields", "load", "--target", str(path)) == 0
    assert "builds" in capsys.readouterr().out


def test_a_document_that_does_not_build_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """Parsing is not the check -- building is."""
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps({"name": "Broken", "type": "struct", "fields": [{"name": "x"}]}))
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "'x' has no type" in capsys.readouterr().err


def test_a_document_with_an_unknown_type_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("name: Broken\ntype: struct\nfields:\n- name: x\n  type: int65\n")
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "int65" in capsys.readouterr().err


def test_an_extension_nobody_reads_names_the_ones_that_are(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    unknown = tmp_path / "log.txt"
    unknown.write_text("name: FixMessage\n")
    assert run("fields", "load", "--target", str(unknown)) == 1
    message = capsys.readouterr().err
    assert ".json" in message and ".yaml" in message and ".toml" in message


def test_a_missing_document_is_reported(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "load", "--target", str(tmp_path / "nowhere.yaml")) == 1
    assert "nowhere.yaml" in capsys.readouterr().err


# -- the round trip the CLI exists for --------------------------------------


def test_dump_then_load_is_the_contract_workflow(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """What CI runs: publish the declaration, then check the file builds."""
    target = tmp_path / "fixmessage.yaml"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "rekep.text.fixmessage:FixMessage",
            "--target",
            str(target),
        )
        == 0
    )
    assert run("fields", "load", "--target", str(target)) == 0
    assert Field.from_file(str(target)) == FixMessage.into_field()
    assert "builds" in capsys.readouterr().out


def test_from_file_is_what_load_reads_with(tmp_path: Path) -> None:
    """The command can never read something the library cannot."""
    target = tmp_path / "shape.json"
    shape = StructField.from_arrow_schema(pyarrow.schema([("a", pyarrow.int32())]), "Shape")
    shape.into_json(str(target))
    assert Field.from_file(str(target)) == shape
    with pytest.raises(ValueError, match="not a document this can read"):
        Field.from_file(str(tmp_path / "shape.parquet"))


# -- the FIX registry --------------------------------------------------------
#
# Registering a newly observed vendor field or a newly confirmed alias is what
# a classification run produces, and it has to be an operation rather than a
# hand edit of a JSON file. Every identity here is synthetic.


@pytest.fixture
def store(tmp_path: Path) -> Path:
    """A registry holding one synthetic field, in its tag shard."""
    registry = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    registry._store_versions(("9.1",))
    registry._store_fields("9.1", [fix_field("FakeRole", 90001, "int", version="9.1")])
    return Path(registry.cache_dir)


def reopened(store: Path) -> FixRegistry:
    """The store as a fresh registry, so a test reads what was written."""
    return FixRegistry(cache_dir=store, offline=True)


def test_a_vendor_field_is_registered_updated_and_removed(store: Path) -> None:
    assert (
        run(
            "fix",
            "registry",
            "add-field",
            "--store",
            str(store),
            "--name",
            "FAKE.VENDOR.CODE",
            "--type",
            "String",
            "--description",
            "A vendor's own code.",
            "--column",
            "fake_vendor_code",
            "--alias",
            "FAKEVENDORCODE",
        )
        == 0
    )
    entry = reopened(store).resolve("FAKE.VENDOR.CODE")
    assert entry.kind == "namespace" and entry.tag is None
    assert entry.column == "fake_vendor_code"
    assert reopened(store).resolve("FAKEVENDORCODE").name == "FAKE.VENDOR.CODE"

    assert (
        run(
            "fix",
            "registry",
            "update-field",
            "--store",
            str(store),
            "--name",
            "FAKE.VENDOR.CODE",
            "--type",
            "String",
            "--column",
            "renamed",
        )
        == 0
    )
    updated = reopened(store).resolve("FAKE.VENDOR.CODE")
    assert updated.column == "renamed"
    assert [alias.name for alias in updated.aliases] == ["FAKEVENDORCODE"], "kept, not dropped"

    assert (
        run("fix", "registry", "remove-field", "--store", str(store), "--name", "FAKEVENDORCODE")
        == 0
    )
    assert reopened(store).resolve("FAKE.VENDOR.CODE") is None


def test_a_numbered_field_is_registered_for_the_versions_it_names(store: Path) -> None:
    assert (
        run(
            "fix",
            "registry",
            "add-field",
            "--store",
            str(store),
            "--name",
            "FakeCode",
            "--tag",
            "90002",
            "--type",
            "String",
            "--version",
            "9.1",
        )
        == 0
    )
    entry = reopened(store).resolve("FakeCode")
    assert entry.tag == 90002 and entry.versions == ("9.1",)
    assert reopened(store).field(90002, "9.1").name == "FakeCode"


def test_removing_a_field_the_store_does_not_have_reports_it(
    store: Path, capsys: pytest.CaptureFixture
) -> None:
    assert (
        run("fix", "registry", "remove-field", "--store", str(store), "--name", "FakeAbsent") == 1
    )
    assert "FakeAbsent" in capsys.readouterr().err


def test_an_alias_is_recorded_with_where_it_was_counted(store: Path) -> None:
    assert (
        run(
            "fix",
            "registry",
            "alias-field",
            "--store",
            str(store),
            "--name",
            "FakeRole",
            "--alias",
            "FakeRolle",
            "--source",
            "brk",
            "--occurrences",
            "41",
        )
        == 0
    )
    (alias,) = reopened(store).resolve("FakeRole").aliases
    assert (alias.name, alias.source, alias.occurrences) == ("FakeRolle", "brk", 41)
    assert reopened(store).resolve("FAKEROLLE").tag == 90001, "matching folds case"


def test_a_change_that_would_break_the_store_is_refused_and_not_written(
    store: Path, capsys: pytest.CaptureFixture
) -> None:
    """The validation is the point of having a verb rather than an editor."""
    run(
        "fix",
        "registry",
        "alias-field",
        "--store",
        str(store),
        "--name",
        "FakeRole",
        "--alias",
        "FakeSpelling",
    )
    assert (
        run(
            "fix",
            "registry",
            "add-field",
            "--store",
            str(store),
            "--name",
            "FakeOther",
            "--tag",
            "90003",
            "--version",
            "9.1",
            "--alias",
            "FakeSpelling",
        )
        == 1
    )
    assert "claimed by" in capsys.readouterr().err
    assert reopened(store).resolve("FakeOther") is None
    assert reopened(store).check() == []


def test_a_component_is_registered_from_a_declaration_and_removed(
    store: Path, tmp_path: Path
) -> None:
    declaration = tmp_path / "fake_parties.json"
    declaration.write_text(
        json.dumps(
            {
                "name": "FakeParties",
                "versions": ["9.1"],
                "members": [
                    {
                        "kind": "group",
                        "name": "NoFakeParties",
                        "tag": 90004,
                        "members": [{"kind": "field", "name": "FakeRole", "tag": 90001}],
                    }
                ],
            }
        )
    )
    assert (
        run(
            "fix",
            "registry",
            "add-component",
            "--store",
            str(store),
            "--declaration",
            str(declaration),
        )
        == 0
    )
    assert reopened(store).component("FakeParties", "9.1").members[0].tag == 90004

    assert (
        run(
            "fix",
            "registry",
            "update-component",
            "--store",
            str(store),
            "--declaration",
            str(declaration),
        )
        == 0
    )
    assert (
        run("fix", "registry", "remove-component", "--store", str(store), "--name", "FakeParties")
        == 0
    )
    assert (
        run("fix", "registry", "remove-component", "--store", str(store), "--name", "FakeParties")
        == 1
    )


def test_check_reports_an_inconsistent_store_and_says_a_sound_one_is_sound(
    store: Path, capsys: pytest.CaptureFixture
) -> None:
    assert run("fix", "registry", "check", "--store", str(store)) == 0
    printed = capsys.readouterr()
    assert printed.out == "", "nothing a pipe would read"
    assert "sound" in printed.err, "and one line saying so, where a person reads"

    clashing = FieldEntry(
        name="FakeOther",
        tag=90003,
        aliases=(Alias(name="FakeRole"),),
        versions=("9.1",),
        type="String",
    )
    reopened(store)._layout.store_field(clashing)
    assert run("fix", "registry", "check", "--store", str(store)) == 1
    assert "FakeRole" in capsys.readouterr().err


def test_bootstrap_fills_a_cold_store_and_writes_what_it_collapsed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one command that fetches the dictionary, and the report a build reads."""

    class Fetching(FixRegistry):
        """A bootstrap whose scrape is one synthetic version."""

        def rebuild(self, *versions: str) -> ConflictReport:
            self._store_versions(("9.1",))
            self._store_fields("9.1", [fix_field("FakeRole", 90001, "int", version="9.1")])
            report = ConflictReport(
                collapses=(
                    Collapse("FakeRole", "values", "9.1", (Dropped("9.0", "Buy", "1"),), 90001),
                )
            )
            self.__dict__["_conflicts"] = report
            return report

    monkeypatch.setattr(cli, "FixRegistry", Fetching)
    target = tmp_path / "fresh"
    report = tmp_path / "conflicts.json"
    assert run("fix", "registry", "bootstrap", "--store", str(target), "--report", str(report)) == 0
    # Naming the default store makes construction pay for the scrape, and the
    # verb must not then pay for a second one.
    monkeypatch.setattr(registry_module, "CACHE_DIRECTORY", tmp_path / "default")
    with pytest.warns(RuntimeWarning):
        assert run("fix", "registry", "bootstrap", "--store", str(tmp_path / "default")) == 0
    assert reopened(tmp_path / "default").field("FakeRole", "9.1").name == "FakeRole"
    assert reopened(target).field("FakeRole", "9.1").name == "FakeRole"
    written = json.loads(report.read_text())
    assert written["counts"]["values"] == 1
    assert written["collapses"][0]["dropped"] == [{"version": "9.0", "reading": "Buy", "key": "1"}]
