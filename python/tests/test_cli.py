"""The `rekep` command, run in this process so a failure is a traceback."""

import dataclasses
import json
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, Log, StructField, cli

#: The contracts this repository publishes, which the CLI has to be able to
#: read -- the same directory `tests/test_schemas.py` pins.
SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


def run(*argv: str) -> int:
    return cli.main(list(argv))


# -- dumping ----------------------------------------------------------------


def test_dump_writes_the_declaration_to_stdout(capsysbinary: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Log") == 0
    written = capsysbinary.readouterr().out
    assert Field.from_yaml(written) == Log.into_field()


def test_dump_takes_a_dotted_class_too(capsysbinary: pytest.CaptureFixture) -> None:
    """`module:Attribute` is what an entry point writes; the dot is what a docstring does."""
    assert run("fields", "dump", "--pyclass", "rekep.text.log.Log") == 0
    assert Field.from_yaml(capsysbinary.readouterr().out) == Log.into_field()


@pytest.mark.parametrize(
    ("suffix", "reader"), [(".yaml", Field.from_yaml), (".json", Field.from_json)]
)
def test_dump_infers_the_format_from_the_target(tmp_path: Path, suffix: str, reader) -> None:
    target = tmp_path / f"log{suffix}"
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Log", "--target", str(target)) == 0
    assert reader(str(target)) == Log.into_field()


def test_dump_format_wins_over_the_extension(tmp_path: Path) -> None:
    """It was typed; the extension was merely there."""
    target = tmp_path / "log.yaml"
    argv = ("fields", "dump", "--pyclass", "rekep.text.log:Log", "--format", "json")
    assert run(*argv, "--target", str(target)) == 0
    assert json.loads(target.read_text())["name"] == "Log"


def test_dump_writes_toml_when_asked(tmp_path: Path) -> None:
    target = tmp_path / "log.toml"
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Log", "--target", str(target)) == 0
    assert Field.from_toml(str(target)) == Log.into_field()


def test_only_the_document_reaches_stdout(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """So a dump with no target pipes, and one with a target says where it went."""
    target = tmp_path / "log.yaml"
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Log", "--target", str(target)) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert str(target) in captured.err


def test_dump_takes_a_plain_dataclass(capsysbinary: pytest.CaptureFixture) -> None:
    """`field_of` is the one reading of "a shape", so the CLI inherits all of it."""
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
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Nothing") == 1
    assert "has no 'Nothing'" in capsys.readouterr().err


def test_a_missing_module_is_reported(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "nowhere.at.all:Log") == 1
    assert "nowhere" in capsys.readouterr().err


def test_a_spec_that_names_no_class_says_how_to_write_one(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "Log") == 1
    assert "module:Attribute" in capsys.readouterr().err


# -- loading ----------------------------------------------------------------


def test_load_builds_what_the_document_declares(capsys: pytest.CaptureFixture) -> None:
    """The count is taken off the declaration and pinned, so a column that left
    the contract cannot take the printed number quietly with it.

    `fix_tags` is the one line the renderer has to spell out of a nested type,
    and `check_sum` pins the public naming, so together they cover the shape.
    """
    assert run("fields", "load", "--target", str(SCHEMAS / "rekep" / "log.yaml")) == 0
    printed = capsys.readouterr().out
    assert len(Log.into_field().names) == 105
    assert "Log: 105 columns, builds" in printed
    assert "unix: int64  [primary key]" in printed
    assert "unix_hour: int64  [partition identity]" in printed
    assert (
        "fix_tags: list<item: struct<key: int32 not null, value: string not null> not null>"
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
    unknown.write_text("name: Log\n")
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
    target = tmp_path / "log.yaml"
    assert run("fields", "dump", "--pyclass", "rekep.text.log:Log", "--target", str(target)) == 0
    assert run("fields", "load", "--target", str(target)) == 0
    assert Field.from_file(str(target)) == Log.into_field()
    assert "builds" in capsys.readouterr().out


def test_from_file_is_what_load_reads_with(tmp_path: Path) -> None:
    """The command can never read something the library cannot."""
    target = tmp_path / "shape.json"
    shape = StructField.from_arrow_schema(pyarrow.schema([("a", pyarrow.int32())]), "Shape")
    shape.into_json(str(target))
    assert Field.from_file(str(target)) == shape
    with pytest.raises(ValueError, match="not a document this can read"):
        Field.from_file(str(tmp_path / "shape.parquet"))
