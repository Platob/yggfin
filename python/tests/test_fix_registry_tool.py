"""The standalone FIX registry browser is a rekep-native view."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow

from rekep import Field
from rekep.fix import FixRegistry, fix_crate_fields, fix_ulbridge_fields

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "tools" / "fix_registry.py"
DOCUMENTATION = ROOT / "docs" / "tools" / "fix-registry.md"


def _application() -> Any:
    specification = importlib.util.spec_from_file_location("rekep_fix_registry_tool", TOOL)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module.app


def _field() -> Field:
    symbol = Field("symbol", "utf8")
    symbol.set_display("Symbol")
    symbol.fix.tag = 55
    symbol.fix.aliases = ["ticker"]
    symbol.fix.description = "Instrument identifier"
    symbol.fix["lineage"] = json.dumps(
        {"entries": [{"since": "2.7", "name": "symbol", "type": "String"}]},
        separators=(",", ":"),
    )
    symbol.fix["codes"] = json.dumps(
        {"codes": [{"value": "AAPL", "name": "Apple"}]},
        separators=(",", ":"),
    )
    return symbol


def test_tool_opens_and_projects_a_native_registry(tmp_path: Path) -> None:
    app = _application()
    setup = dict(app._setup._glbls)
    location = tmp_path / "fix"
    FixRegistry.from_fields([_field()]).write_into(location)

    dictionary = setup["open_registry"](location.as_uri())

    rows = setup["into_registry_rows"](dictionary)
    assert next(row for row in rows if row["tag"] == 55) == {
        "_id": "55:",
        "_name": "symbol",
        "_search": "55 symbol symbol ticker instrument identifier",
        "tag": 55,
        "name": "Symbol",
        "branch": "standard",
        "shape": "field",
        "Arrow kind": "text",
        "FIX type": "String",
        "since": "2.7",
        "aliases": "ticker",
        "description": "Instrument identifier",
    }
    assert len(rows) == 1 + len(fix_crate_fields()) + len(fix_ulbridge_fields())
    assert setup["metadata_records"](_field(), "codes", "codes") == [
        {"value": "AAPL", "name": "Apple"}
    ]


def test_tool_uses_native_shape_and_schema_operations() -> None:
    app = _application()
    setup = dict(app._setup._glbls)
    group = Field.from_arrow(
        pyarrow.field(
            "parties",
            pyarrow.list_(
                pyarrow.struct(
                    [
                        pyarrow.field("partyid", pyarrow.string(), nullable=False),
                        pyarrow.field("partyrole", pyarrow.int32()),
                    ]
                )
            ),
        )
    )
    group.fix.tag = 453

    assert [row["path"] for row in setup["member_rows"](group)] == ["partyid", "partyrole"]
    schema = setup["into_fixmsg_schema"](FixRegistry.from_fields([_field()]))
    assert schema.name == "FixMsg"
    assert schema["symbol"].display == "Symbol"
    assert {member.name for member in schema} >= {
        "symbol",
        "nofixentries",
        "nounmappedfixentries",
    }
    assert Field.from_json(schema.into_json()) == schema


def test_tool_is_strict_marimo_and_uses_the_rekep_fix_surface() -> None:
    checked = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "marimo", "check", "--strict", str(TOOL)],
        capture_output=True,
        text=True,
        check=False,
    )
    source = TOOL.read_text(encoding="utf-8")

    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert "from rekep.fix import PAYLOAD_COLUMN, FixCodec, FixRegistry, fix_registry" in source
    assert "yggdryl" not in source.casefold()
    assert "Task" not in source
    assert "Iceberg" not in source
    assert not (ROOT / "tasks" / "fix_registry").exists()


def test_documentation_labels_the_standalone_tool_and_uv_entrypoint() -> None:
    page = DOCUMENTATION.read_text(encoding="utf-8")

    assert "Standalone tool" in page
    assert "uv run --project python --group runner --frozen" in page
    assert "fix_registry" in page
    assert "6,883" in page
    assert "Field.explode_fields()" in page
    assert "Field.into_json(indent=2)" in page
    assert "carrier stating the payload column" in page
