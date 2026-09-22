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
from rekep.fix import FixRegistry

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "tools" / "fix_registry.py"
DUMP = ROOT / "tools" / "fix_registry_dump.py"
DOCUMENTATION = ROOT / "docs" / "tools" / "fix-registry.md"
ASSETS = ROOT / "docs" / "assets"


def _application() -> Any:
    specification = importlib.util.spec_from_file_location("rekep_fix_registry_tool", TOOL)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module.app


def _dump_module() -> Any:
    specification = importlib.util.spec_from_file_location("rekep_fix_registry_dump", DUMP)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def _field() -> Field:
    symbol = Field("symbol", "utf8")
    symbol.set_display("Symbol")
    symbol.fix.tag = 55
    symbol.fix.names = ["ticker"]
    symbol.fix.description = "Instrument identifier"
    # Both documents are the collection itself, in the order the
    # specification states it, rather than one named member of an object, and
    # a lineage entry types its field with a datatype rather than a spelling.
    symbol.fix["lineage"] = json.dumps(
        [{"since": "2.7", "name": "symbol", "type": {"type": "string"}}],
        separators=(",", ":"),
    )
    return symbol


def _registry() -> FixRegistry:
    """A field names one vocabulary; the registry owns its members."""
    registry = FixRegistry()
    registry.set_codeset("symbolcodeset", [{"value": "AAPL", "name": "Apple"}])
    symbol = _field()
    symbol.fix.codeset = "symbolcodeset"
    registry.insert(symbol)
    return registry


def test_tool_opens_and_projects_a_native_registry(tmp_path: Path) -> None:
    app = _application()
    setup = dict(app._setup._glbls)
    location = tmp_path / "fix"
    _registry().write_into(location)

    dictionary = setup["open_registry"](location.as_uri())

    rows = setup["into_registry_rows"](dictionary)
    assert next(row for row in rows if row["tag"] == 55) == {
        "_id": dictionary.field_by_tag(55).fix.id,
        "_search": "55 symbol symbol ticker instrument identifier",
        "_name": "symbol",
        "tag": 55,
        "name": "Symbol",
        "dialect": "standard",
        "shape": "field",
        "category": "fields",
        "Arrow kind": "text",
        "FIX type": "string",
        "since": "2.7",
        "names": "ticker",
        "description": "Instrument identifier",
    }
    # A bare registry is already the crate's own definitions and the standard
    # clocks seeded beside them, so the store contributes the one row this
    # fixture defines and the crate contributes the rest.
    assert len(rows) == 1 + len(list(FixRegistry()))
    symbol = dictionary.field_by_tag(55)
    assert symbol.fix.codeset == "symbolcodeset"
    assert setup["codeset_records"](dictionary, symbol)[0]["name"] == "Apple"


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
    schema = setup["into_fixmsg_schema"](_registry())
    assert schema.name == "FixMsg"
    assert schema["symbol"].display == "Symbol"
    # A pair no dictionary explains is an entry of tag 0 inside the arrival
    # record, so one record column holds everything that arrived.
    assert {member.name for member in schema} >= {"symbol", "nofixentries"}
    assert Field.from_json(schema.into_json()) == schema


def test_dump_references_one_registry_owned_code_set(tmp_path: Path, monkeypatch) -> None:
    """The index names a vocabulary; the detail document states it once."""
    dump = _dump_module()
    monkeypatch.setattr(dump, "ASSETS", tmp_path)
    monkeypatch.setattr(dump, "fix_registry", _registry)

    assert dump.main() == 0
    index = json.loads((tmp_path / "fix-registry.json").read_text(encoding="utf-8"))
    details = json.loads((tmp_path / "fix-details.json").read_text(encoding="utf-8"))
    symbol = next(row for row in index["fields"] if row["name"] == "symbol")

    assert symbol["codeset"] == "symbolcodeset"
    assert details["fields"][str(symbol["id"])]["codeset"] == "symbolcodeset"
    assert "codes" not in details["fields"][str(symbol["id"])]
    assert details["codesets"]["symbolcodeset"][0]["name"] == "Apple"


def test_browser_decoder_resolves_a_centralized_code_set() -> None:
    """The published decoder reads Side's name through its one shared set."""
    script = r"""
const fs = require("node:fs");
class Element {
  constructor(tag) {
    this.tag = tag; this.children = []; this.dataset = {};
    this.handlers = {}; this._text = "";
  }
  appendChild(child) { this.children.push(child); return child; }
  addEventListener(name, handler) { this.handlers[name] = handler; }
  setAttribute(name, value) { this[name] = value; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text; }
}
const mount = new Element("div");
mount.dataset.fix = "decode";
global.window = {};
global.document = {
  readyState: "complete",
  currentScript: { src: "file:///docs/assets/fix.js" },
  baseURI: "file:///docs/",
  createElement: (tag) => new Element(tag),
  querySelectorAll: () => [mount],
  querySelector: () => null,
  addEventListener: () => {},
};
const registry = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const details = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
global.fetch = (url) => Promise.resolve({
  ok: true,
  json: () => Promise.resolve(String(url).includes("fix-details") ? details : registry)
});
require(process.argv[1]);
const contains = (node, text) => node._text === text ||
  node.children.some((child) => contains(child, text));
setTimeout(() => {
  if (!contains(mount, "Buy")) throw new Error("centralized Side code name was not rendered");
}, 0);
"""
    checked = subprocess.run(  # noqa: S603
        [
            "node",
            "-e",
            script,
            "--",
            str(ASSETS / "fix.js"),
            str(ASSETS / "fix-registry.json"),
            str(ASSETS / "fix-details.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr


def test_tool_is_strict_marimo_and_uses_the_rekep_fix_surface() -> None:
    checked = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "marimo", "check", "--strict", str(TOOL)],
        capture_output=True,
        text=True,
        check=False,
    )
    source = TOOL.read_text(encoding="utf-8")

    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert "from rekep.fix import FixRegistry, fix_codec, fix_registry" in source
    assert "yggdryl" not in source.casefold()
    assert "Task" not in source
    assert "Iceberg" not in source
    assert not (ROOT / "tasks" / "fix_registry").exists()


def test_documentation_labels_the_standalone_tool_and_uv_entrypoint() -> None:
    page = DOCUMENTATION.read_text(encoding="utf-8")

    assert "Standalone tool" in page
    assert "uv run --project python --group runner --frozen" in page
    assert "fix_registry" in page
    assert "7,781" in page
    assert "Field.explode_fields()" in page
    assert "Field.into_json(indent=2)" in page
    assert "registry-dependent `FixMsg` projection" in page
