"""Folder registries: one entry per config file, shared by the deployments."""

from __future__ import annotations

import json
import pathlib
import tomllib
from collections.abc import Mapping
from typing import Any

from rekep.render import render
from rekep.require import require

#: Config extensions a registry folder is scanned for.
EXTENSIONS = (".yaml", ".yml", ".toml", ".json")


def entries(directory: pathlib.Path, cls: type, context: Mapping[str, Any]) -> list[Any]:
    """One entry per config file in `directory`, the stem defaulting `name`."""
    found = []
    for path in sorted(directory.glob("*")) if directory.is_dir() else []:
        if path.suffix not in EXTENSIONS:
            continue
        mapping = parse(path, context)
        mapping.setdefault("name", path.stem)
        found.append(cls.from_dict(mapping))
    return found


def parse(path: pathlib.Path, context: Mapping[str, Any]) -> dict[str, Any]:
    """One config file to a mapping, Jinja rendered first."""
    text = render(path.read_text(encoding="utf-8"), **context)
    if path.suffix in (".yaml", ".yml"):
        return require("yaml", "yaml").safe_load(text) or {}
    if path.suffix == ".toml":
        return tomllib.loads(text)
    return dict(json.loads(text))


def named(found: list[Any], name: str, kind: str) -> Any:
    """The entry called `name`, or a refusal listing what is declared."""
    for entry in found:
        if entry.name == name:
            return entry
    known = ", ".join(entry.name for entry in found) or "none"
    raise KeyError(f"no {kind} named {name!r} in the deployment (declared: {known})")
