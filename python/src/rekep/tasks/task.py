"""Marimo application configuration as a portable document."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from rekep.convert import Convertible


@dataclasses.dataclass
class Task(Convertible):
    """A Marimo application and the definitions a runner runs it with."""

    name: str = ""
    """Stable scheduler and result name."""

    application: str = ""
    """Marimo application path, relative to this task document."""

    parameters: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Defaults for the application's parameter cell, one key per definition."""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a task document must name its task")
        if not self.application:
            raise ValueError("a task document must point to a Marimo application")

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Task:
        """Build a task, refusing a document that carries an undeclared key."""
        if not isinstance(mapping, Mapping):
            raise TypeError("a task document is a mapping")
        declared = {member.name for member in dataclasses.fields(cls)}
        unexpected = sorted(set(mapping) - declared)
        if unexpected:
            raise TypeError(
                "a task document declares name, application and parameters; unexpected "
                + ", ".join(unexpected)
            )
        # Checked before decoding, which would otherwise report a list of
        # parameters as whatever failed to unpack inside it.
        if not isinstance(mapping.get("parameters", {}), Mapping):
            raise TypeError("task parameters must be a mapping of definition names")
        return super().from_dict(mapping)

    def into_application_path(self, document: str | Path) -> Path:
        """Resolve the application beside `document`, refusing one outside it.

        Containment is the check a deployment needs: a task document names the
        job next to it, so an application reached through `..` or out of
        another checkout is a mistake rather than a configuration.
        """
        directory = Path(document).resolve().parent
        named = Path(self.application)
        resolved = (named if named.is_absolute() else directory / named).resolve()
        if not resolved.is_relative_to(directory):
            raise ValueError(f"{self.application} is outside {directory}")
        if not resolved.is_file():
            raise FileNotFoundError(resolved)
        return resolved
