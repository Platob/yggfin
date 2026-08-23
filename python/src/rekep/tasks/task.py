"""Notebook configuration as a portable document."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from rekep.convert import Convertible


@dataclasses.dataclass
class Task(Convertible):
    """A notebook and the parameters a runner injects into it."""

    name: str = ""
    """Stable scheduler and output name."""

    notebook: str = ""
    """Notebook path, relative to this task document when not absolute."""

    parameters: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Values injected into the notebook's tagged parameters cell."""

    def __post_init__(self) -> None:
        if not self.notebook:
            raise ValueError("a task document must point to a notebook")

    def into_notebook_path(self, document: str | Path) -> Path:
        """Resolve the notebook beside `document`; absolute paths stay absolute."""
        notebook = Path(self.notebook)
        if notebook.is_absolute():
            return notebook
        return Path(document).resolve().parent / notebook
