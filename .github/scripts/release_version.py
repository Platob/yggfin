"""Decide whether `pyproject.toml` names a version this repository has not released.

The release is cut from the version the project declares, so a merged bump is
the whole trigger and there is no second place to remember to tag. This says
only what is true -- which version, which one preceded it, whether it moved --
and leaves tagging and publishing to the workflow, which is where the
credentials are.

Run standalone so it can be tested: `.github/workflows/release.yml` calls it,
and `python/tests/test_ci.py` calls it too, over temporary repositories.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from packaging.version import InvalidVersion, Version

#: How a released version is spelled as a git tag. One prefix, no variants:
#: a second spelling is a second thing to search for on every run.
PREFIX = "v"


@dataclasses.dataclass(frozen=True)
class Decision:
    """What the working tree says about releasing, as the workflow reads it."""

    #: The version `pyproject.toml` declares.
    version: str
    #: The newest version already tagged, empty where nothing is.
    previous: str
    #: Whether to cut a release for `version`.
    cut: bool
    #: Why, in one line a person reads in the run summary.
    reason: str

    def into_dict(self) -> dict[str, str]:
        return {
            "version": self.version,
            "previous": self.previous,
            "cut": "true" if self.cut else "false",
            "reason": self.reason,
        }


def project_version(pyproject: Path) -> str:
    """The version `pyproject.toml` declares, refusing one it computes."""
    document = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = document.get("project", {})
    if "version" in project.get("dynamic", []):
        raise ValueError(f"{pyproject} computes its version; this reads a declared one")
    declared = project.get("version")
    if not declared:
        raise ValueError(f"{pyproject} declares no version")
    version = str(declared)
    parsed = Version(version)
    if len(parsed.release) != 3 or str(parsed) != version:
        raise ValueError(
            f"{pyproject} version must be canonical MAJOR.MINOR.PATCH, got {version!r}"
        )
    return version


def released(directory: Path) -> list[Version]:
    """Every version already tagged here, oldest first.

    A tag that does not spell a version is not one -- the repository may carry
    tags for other things -- so it is skipped rather than failing the run.
    """
    found = subprocess.run(  # noqa: S603
        ["git", "tag", "--list", f"{PREFIX}*"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
        cwd=directory,
    )
    versions = []
    for line in found.stdout.split("\n"):
        name = line.strip()
        if not name.startswith(PREFIX):
            continue
        try:
            versions.append(Version(name[len(PREFIX) :]))
        except InvalidVersion:
            continue
    return sorted(versions)


def decide(declared: str, tagged: list[Version]) -> Decision:
    """Whether `declared` is a version to cut, given what is already tagged."""
    version = Version(declared)
    previous = str(tagged[-1]) if tagged else ""
    if version in tagged:
        return Decision(declared, previous, False, f"{PREFIX}{declared} is already released")
    if tagged and version < tagged[-1]:
        return Decision(
            declared,
            previous,
            False,
            f"{declared} is behind the released {previous}; nothing to cut",
        )
    if not tagged:
        return Decision(declared, previous, True, f"the first release, at {declared}")
    return Decision(declared, previous, True, f"{previous} was released; {declared} is the bump")


def changes(directory: Path, previous: str) -> str:
    """Every commit since `previous`, as the release notes carry them.

    Subjects only. A release note is read to answer "what moved", and a body
    per commit buries that under the reasoning for each one -- which is in the
    commits, where it belongs, and linked from every line here.
    """
    span = [f"{PREFIX}{previous}..HEAD"] if previous else ["HEAD"]
    found = subprocess.run(  # noqa: S603
        ["git", "log", "--no-merges", "--reverse", "--pretty=format:- %s (%h)", *span],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
        cwd=directory,
    )
    return found.stdout.strip() or "- no commits since the previous release"


def _emit(decision: Decision, notes: str) -> None:
    """Write the outputs the workflow reads, and the summary a person reads."""
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            for key, value in decision.into_dict().items():
                handle.write(f"{key}={value}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(f"## Release check\n\n{decision.reason}.\n\n")
            if decision.cut:
                handle.write(f"Cutting `{PREFIX}{decision.version}`.\n\n{notes}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--pyproject", type=Path, default=None)
    parser.add_argument("--notes", type=Path, default=None, help="write the release notes here")
    arguments = parser.parse_args(argv)

    root = arguments.root.resolve()
    pyproject = arguments.pyproject or root / "python" / "pyproject.toml"
    decision = decide(project_version(pyproject), released(root))
    notes = changes(root, decision.previous) if decision.cut else ""
    if arguments.notes and decision.cut:
        arguments.notes.write_text(notes + "\n", encoding="utf-8")
    _emit(decision, notes)
    print(json.dumps({**decision.into_dict(), "notes": notes}, indent=1))
    return 0


if __name__ == "__main__":  # pragma: no cover - the workflow's entry point
    sys.exit(main())
