"""CI keeps the costly Iceberg checks available without slowing every PR."""

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def _release_version() -> ModuleType:
    """The release check, imported from where the workflow runs it."""
    path = ROOT / ".github" / "scripts" / "release_version.py"
    spec = importlib.util.spec_from_file_location("release_version", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered first: the module declares a dataclass, and `dataclasses`
    # resolves its annotations through `sys.modules[cls.__module__]`.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RELEASE = _release_version()


def _workflow(name: str) -> dict:
    return yaml.load(
        (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )


def _trusted_push_branch(workflow: dict) -> str:
    branches = workflow["on"]["push"]["branches"]
    assert len(branches) == 1
    branch = branches[0]
    assert branch and not any(character in branch for character in "*+?[]!")
    return branch


def test_pull_request_ci_selects_the_fast_suite() -> None:
    workflow = _workflow("ci.yml")
    steps = workflow["jobs"]["test"]["steps"]
    test = next(step for step in steps if step.get("name") == "Test")
    assert '-m "not integration"' in test["run"]


def test_the_integration_workflow_runs_only_trusted_code_paths() -> None:
    workflow = _workflow("integration.yml")
    assert workflow["permissions"] == {"contents": "read"}
    assert _trusted_push_branch(workflow) == _trusted_push_branch(_workflow("ci.yml"))
    assert workflow["on"]["issue_comment"]["types"] == ["created"]

    job = workflow["jobs"]["test"]
    condition = job["if"]
    assert "github.event.issue.pull_request" in condition
    assert "--integration" in condition
    assert all(role in condition for role in ("OWNER", "MEMBER", "COLLABORATOR"))

    checkout = next(
        step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@")
    )
    assert checkout["uses"] == ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1")
    assert "refs/pull/{0}/merge" in checkout["with"]["ref"]
    assert checkout["with"]["persist-credentials"] == "false"
    setup = next(
        step for step in job["steps"] if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    assert setup["uses"] == "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d"

    commands = [step["run"] for step in job["steps"] if "run" in step]
    assert any("pytest -q -m integration" in command for command in commands)
    assert all("comment.body" not in command for command in commands)


def test_the_release_attaches_the_wheel_and_optionally_publishes_it() -> None:
    workflow = _workflow("release.yml")
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["on"]["release"]["types"] == ["published"]
    assert "workflow_dispatch" in workflow["on"]
    assert set(workflow["on"]) == {"push", "release", "workflow_dispatch"}
    assert _trusted_push_branch(workflow) == _trusted_push_branch(_workflow("ci.yml"))

    job = workflow["jobs"]["publish"]
    assert "environment" in job
    assert job["permissions"] == {"contents": "write"}
    assert job["env"]["UV_PUBLISH_URL"] == "${{ vars.ARTIFACTORY_PYPI_URL }}"
    assert job["env"]["UV_PUBLISH_CHECK_URL"] == ("${{ vars.ARTIFACTORY_PYPI_CHECK_URL }}")
    assert job["env"]["UV_PUBLISH_USERNAME"] == "${{ secrets.ARTIFACTORY_USERNAME }}"
    assert job["env"]["UV_PUBLISH_PASSWORD"] == "${{ secrets.ARTIFACTORY_TOKEN }}"

    steps = job["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["uses"] == ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1")
    assert "needs.version.outputs.version" in checkout["with"]["ref"]
    assert "github.event.release.tag_name" in checkout["with"]["ref"]
    assert checkout["with"]["persist-credentials"] == "false"
    setup = next(step for step in steps if step.get("uses", "").startswith("astral-sh/setup-uv@"))
    assert setup["uses"] == "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d"
    commands = {step["name"]: step["run"] for step in steps if "name" in step and "run" in step}
    assert "uv build --no-sources" in commands["Build rekep"]
    attach = next(
        step for step in steps if step.get("name") == "Attach rekep to the GitHub release"
    )
    assert attach["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "gh release upload" in attach["run"]
    assert "--clobber" in attach["run"]
    configured = next(
        step for step in steps if step.get("name") == "Check Artifactory configuration"
    )
    assert configured["id"] == "artifactory"
    assert all(
        name in configured["run"]
        for name in (
            "ARTIFACTORY_PYPI_URL",
            "ARTIFACTORY_PYPI_CHECK_URL",
            "ARTIFACTORY_USERNAME",
            "ARTIFACTORY_TOKEN",
        )
    )
    assert 'present" -eq 0' in configured["run"]
    assert "partially configured" in configured["run"]
    publish = next(step for step in steps if step.get("name") == "Publish rekep")
    assert publish["if"] == "steps.artifactory.outputs.configured == 'true'"
    assert "uv publish --no-attestations" in publish["run"]
    assert steps.index(attach) < steps.index(configured) < steps.index(publish)
    assert not any("FIX registry" in name for name in commands)


def test_pages_builds_the_strict_docs_and_deploys_only_trusted_code() -> None:
    workflow = _workflow("pages.yml")
    assert _trusted_push_branch(workflow) == _trusted_push_branch(_workflow("ci.yml"))
    assert set(workflow["on"]) == {"push", "workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"] == {
        "group": "pages",
        "cancel-in-progress": "false",
    }

    build = workflow["jobs"]["build"]
    assert build["if"] == "github.ref_name == github.event.repository.default_branch"
    steps = build["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["uses"] == ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1")
    assert checkout["with"]["persist-credentials"] == "false"
    setup = next(step for step in steps if step.get("uses", "").startswith("astral-sh/setup-uv@"))
    assert setup["uses"] == "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d"
    commands = {step["name"]: step["run"] for step in steps if "name" in step and "run" in step}
    assert "--group docs --frozen" in commands["Sync"]
    assert "mkdocs build --strict" in commands["Build"]
    uploaded = next(
        step for step in steps if step.get("uses", "").startswith("actions/upload-pages-artifact@")
    )
    assert uploaded["uses"] == (
        "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
    )
    assert uploaded["with"]["path"] == "site"
    assert steps.index(uploaded) > next(
        index for index, step in enumerate(steps) if step.get("name") == "Build"
    )

    deploy = workflow["jobs"]["deploy"]
    assert deploy["if"] == "github.ref_name == github.event.repository.default_branch"
    assert deploy["needs"] == "build"
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert deploy["environment"]["name"] == "github-pages"
    assert "steps.deployment.outputs.page_url" in deploy["environment"]["url"]
    deploy_steps = deploy["steps"]
    configured = next(
        step for step in deploy_steps if step.get("uses", "").startswith("actions/configure-pages@")
    )
    assert configured["uses"] == (
        "actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d"
    )
    deployment = next(
        step for step in deploy_steps if step.get("uses", "").startswith("actions/deploy-pages@")
    )
    assert deployment["uses"] == ("actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128")
    assert deployment["id"] == "deployment"
    assert deploy_steps.index(configured) < deploy_steps.index(deployment)


# -- cutting a release from the declared version -----------------------------


def test_a_version_bump_is_what_cuts_a_release() -> None:
    """The one job that may write to the repository, and what gates it."""
    workflow = _workflow("release.yml")
    job = workflow["jobs"]["version"]
    assert job["permissions"] == {"contents": "write"}, "and nothing wider"

    steps = job["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["uses"] == ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1")
    assert checkout["with"]["persist-credentials"] == "false"
    assert checkout["with"]["fetch-depth"] == "0", "the notes span every commit since the tag"
    setup = next(step for step in steps if step.get("uses", "").startswith("astral-sh/setup-uv@"))
    assert setup["uses"] == "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d"

    commands = {step["name"]: step["run"] for step in steps if "name" in step and "run" in step}
    assert ".github/scripts/release_version.py" in commands["Decide"]
    cut = next(step for step in steps if step.get("name") == "Cut the release")
    assert cut["if"] == "steps.decide.outputs.cut == 'true'"
    assert cut["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "gh release create" in cut["run"], "which creates the tag as well"

    # A release created with the workflow's own token starts no other run, so
    # publishing has to be chained here rather than left to the release event.
    publish = workflow["jobs"]["publish"]
    assert publish["needs"] == "version"
    assert "needs.version.outputs.cut == 'true'" in publish["if"]
    assert "github.event_name != 'push'" in publish["if"]


def _repository(tmp_path: Path, version: str, tags: tuple[str, ...] = ()) -> Path:
    """A repository declaring `version`, with `tags` already released."""
    root = tmp_path / "repo"
    (root / "python").mkdir(parents=True)
    (root / "python" / "pyproject.toml").write_text(
        f'[project]\nname = "rekep"\nversion = "{version}"\n', encoding="utf-8"
    )
    run = lambda *argv: subprocess.run(argv, cwd=root, check=True, capture_output=True)  # noqa: E731
    run("git", "init", "-q", "-b", "main")
    run("git", "config", "user.email", "test@example.invalid")
    run("git", "config", "user.name", "test")
    run("git", "add", "-A")
    run("git", "commit", "-qm", "the first thing")
    for tag in tags:
        run("git", "tag", tag)
        (root / "python" / f"{tag}.txt").write_text(tag, encoding="utf-8")
        run("git", "add", "-A")
        run("git", "commit", "-qm", f"work after {tag}")
    return root


@pytest.mark.parametrize(
    ("version", "tags", "cut", "previous"),
    [
        ("0.1.0", (), True, ""),
        ("0.2.0", ("v0.1.0",), True, "0.1.0"),
        ("0.1.0", ("v0.1.0",), False, "0.1.0"),
        ("0.1.0", ("v0.1.0", "v0.2.0"), False, "0.2.0"),
        ("1.0.0", ("v0.9.0", "v0.10.0"), True, "0.10.0"),
        ("1.0.0rc1", ("v0.9.0",), True, "0.9.0"),
    ],
    ids=["first", "bump", "already", "behind", "ten-sorts-after-nine", "prerelease"],
)
def test_a_release_is_cut_only_for_a_version_ahead_of_every_tag(
    tmp_path: Path, version: str, tags: tuple[str, ...], cut: bool, previous: str
) -> None:
    """`0.10.0` is after `0.9.0`, which a string comparison gets backwards."""
    root = _repository(tmp_path, version, tags)
    decision = RELEASE.decide(
        RELEASE.project_version(root / "python" / "pyproject.toml"), RELEASE.released(root)
    )
    assert (decision.cut, decision.version, decision.previous) == (cut, version, previous)
    assert decision.reason, "a run summary always says why"


def test_the_notes_are_every_commit_since_the_previous_release(tmp_path: Path) -> None:
    root = _repository(tmp_path, "0.2.0", ("v0.1.0",))
    head = subprocess.run(
        ["git", "log", "-1", "--pretty=%h"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()
    assert RELEASE.changes(root, "0.1.0") == f"- work after v0.1.0 ({head})"
    whole = RELEASE.changes(root, "")
    assert "the first thing" in whole and "work after v0.1.0" in whole


def test_a_tag_that_does_not_spell_a_version_is_not_one(tmp_path: Path) -> None:
    """The repository may carry tags for other things; they are skipped."""
    root = _repository(tmp_path, "0.2.0", ("v0.1.0",))
    subprocess.run(["git", "tag", "vendor-drop"], cwd=root, check=True, capture_output=True)
    subprocess.run(["git", "tag", "v-not-a-version"], cwd=root, check=True, capture_output=True)
    assert [str(one) for one in RELEASE.released(root)] == ["0.1.0"]


def test_a_computed_version_is_refused_rather_than_guessed_at(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "rekep"\ndynamic = ["version"]\n', encoding="utf-8")
    with pytest.raises(ValueError, match="computes its version"):
        RELEASE.project_version(pyproject)


def test_the_declared_version_is_the_one_the_workflow_reads() -> None:
    """The script defaults to the path the repository actually keeps it at."""
    assert RELEASE.project_version(ROOT / "python" / "pyproject.toml")
    assert sys.executable, "the script runs under whatever uv resolves"
