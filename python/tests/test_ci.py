"""CI keeps the costly Iceberg checks available without slowing every PR."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


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
    assert "refs/pull/{0}/merge" in checkout["with"]["ref"]
    assert checkout["with"]["persist-credentials"] == "false"

    commands = [step["run"] for step in job["steps"] if "run" in step]
    assert any("pytest -q -m integration" in command for command in commands)
    assert all("comment.body" not in command for command in commands)


def test_the_release_publishes_rekep_before_the_full_registry() -> None:
    workflow = _workflow("release.yml")
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["on"]["release"]["types"] == ["published"]
    assert "workflow_dispatch" in workflow["on"]
    assert set(workflow["on"]) == {"release", "workflow_dispatch"}

    job = workflow["jobs"]["publish"]
    assert "environment" in job
    assert job["env"]["UV_PUBLISH_URL"] == "${{ vars.ARTIFACTORY_PYPI_URL }}"
    assert job["env"]["UV_PUBLISH_CHECK_URL"] == ("${{ vars.ARTIFACTORY_PYPI_CHECK_URL }}")
    assert job["env"]["REKEP_FIX_REGISTRY_URL"] == "${{ vars.REKEP_FIX_REGISTRY_URL }}"
    assert job["env"]["ARTIFACTORY_TOKEN"] == "${{ secrets.ARTIFACTORY_TOKEN }}"

    steps = job["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["persist-credentials"] == "false"
    commands = {step["name"]: step["run"] for step in steps if "name" in step and "run" in step}
    names = [step.get("name") for step in steps]
    assert "uv build --no-sources" in commands["Build rekep"]
    assert "fix-registry.zip" in commands["Build full FIX registry"]
    assert "UV_PUBLISH_CHECK_URL" in commands["Check Artifactory configuration"]
    assert "uv publish --no-attestations" in commands["Publish rekep"]
    registry = commands["Publish full FIX registry"]
    assert "--request PUT" in registry
    assert "X-Checksum-Sha256" in registry
    assert '"$REKEP_FIX_REGISTRY_URL"' in registry
    assert names.index("Publish rekep") < names.index("Publish full FIX registry")
