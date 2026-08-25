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
