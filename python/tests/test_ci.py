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
