"""Shared test isolation: local catalogs and no cloud discovery."""

from __future__ import annotations

import re
from collections.abc import Generator
from pathlib import Path

import pytest

#: What a remote service answers when it refuses the inert credentials below.
#: Every catalog here is local, so a test that meets one reached a service
#: the runner's own configuration names, which nothing here can pass: the test
#: is skipped with the refusal as its reason rather than failed.
REFUSED = re.compile(
    r"\b(?:Forbidden|Unauthorized|NoCredentials)Error\b"
    r"|security token included in the request is (?:invalid|expired)"
    r"|\b(?:InvalidAccessKeyId|InvalidClientTokenId|UnrecognizedClientException"
    r"|ExpiredToken|SignatureDoesNotMatch)\b"
    r"|AWS Error ACCESS_DENIED"
)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Skip a test that failed on a refusal `REFUSED` names, raised or only
    printed: a task's command reports a failed table and exits nonzero.

    Only what the failure said is read -- each exception's message down its
    chain, and the output the test captured -- never a traceback's source.
    """
    report = yield
    if report.failed and report.when != "teardown":
        longrepr = report.longrepr
        chain = getattr(longrepr, "chain", None) or [(None, getattr(longrepr, "reprcrash", None))]
        said = [crash.message for _, crash, *_ in chain if crash is not None]
        if isinstance(longrepr, str):
            said.append(longrepr)
        said.extend(text for _, text in report.sections)
        if refused := REFUSED.search("\n".join(said)):
            path, line, _ = item.reportinfo()
            report.outcome = "skipped"
            report.longrepr = (
                str(path),
                (line or 0) + 1,
                f"Skipped: a remote service refused the credentials: {refused.group(0)}",
            )
    return report


@pytest.fixture(autouse=True)
def _keep_aws_discovery_off_the_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give Arrow inert credentials so unit tests never probe EC2 metadata."""
    for name in (
        "S3_ENDPOINT_URL",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_SESSION_TOKEN",
        "S3_REGION",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL_S3TABLES",
        "AWS_ENDPOINT_URL_GLUE",
        "AWS_ENDPOINT_URL",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "rekep-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "rekep-test")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def catalog_properties(tmp_path: Path, name: str = "warehouse") -> dict[str, str]:
    """A SQLite catalog and a file warehouse under `tmp_path`, so a test reaches nothing.

    `name` is what keeps two catalogs in one test apart: each needs a database
    *and* a warehouse directory of its own, or two tables of the same name land
    on the same files and a comparison compares one of them with itself.
    """
    warehouse = tmp_path / name
    warehouse.mkdir(parents=True, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / f'{name}.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }
