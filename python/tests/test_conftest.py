"""The suite's own isolation: a refused credential skips, nothing else does."""

import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from .conftest import REFUSED

PROBES = """
import sys


def test_raised():
    raise RuntimeError("pyiceberg.exceptions.ForbiddenError: RESTError 403")


def test_printed():
    print(
        "parse_fix_refined: ForbiddenError: RESTError 403: Received unexpected JSON "
        'Payload: {"message":"The security token included in the request is invalid."}',
        file=sys.stderr,
    )
    assert 1 == 0


def test_failed():
    # A ForbiddenError this source names is not one the test met.
    assert 1 == 0
"""


def test_a_refused_credential_skips_and_any_other_failure_fails(tmp_path: Path) -> None:
    """Raised or only printed -- a subprocess prints its
    failure and exits nonzero -- a refusal is the environment's, and the skip
    says so. A traceback's source naming one is not a refusal."""
    shutil.copy(Path(__file__).with_name("conftest.py"), tmp_path / "conftest.py")
    (tmp_path / "test_probes.py").write_text(textwrap.dedent(PROBES), encoding="utf-8")

    ran = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "pytest", "-q", "-rfs", "-p", "no:cacheprovider", str(tmp_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    # Every assertion reads the run with its refusals masked, because a
    # failed assertion quotes its operands and one quoting a refusal would
    # skip this test rather than fail it.
    returncode, said = ran.returncode, REFUSED.sub("<refusal>", ran.stdout)
    assert returncode == 1, said
    assert "1 failed, 2 skipped" in said, said
    assert "FAILED test_probes.py::test_failed" in said, said
    skipped = [line for line in said.splitlines() if line.startswith("SKIPPED")]
    assert len(skipped) == 2, said
    assert all(line.endswith("refused the credentials: <refusal>") for line in skipped), said
