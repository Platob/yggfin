"""The benchmarks have to still run against the code they measure.

Nothing else imports them, so a rename in `src/` leaves a benchmark that
raises on its first line and says so to nobody. Three of the six had rotted
that way -- two on a name that no longer existed, one on a column that had
become NOT NULL -- and every one was an import or a first call, which is what
this catches for the price of importing six modules.

Running them is the integration-marked half. `--quick` is one complete smoke
pass through every assertion; statistically repeated timings remain the
standalone benchmark's job. Scalar references use bounded samples there.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

BENCHMARKS = Path(__file__).resolve().parents[1] / "benchmarks"

#: Every benchmark, and whether `--quick` can run here. All six do: the
#: registry one reads the published archive and a copy of it on disk, and
#: answers every question from both rather than fetching anything.
SCRIPTS = {
    "bench_cast": True,
    "bench_fix": True,
    "bench_fix_registry": True,
    "bench_iceberg": True,
    "bench_market": True,
    "bench_text_file": True,
}


def test_every_benchmark_is_listed_here() -> None:
    found = {path.stem for path in BENCHMARKS.glob("bench_*.py")}
    assert found == set(SCRIPTS), "a new benchmark is not covered, or a listed one is gone"


@pytest.mark.parametrize("name", sorted(SCRIPTS))
def test_a_benchmark_still_imports(name: str) -> None:
    """Its imports are its claim about what the package still exports."""
    spec = importlib.util.spec_from_file_location(name, BENCHMARKS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(name, None)
    assert callable(module.main)


@pytest.mark.integration
@pytest.mark.parametrize("name", sorted(one for one, runs in SCRIPTS.items() if runs))
def test_a_benchmark_still_runs(name: str) -> None:
    """`--quick` end to end: an assertion inside one is a claim about the code."""
    done = subprocess.run(  # noqa: S603
        [sys.executable, str(BENCHMARKS / f"{name}.py"), "--quick"],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    assert done.returncode == 0, done.stderr[-4000:]
