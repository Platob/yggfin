"""Optional Jinja rendering for deploy-time configuration."""

from __future__ import annotations

import functools
import os
import re
import subprocess
from typing import Any

from rekep.require import require

#: Branches whose slug means "trunk": suffix and prefix render empty there, so
#: production objects keep clean names and only branches get decorated.
TRUNK_BRANCHES = ("", "main", "master")


def render(text: str, /, **context: Any) -> str:
    """Render `text` as a Jinja template when it is one, else pass it through.

    Config and DDL files often need one deploy-time value -- a bucket, a
    catalog, an account -- so any of them may use `{{ }}`/`{% %}` and be
    rendered with the caller's context plus `env` (the process environment)
    and the git context below. Text without Jinja markers never touches
    Jinja, so the dependency stays optional; undefined variables raise rather
    than dissolving into empty strings, because a half-rendered config is
    worse than a loud one.
    """
    if "{{" not in text and "{%" not in text:
        return text
    jinja2 = require("jinja2", "jinja")
    template = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(text)  # noqa: S701
    return template.render(env=os.environ, **{**git_context(), **context})


@functools.lru_cache(maxsize=1)
def git_context() -> dict[str, str]:
    """Git and GitHub facts, preformatted for templating names.

    GitHub Actions variables win over asking git, so CI renders what it is
    actually building (a PR's head branch, the pushed sha). The preformatted
    `git_branch_suffix`/`git_branch_prefix` carry their own `_` separator and
    are empty on trunk, so `log_records{{ git_branch_suffix }}` is
    `log_records` in production and `log_records_feature_x` on a branch.
    """
    branch = (
        os.environ.get("GITHUB_HEAD_REF")
        or os.environ.get("GITHUB_REF_NAME")
        or _git("branch", "--show-current")
    )
    sha = os.environ.get("GITHUB_SHA") or _git("rev-parse", "HEAD")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", branch).strip("_").lower()
    trunk = slug in TRUNK_BRANCHES
    return {
        "git_branch": branch,
        "git_branch_slug": slug,
        "git_branch_suffix": "" if trunk else f"_{slug}",
        "git_branch_prefix": "" if trunk else f"{slug}_",
        "git_sha": sha,
        "git_short_sha": sha[:7],
        "git_repository": os.environ.get("GITHUB_REPOSITORY", ""),
    }


def _git(*args: str) -> str:
    """One git query, empty outside a repository or without git installed."""
    try:
        result = subprocess.run(  # noqa: S603, S607 - fixed argv, no shell
            ["git", *args], capture_output=True, text=True, timeout=5, check=False
        )
    except OSError:
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""
