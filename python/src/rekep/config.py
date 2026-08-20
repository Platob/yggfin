"""Where a declared resource lives on disk, and what is loaded right now.

Two things, both small, both shared by `Dataset` and `Job`.

**A folder.** A side file has to live somewhere, and there are two sensible
somewheres: inside the repository that owns the pipeline (`stacks/datasets`,
reviewed and deployed with the code), or in the user's own config
(`~/.config/rekep/datasets`, theirs alone, not committed anywhere). `folder()`
prefers an explicit argument, then the repository's, then the user's -- so a
checkout keeps working unchanged and a bare `pip install rekep` still has
somewhere to put things.

**A registry.** Resolving a `rekep:///datasets/...` reference should not mean
re-reading a directory, and two modules asking for the same dataset should get the same
object. `REGISTRY` is that: process-wide, keyed by URI, filled as resources
load. It is deliberately a plain dict behind three functions rather than a
class with state -- there is exactly one of these per process, and a class
you may only instantiate once is a module wearing a hat.
"""

from __future__ import annotations

import os
import pathlib
from typing import Any

from rekep.namespace import PREFIX

#: Root of the user's own configuration: `$REKEP_CONFIG_HOME`, else
#: XDG's `$XDG_CONFIG_HOME/rekep`, else `~/.config/rekep`.
CONFIG_HOME = pathlib.Path(
    os.environ.get("REKEP_CONFIG_HOME")
    or pathlib.Path(os.environ.get("XDG_CONFIG_HOME") or pathlib.Path.home() / ".config") / "rekep"
)

#: Root of a checkout's own declarations, relative to the working directory.
STACKS_HOME = pathlib.Path(os.environ.get("REKEP_STACKS_HOME", "stacks"))

#: Every loaded resource, by `str(resource_uri())`. Shared across modules on
#: purpose: one dataset declaration, one object.
REGISTRY: dict[str, Any] = {}


def folder(
    service: str, root: str | os.PathLike[str] | None = None, *, create: bool = False
) -> pathlib.Path:
    """Where `service`'s side files live -- `datasets`, `jobs`.

    `root` wins when given. Otherwise the checkout's `stacks/<service>` when
    it exists, because a repository that declares its own pipelines should
    not be quietly overridden by whatever is in a home directory; and the
    user's config home when it does not.

    `create=True` makes the directory, which is what dumping wants and
    loading does not: a missing folder means "nothing declared", not an
    error.
    """
    if root is not None:
        chosen = pathlib.Path(root)
    else:
        local = STACKS_HOME / service
        chosen = local if local.is_dir() else CONFIG_HOME / service
    if create:
        chosen.mkdir(parents=True, exist_ok=True)
    return chosen


def register(resource: Any) -> Any:
    """Put `resource` in the shared registry under its URI, and return it."""
    REGISTRY[str(resource.resource_uri())] = resource
    return resource


def lookup(uri: str, service: str | None = None) -> Any | None:
    """The registered resource `uri` names, or None.

    Every way of writing the identity finds it -- `rekep:///datasets/a/b`,
    `/datasets/a/b`, and a bare `a/b` with `service="datasets"` are one
    resource, so they resolve to one entry rather than three misses.
    """
    from rekep.namespace import ResourceUri

    return REGISTRY.get(str(ResourceUri.parse(uri, service=service)))


def registered(service: str | None = None) -> list[Any]:
    """Everything loaded so far, optionally just one service's.

    The key is the whole URI, so the service is simply what it leads with --
    no second index to keep in step with the first, and nothing to normalise:
    every key was written by one formatter.
    """
    if service is None:
        return list(REGISTRY.values())
    prefix = f"{PREFIX}/{service}/"
    return [resource for uri, resource in REGISTRY.items() if uri.startswith(prefix)]


def clear() -> None:
    """Empty the registry -- for tests, and for a process that reloads config."""
    REGISTRY.clear()
