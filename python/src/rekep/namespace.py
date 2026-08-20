"""Namespace and ResourceUri: how everything here is named.

OpenLineage identifies a `Job` and a `Dataset` by `namespace` + `name`
(https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md),
and both use the same recipe wherever the identifier nests: a job's
hierarchical name (`dag_id.task_id`), a dataset's schema-qualified name
(`db.schema.table`), a namespace's own authority (`catalog.namespace`). One
`Namespace` models all of it -- a name, an optional parent, and a `path()`
that walks the chain -- so `Job` and `Dataset` build their identifiers from it
instead of hand-joining strings.

`ResourceUri` is the identity those levels add up to: a service, a path and
an optional branch, spelled `ds:/catalog/namespace/name#branch`,
`job:/namespace/name`, or generically `rekep:/datasets/catalog/namespace/name`.
It is the one parser and the one formatter, so a resource written one way
reads back the same however it was spelled.
"""

from __future__ import annotations

import dataclasses
import urllib.parse

from rekep.records.record import Record, record


@record
class Namespace(Record):
    """One level of a hierarchical identifier, nesting through `parent`.

    A bare `Namespace(name="iceberg")` is a root; `namespace.child("trading")`
    is `iceberg.trading`. `path()` walks parent to self and joins with
    `separator`, so the same class builds an OpenLineage namespace
    (`s3://bucket`), a dotted dataset name (`db.schema.table`) or a job's
    hierarchical name (`dag_id.task_id`) -- whichever levels it is given.

    Self-referential by design: `into_arrow_schema()` refuses it (Arrow has
    no recursive types) the same way it refuses any recursive record, naming
    the field and the cycle. `Namespace` is a path-building helper, not a
    column -- nothing projects it onto Arrow.
    """

    name: str
    """This level's own name, unqualified by its parent."""

    parent: Namespace | None = None
    """The namespace this one lives inside, or None at the root."""

    separator: str = "."
    """Joiner between levels when building `path()`."""

    def levels(self) -> list[str]:
        """Every level's name, root first."""
        return [*self.parent.levels(), self.name] if self.parent is not None else [self.name]

    def path(self, separator: str | None = None) -> str:
        """The full path from the root to this level, `separator`-joined."""
        return (self.separator if separator is None else separator).join(self.levels())

    def depth(self) -> int:
        """How many levels deep this namespace is; a root is depth 1."""
        return len(self.levels())

    def child(self, name: str, *, separator: str | None = None) -> Namespace:
        """A new namespace one level under this one."""
        return Namespace(
            name=name, parent=self, separator=self.separator if separator is None else separator
        )

    @classmethod
    def of(cls, *levels: str, separator: str = ".") -> Namespace:
        """Build a namespace chain from `levels`, root first.

        `Namespace.of("iceberg", "trading", "orders")` is the same namespace
        `Namespace(name="iceberg").child("trading").child("orders")` builds,
        one call instead of one per level.
        """
        if not levels:
            raise ValueError("a namespace needs at least one level")
        current: Namespace | None = None
        for level in levels:
            current = cls(name=level, parent=current, separator=separator)
        return current  # type: ignore[return-value]

    def __str__(self) -> str:
        return self.path()


#: Short scheme -> the service it names. The service is also the first path
#: part of the generic `rekep:` form, so one table describes both spellings.
SERVICES = {"ds": "datasets", "job": "jobs"}

#: The reverse, for building.
SCHEMES = {service: scheme for scheme, service in SERVICES.items()}

#: The scheme that spells the service out instead of encoding it.
GENERIC_SCHEME = "rekep"


@dataclasses.dataclass(frozen=True)
class ResourceUri:
    """One resource's identity: a service, a path, and optionally a branch.

    Every resource this package addresses -- a dataset, a job -- is named the
    same way, and the spelling is a **path**, not a dotted string::

        ds:/warehouse/trading/orders#dev
        job:/pipeline/logs_to_records
        rekep:/datasets/warehouse/trading/orders#dev

    A path because that is what the thing is: a catalog contains namespaces,
    a namespace contains tables, and `/` is how every filesystem, URL and
    object store already spells containment. A dot cannot say that without
    ambiguity -- `a.b.c` might be three levels or a name with dots in it,
    and Iceberg namespaces are legitimately multi-level.

    The two spellings are the same identity. `ds:`/`job:` are shorthands
    that encode the service in the scheme; `rekep:` spells it as the first
    path part, which is what makes the scheme *generic* -- a new service is
    a new first path part, not a new scheme to teach every parser.

    The fragment is a branch, because a branch is not a different resource:
    `orders#dev` and `orders` are one table read at two refs, exactly the
    relationship a fragment models in every other URI.

    Levels are read right to left -- `name` is always the last, `namespace`
    the one before it, `catalog` the one before that -- so a shorter path is
    a less qualified name rather than a different shape.
    """

    service: str
    """Which kind of resource: `datasets`, `jobs`."""

    levels: tuple[str, ...]
    """The path, root first: catalog, namespace, name -- as many as given."""

    branch: str | None = None
    """The ref this identity is read or written at, when it names one."""

    # -- building ---------------------------------------------------------

    @classmethod
    def of(cls, service: str, *levels: str, branch: str | None = None) -> ResourceUri:
        """Build from parts, skipping empty levels."""
        kept = tuple(str(level) for level in levels if level)
        if not kept:
            raise ValueError(f"a {service} uri needs at least a name")
        return cls(service=service, levels=kept, branch=branch or None)

    @classmethod
    def parse(cls, text: str, *, service: str | None = None) -> ResourceUri:
        """Read any of the spellings above back into one identity.

        `service` is the fallback for a bare path (`trading/orders`), which
        is what a side file naturally writes when its folder already says
        what kind of resource it holds.
        """
        split = urllib.parse.urlsplit(str(text).strip())
        levels = [part for part in (split.netloc, *split.path.split("/")) if part]
        scheme = split.scheme.lower()
        if scheme in SERVICES:
            found = SERVICES[scheme]
        elif scheme in (GENERIC_SCHEME, ""):
            found = levels.pop(0) if levels and levels[0] in SCHEMES else service
        else:
            raise ValueError(
                f"{text!r}: unknown scheme {scheme!r}; use "
                f"{', '.join(f'{s}:' for s in SERVICES)} or {GENERIC_SCHEME}:/<service>/<path>"
            )
        if not found:
            raise ValueError(
                f"{text!r}: no service; name one with a scheme, a leading path part, or service="
            )
        return cls.of(found, *levels, branch=split.fragment or None)

    # -- reading ----------------------------------------------------------

    def name(self) -> str:
        """The resource's own name: the last level."""
        return self.levels[-1]

    def namespace(self) -> str:
        """The level the name lives in, `default` when the path is bare."""
        return self.levels[-2] if len(self.levels) > 1 else "default"

    def catalog(self) -> str | None:
        """The level the namespace lives in, when the path is qualified that far."""
        return self.levels[-3] if len(self.levels) > 2 else None

    def path(self) -> str:
        """The levels alone, `/`-joined, no scheme and no fragment."""
        return "/".join(self.levels)

    def at(self, branch: str | None) -> ResourceUri:
        """The same resource, read or written at `branch`."""
        return dataclasses.replace(self, branch=branch or None)

    def child(self, name: str) -> ResourceUri:
        """One level deeper: this path with `name` appended."""
        return dataclasses.replace(self, levels=(*self.levels, name))

    # -- writing ----------------------------------------------------------

    def __str__(self) -> str:
        """The short form: `ds:/catalog/namespace/name#branch`."""
        return f"{SCHEMES[self.service]}:/{self.path()}{self._fragment()}"

    def generic(self) -> str:
        """The long form: `rekep:/datasets/catalog/namespace/name#branch`."""
        return f"{GENERIC_SCHEME}:/{self.service}/{self.path()}{self._fragment()}"

    def _fragment(self) -> str:
        return f"#{self.branch}" if self.branch else ""
