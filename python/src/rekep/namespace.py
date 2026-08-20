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
an optional branch, spelled `rekep:///<service>/<path>#branch` --
`rekep:///datasets/catalog/namespace/name#dev`, `rekep:///jobs/namespace/name`.
Three slashes, the shape `file:///var/log` has: the authority is **empty and
reserved**, because what would go there is a host, and a rekep identity is not
hosted anywhere yet. Leaving the slot open costs nothing now and is the
difference between adding `rekep://lake.internal/datasets/...` later and
rewriting every URI ever committed.
**One spelling, not a family of them**: it is the one parser and the one
formatter, so a URI in a log line, a side file and the registry key is the
same string rather than three that have to be normalised before they can be
compared.

**A resource that names itself names itself with a URI, and `uri:` never
means anything else.** A dataset, a task and a dag each spell one
(`uri: rekep:///jobs/pipeline/files_to_logs`); a stack's catalogs and
namespaces are named by the registry folder and file stem they live in
instead. Either way nothing else may take the word: an Iceberg catalog's
connection string is `endpoint:`, not a second thing called `uri`.
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


#: Every service a URI may name: one entry per resource that names *itself*,
#: a dataset, a task, a dag -- plus `records`, whose members are classes named
#: by what they are called (`rekep:///records/log`) rather than declared in a
#: side file: a config that points at a schema, and an orchestrator's asset
#: graph, both need a name for one, and every name here is a URI.
#: A stack's catalogs and namespaces are not here -- their identity is the
#: registry folder they sit in and the stem of their file, which is why they
#: are addressed by name and not by URI.
SERVICES = ("datasets", "jobs", "dags", "records")

#: The one scheme, written with an empty authority: `rekep:///<service>/<path>`.
#: There is no short form -- a `ds:`/`job:`/`dag:` shorthand would be a second
#: spelling of the same identity, and every parser, every log line and every
#: side file would then have to know both.
SCHEME = "rekep"

#: Scheme and authority: what every identity starts with, before the path. One
#: constant because it is both what `__str__` writes and what `parse` insists
#: on, and two places spelling a prefix separately is how they drift apart.
PREFIX = f"{SCHEME}://"


@dataclasses.dataclass(frozen=True)
class ResourceUri:
    """One resource's identity: a service, a path, and optionally a branch.

    Every resource this package addresses -- a dataset, a task, a dag, and
    the record a schema lives in -- is named the same way, in one spelling::

        rekep:///datasets/warehouse/trading/orders#dev
        rekep:///jobs/pipeline/logs_to_records
        rekep:///dags/pipeline/trading_logs
        rekep:///records/log

    A path because that is what the thing is: a catalog contains namespaces,
    a namespace contains tables, and `/` is how every filesystem, URL and
    object store already spells containment. A dot cannot say that without
    ambiguity -- `a.b.c` might be three levels or a name with dots in it,
    and Iceberg namespaces are legitimately multi-level.

    **Three slashes, because the authority is empty and reserved.** `//`
    opens the slot a URI keeps for a host and the third `/` begins the path,
    exactly as `file:///var/log` does. Nothing hosts a rekep identity today,
    so nothing goes in that slot -- but a deployment that one day needs to
    say *whose* datasets these are writes
    `rekep://lake.internal/datasets/...` without one existing URI changing.
    Spending the slot on the service instead would spend, for a name the
    path already has room for, the one part of the syntax that cannot be
    added later.

    **The service is the first path part, never a scheme of its own.** That
    is what makes `rekep:` generic: a new kind of resource is a new first
    part, not a new scheme to teach every parser -- and one identity has one
    spelling, so a URI in a log, a side file and a registry key are the same
    string rather than three that have to be normalised before comparison.

    The fragment is a branch, because a branch is not a different resource:
    `orders#dev` and `orders` are one table read at two refs, exactly the
    relationship a fragment models in every other URI.

    Levels are read right to left -- `name` is always the last, `namespace`
    the one before it, `catalog` the one before that -- so a shorter path is
    a less qualified name rather than a different shape.
    """

    service: str
    """Which kind of resource: one of `SERVICES`."""

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
        """Read `rekep:///<service>/<path>#branch` back into one identity.

        A schemeless path is accepted as well, because a reference made from
        inside a service already knows which one it is in: `service=` names
        it, and the path may still lead with the service itself. That is a
        *fallback for an incomplete reference*, not a second spelling -- what
        comes back out of `__str__` is always the full form.

        Anything else carrying this scheme is refused by name, because the
        slashes are the point:

        - fewer than three (`rekep:/x`, `rekep://x`) writes the service into
          the slot the host is reserved for, and puts a second string in
          circulation for one identity -- which is what a single spelling
          exists to prevent.
        - a filled authority (`rekep://host/x`) names a host nothing reads
          yet. Parsing it would mean dropping that host silently, and a name
          half-kept is worse than one refused: this way the mistake lands
          here rather than in whichever lookup quietly misses.
        """
        text = str(text).strip()
        split = urllib.parse.urlsplit(text)
        scheme = split.scheme.lower()
        if scheme not in (SCHEME, ""):
            raise ValueError(
                f"{text!r}: unknown scheme {scheme!r}; every resource is named "
                f"{PREFIX}/<service>/<path>, with the service one of {', '.join(SERVICES)}"
            )
        if scheme and split.netloc:
            raise ValueError(
                f"{text!r}: {split.netloc!r} sits in the authority, which is reserved for a host "
                f"and read by nothing yet; write {PREFIX}/<service>/<path>"
            )
        if scheme and split.path.strip("/") and not text.lower().startswith(f"{PREFIX}/"):
            raise ValueError(
                f"{text!r}: an identity is written with three slashes -- the authority is empty, "
                f"not missing; write {PREFIX}/{split.path.lstrip('/')}"
            )
        levels = [part for part in split.path.split("/") if part]
        found = levels.pop(0) if levels and levels[0] in SERVICES else service
        if not found:
            raise ValueError(f"{text!r}: no service; name one as the first path part or service=")
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
        """The levels alone, `/`-joined, no scheme, service or fragment."""
        return "/".join(self.levels)

    def at(self, branch: str | None) -> ResourceUri:
        """The same resource, read or written at `branch`."""
        return dataclasses.replace(self, branch=branch or None)

    def child(self, name: str) -> ResourceUri:
        """One level deeper: this path with `name` appended."""
        return dataclasses.replace(self, levels=(*self.levels, name))

    # -- writing ----------------------------------------------------------

    def __str__(self) -> str:
        """The whole identity: `rekep:///<service>/<path>#branch`."""
        return f"{PREFIX}/{self.service}/{self.path()}{self._fragment()}"

    def _fragment(self) -> str:
        return f"#{self.branch}" if self.branch else ""
