"""Namespace: a generic, hierarchical identifier that builds its own path.

OpenLineage identifies a `Job` and a `Dataset` by `namespace` + `name`
(https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md),
and both use the same recipe wherever the identifier nests: a job's
hierarchical name (`dag_id.task_id`), a dataset's schema-qualified name
(`db.schema.table`), a namespace's own authority (`catalog.namespace`). One
`Namespace` models all of it -- a name, an optional parent, and a `path()`
that walks the chain -- so `Job` and `Dataset` build their identifiers from it
instead of hand-joining strings.
"""

from __future__ import annotations

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


def unique_uri(scheme: str, namespace: str | None, name: str) -> str:
    """A globally unique id for one namespace+name pair: `scheme://namespace/name`.

    The one place every lineage resource builds its identifier from --
    `Job.uri()` and `Dataset.uri()` both call this, `scheme` naming the
    resource kind (`job`, `dataset`), never a storage protocol -- so a job
    and a dataset sharing a name in the same namespace can never collide.
    `namespace` may itself be a dotted path (`Namespace(...).path()`); it is
    just another level, split on `.` and rejoined with `/`.
    """
    levels = [*namespace.split("."), name] if namespace else [name]
    return f"{scheme}://{Namespace.of(*levels).path(separator='/')}"
