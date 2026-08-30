"""The tables a pipeline run writes, created before anything runs.

A notebook creates its own target on the first write, so a run against an
empty catalog already lands every table it needs. That is not enough where the
catalog is not the runner's to write to: a Glue catalog over an S3 warehouse
is deployed once, by whoever owns the account, ahead of the jobs that fill it.
So the layout is declared here rather than discovered from a run -- and both
paths still create the same table, because `create_with_field` is the one
place a table is made.

Declaring it here also gives the layout a single reader: the tables, the shape
each one carries and the columns each is laid out by, in the order a run
fills them.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import Any

from rekep.fields import StructField
from rekep.fix.rules import MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY
from rekep.iceberg import IcebergCatalog
from rekep.market import Book, Execution, InstrumentUpdate, Order
from rekep.text import FixMsg, Message

#: Event files follow their byte-ordered identity; its leading clock preserves
#: event order while keeping one declared sort key in every storage engine.
EVENT_SORT: tuple[str, ...] = ("hash",)

#: Parsed FIX tables use the same physical order. A time-ordered consumer asks
#: for `(unix, msgseqnum, hash)` explicitly and the bounded reader supplies it.
FIX_SORT: tuple[str, ...] = EVENT_SORT


@dataclasses.dataclass(frozen=True)
class Deployed:
    """One table the pipeline writes: what it is called and what it holds."""

    #: Catalog table identifier, `namespace.table`.
    table: str

    #: The declaring class. Its `into_field` is what the notebook writing this
    #: table builds its own schema from, so a deployed table and a written one
    #: cannot disagree.
    shape: type

    #: Columns the table records a sort order on. None leaves the shape's own
    #: `sort_key()` declarations, which is what a reference table wants.
    sort_by: tuple[str, ...] | None = EVENT_SORT

    def into_field(self) -> StructField:
        """The shape this table carries, named as the table."""
        return self.shape.into_field(self.table)


#: Every table the five pipeline tasks write, in the order they fill them.
#: The `fix.*` names are the router's own categories, so a category added
#: there arrives here rather than being spelled twice.
TABLES: tuple[Deployed, ...] = (
    Deployed("logs.messages", Message),
    Deployed(f"fix.{MARKET_CATEGORY}", FixMsg, FIX_SORT),
    Deployed(f"fix.{MISC_CATEGORY}", FixMsg, FIX_SORT),
    Deployed(f"fix.{UNKNOWN_CATEGORY}", FixMsg, FIX_SORT),
    Deployed("market.instruments", InstrumentUpdate, None),
    Deployed("market.books", Book),
    Deployed("market.orders", Order),
    Deployed("market.executions", Execution),
)


def deploy(
    catalog: str = "default",
    *,
    properties: dict[str, str] | None = None,
    table_properties: dict[str, str] | None = None,
    branch: str | None = None,
    tables: Sequence[str] | None = None,
    dry_run: bool = False,
) -> dict[str, str]:
    """Create the declared tables, and say what each one was: what it did.

    Idempotent, and deliberately so in one direction only: a table already in
    the catalog is left exactly as it is, properties included. Redeploying is
    how a deployment is checked, not how one is rewritten -- `optimize` owns
    retrofitting properties onto a table that already holds rows.

    Returns `created`, `present` or, under `dry_run`, `missing` per table.
    """
    declared = {shape.table: shape for shape in TABLES}
    wanted = list(declared) if tables is None else list(dict.fromkeys(tables))
    unknown = [name for name in wanted if name not in declared]
    if unknown:
        raise ValueError(
            f"the pipeline writes no such table: {', '.join(unknown)}; "
            f"it writes {', '.join(declared)}"
        )
    store = IcebergCatalog(name=catalog, properties=dict(properties or {}))
    try:
        return {
            name: _deployed(store, declared[name], table_properties, branch, dry_run)
            for name in wanted
        }
    finally:
        store.close()


def _deployed(
    store: IcebergCatalog,
    shape: Deployed,
    table_properties: dict[str, str] | None,
    branch: str | None,
    dry_run: bool,
) -> str:
    """One table, through the catalog handle the whole deployment shares."""
    dataset: Any = store.dataset(
        shape.table,
        field=shape.into_field(),
        table_properties=dict(table_properties or {}),
        branch=branch,
        sort_by=shape.sort_by,
    )
    if dataset.exists:
        return "present"
    if dry_run:
        return "missing"
    dataset.create_with_field(dataset.field)
    return "created"


__all__ = [
    "EVENT_SORT",
    "FIX_SORT",
    "TABLES",
    "Deployed",
    "deploy",
]
