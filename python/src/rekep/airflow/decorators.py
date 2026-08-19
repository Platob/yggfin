"""`dag` and `task`, with data lineage declared by record.

Same names, same call shapes as Airflow's own decorators, plus `consumes` and
`produces` -- lists of `Record` classes. From those it fills in what Airflow
needs to draw the lineage graph:

- the DAG is tagged with every record it touches and documented with a
  Consumes/Produces table,
- a task's `outlets` become the assets standing for the records it produces,
  its `inlets` the ones it consumes, each carrying the record's schema and
  description as asset extras.

Everything else is passed straight through, so anything Airflow's decorators
accept is accepted here.
"""

from __future__ import annotations

from typing import Any

from rekep.airflow import lineage
from rekep.airflow.lineage import Records


def dag(*args: Any, consumes: Records = (), produces: Records = (), **kwargs: Any) -> Any:
    """Airflow's `@dag`, with lineage tags and docs derived from records."""
    from rekep.airflow import sdk

    kwargs = _with_lineage(kwargs, consumes, produces, docs_key="doc_md")
    return sdk.dag(*args, **kwargs)


def task(*args: Any, consumes: Records = (), produces: Records = (), **kwargs: Any) -> Any:
    """Airflow's `@task`, with inlets and outlets derived from records.

    Works bare (`@task`) and called (`@task(consumes=[Log])`), like the
    decorator it wraps.
    """
    from rekep.airflow import sdk

    if args and callable(args[0]) and not kwargs and not consumes and not produces:
        return sdk.task(args[0])

    kwargs = _with_assets(kwargs, consumes, produces)
    kwargs = _with_lineage(kwargs, consumes, produces, docs_key="doc_md")
    return sdk.task(*args, **kwargs)


class DAG:
    """Airflow's `DAG`, constructed with lineage derived from records.

    A class standing in for a class: `with DAG(...)` and `DAG(...)` both build
    a plain Airflow DAG, so everything downstream -- the scheduler, the
    context manager protocol, serialisation -- sees the real thing.
    """

    def __new__(
        cls, *args: Any, consumes: Records = (), produces: Records = (), **kwargs: Any
    ) -> Any:
        from rekep.airflow import sdk

        return sdk.DAG(*args, **_with_lineage(kwargs, consumes, produces, docs_key="doc_md"))


def _with_lineage(
    kwargs: dict[str, Any], consumes: Records, produces: Records, *, docs_key: str
) -> dict[str, Any]:
    """Merge derived tags and docs into `kwargs`, keeping what the caller wrote.

    The caller's own tags and docs are appended to, never replaced: lineage is
    added information, and a decorator that silently drops an argument is a
    debugging session waiting to happen.
    """
    if not consumes and not produces:
        return kwargs
    merged = dict(kwargs)
    merged["tags"] = sorted({*merged.get("tags", ()), *lineage.tags_of(consumes, produces)})
    documentation = lineage.documentation_of(consumes, produces)
    existing = merged.get(docs_key)
    merged[docs_key] = f"{existing}\n\n{documentation}" if existing else documentation
    return merged


def _with_assets(kwargs: dict[str, Any], consumes: Records, produces: Records) -> dict[str, Any]:
    """Merge record assets into `inlets`/`outlets`, keeping what the caller wrote."""
    if not consumes and not produces:
        return kwargs
    merged = dict(kwargs)
    for key, records in (("inlets", consumes), ("outlets", produces)):
        if records:
            merged[key] = [*merged.get(key, ()), *(lineage.asset_of(r) for r in records)]
    return merged
