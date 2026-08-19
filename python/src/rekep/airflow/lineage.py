"""What a record contributes to an Airflow DAG: an asset, and its metadata.

Nothing here imports Airflow except `asset_of`, so the metadata a DAG carries
can be built, tested and inspected without an Airflow install.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from rekep.records.annotations import docstring_summary
from rekep.records.record import Record

#: URI scheme for assets this package defines. Airflow treats a URI as opaque,
#: so the only requirement is that it is stable: rename a record and you have
#: renamed the asset, which is exactly the lineage break it represents.
ASSET_SCHEME = "rekep"

Records = Sequence[type[Record]]


def asset_uri(record: type[Record]) -> str:
    """Stable URI for the data product `record` describes."""
    return f"{ASSET_SCHEME}://{record.__module__}.{record.__qualname__}"


def asset_name(record: type[Record]) -> str:
    """Short name for the asset, as it appears in the Airflow graph."""
    return record.__qualname__


def asset_of(record: type[Record]) -> Any:
    """The Airflow asset standing for `record`, carrying its schema as extras."""
    from rekep.airflow.sdk import asset

    return asset(asset_name(record), asset_uri(record), metadata_of(record))


def metadata_of(record: type[Record]) -> dict[str, str]:
    """Everything about `record` worth carrying alongside a run.

    The field list is flattened to one string rather than nested: Airflow
    renders asset extras as a flat table, and a nested structure there is
    unreadable.
    """
    from rekep import __version__

    return {
        "record": f"{record.__module__}.{record.__qualname__}",
        "description": docstring_summary(record),
        "fields": ", ".join(f"{f.name}: {f.type}" for f in record.into_arrow_schema()),
        "rekep_version": __version__,
    }


def tags_of(consumes: Records, produces: Records) -> list[str]:
    """Tags that make a DAG findable by the records it touches."""
    return sorted({ASSET_SCHEME, *(asset_name(r) for r in (*consumes, *produces))})


def documentation_of(consumes: Records, produces: Records) -> str:
    """Markdown lineage table, rendered into the DAG or task docs."""
    sections = [
        section
        for label, records in (("Consumes", consumes), ("Produces", produces))
        if (section := _section(label, records))
    ]
    return "\n\n".join(sections)


def _section(label: str, records: Records) -> str:
    if not records:
        return ""
    rows = "\n".join(
        f"| `{asset_name(r)}` | {docstring_summary(r) or ''} | `{asset_uri(r)}` |" for r in records
    )
    return f"### {label}\n\n| Record | Description | Asset |\n| --- | --- | --- |\n{rows}"
