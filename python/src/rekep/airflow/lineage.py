"""What a record contributes to an Airflow DAG: an asset, and its metadata.

Nothing here imports Airflow except `asset_of`, so the metadata a DAG carries
can be built, tested and inspected without an Airflow install.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from rekep.namespace import SCHEME, ResourceUri
from rekep.records.annotations import docstring_summary
from rekep.records.record import Record

#: The service an asset's URI names. Airflow treats a URI as opaque, so the
#: only requirement is that it is stable -- rename a record and you have
#: renamed the asset, which is exactly the lineage break it represents -- but
#: it is spelled through `ResourceUri` anyway: a string that looks like one of
#: ours and is not would be worse than either.
ASSET_SERVICE = "records"

#: Tag key marking a dag as generated from rekep declarations, whatever else
#: it carries. A mapping needs the key to say what the value means, so the
#: bare `rekep` tag a list would have carried becomes `generator=rekep`.
GENERATOR_KEY = "generator"

Records = Sequence[type[Record]]


def asset_uri(record: type[Record]) -> str:
    """Stable URI for the data product `record` describes.

    `rekep:///records/<module>.<Class>` -- the same three-slash form every
    other identity here has, built by the same formatter. A record is a
    schema rather than a declared resource, so it names itself by the one
    thing that does identify it: where the class lives.
    """
    return str(ResourceUri.of(ASSET_SERVICE, f"{record.__module__}.{record.__qualname__}"))


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


def tags_of(consumes: Records, produces: Records) -> dict[str, str]:
    """Tags that make a dag findable by the records it touches.

    Keyed by record, valued by what the dag does with it. A mapping says
    *why* a record tags this dag, which a bare list of names never could --
    and a record both read and written gets one tag saying both, rather than
    appearing twice or, worse, once.
    """
    tags = {GENERATOR_KEY: SCHEME}
    for records, direction in ((consumes, "consumes"), (produces, "produces")):
        for entry in records:
            name = asset_name(entry)
            declared = tags.get(name)
            tags[name] = f"{declared}, {direction}" if declared else direction
    return dict(sorted(tags.items()))


def airflow_tags(tags: Mapping[str, str]) -> list[str]:
    """A tag mapping as Airflow wants it: one flat string per entry.

    Airflow's own tags are a list of opaque strings, so the mapping is
    flattened at that boundary and nowhere else -- `key=value`, or the bare
    key when there is no value to state. Everything upstream of this line
    keeps the mapping, which is what makes two declarations of the same tag
    one decision instead of two entries.
    """
    return [f"{key}={value}" if value else key for key, value in sorted(tags.items())]


def documentation_of(consumes: Records, produces: Records) -> str:
    """Markdown lineage table, rendered into the dag or task docs."""
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


def dag_arguments(consumes: Records, produces: Records, **kwargs: Any) -> dict[str, Any]:
    """`kwargs` for Airflow's `DAG`, with tags and docs derived from records.

    The caller's own tags and docs are added to, never replaced: lineage is
    added information, and silently dropping an argument someone wrote is a
    debugging session waiting to happen. A key the caller declares itself
    wins over the derived one -- an explicit declaration is a decision, and
    a derivation is a default.

    `tags` arrives as a mapping (a rekep `Dag`'s or `Job`'s own) and leaves
    as Airflow's list of strings: this is the boundary, so nothing above it
    has to think in flattened tags.
    """
    merged = dict(kwargs)
    tags = dict(merged.pop("tags", None) or {})
    if consumes or produces:
        tags = {**tags_of(consumes, produces), **tags}
        written = documentation_of(consumes, produces)
        existing = merged.get("doc_md")
        merged["doc_md"] = f"{existing}\n\n{written}" if existing else written
    if tags:
        merged["tags"] = airflow_tags(tags)
    return merged


def task_arguments(consumes: Records, produces: Records, **kwargs: Any) -> dict[str, Any]:
    """`kwargs` for Airflow's `@task`: the docs, plus inlets and outlets.

    The assets are what let Airflow draw the graph -- an outlet for every
    record produced, an inlet for every one consumed, each carrying the
    record's schema as asset extras.

    Tags are dropped rather than passed on: Airflow tags a *dag*, and an
    operator handed an argument it does not know refuses to parse, so a
    task's kwargs are the dag's minus the one that only a dag can take.
    """
    merged = dag_arguments(consumes, produces, **kwargs)
    merged.pop("tags", None)
    if not consumes and not produces:
        return merged
    for key, records in (("inlets", consumes), ("outlets", produces)):
        if records:
            merged[key] = [*merged.get(key, ()), *(asset_of(record) for record in records)]
    return merged
