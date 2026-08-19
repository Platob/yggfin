"""Records as OpenLineage datasets: schema and documentation facets, derived.

A record's Arrow schema and docstring are the one authority on what it is
(house rule #7); this only re-projects them into the facets OpenLineage
expects -- the same move `rekep.airflow.lineage` makes for Airflow assets,
just towards a different consumer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from rekep.records.annotations import docstring_summary
from rekep.records.record import Record
from rekep.require import require

if TYPE_CHECKING:  # pragma: no cover - openlineage is imported at the point of use
    from openlineage.client.event_v2 import InputDataset, OutputDataset


def namespace_of(record: type[Record]) -> str:
    """The dataset namespace: the record's module -- stable across a rename of the class."""
    return record.__module__


def dataset_of(
    record: type[Record], kind: Literal["input", "output"]
) -> InputDataset | OutputDataset:
    """`record` as an OpenLineage dataset, its schema and docs carried as facets.

    `kind` picks `InputDataset` or `OutputDataset` -- OpenLineage gives each a
    distinct type even though the payload here is identical either way.
    """
    require("openlineage.client", "openlineage")
    from openlineage.client import event_v2
    from openlineage.client.facet_v2 import documentation_dataset, schema_dataset

    facets: dict[str, Any] = {
        "schema": schema_dataset.SchemaDatasetFacet(fields=_fields(record, schema_dataset))
    }
    summary = docstring_summary(record)
    if summary:
        facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=summary
        )

    cls = event_v2.InputDataset if kind == "input" else event_v2.OutputDataset
    return cls(namespace=namespace_of(record), name=record.__qualname__, facets=facets)


def _fields(record: type[Record], schema_dataset: Any) -> list[Any]:
    return [
        schema_dataset.SchemaDatasetFacetFields(
            name=field.name,
            type=str(field.type),
            description=_description(field),
            ordinal_position=position,
        )
        for position, field in enumerate(record.into_arrow_schema(), start=1)
    ]


def _description(field: Any) -> str | None:
    description = (field.metadata or {}).get(b"description")
    return description.decode("utf-8") if description is not None else None
