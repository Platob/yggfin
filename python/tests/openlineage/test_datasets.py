"""Records as OpenLineage datasets need no live client -- only the facet classes."""

import pytest

pytest.importorskip("openlineage.client")

from rekep.models import Log
from rekep.openlineage import dataset_of, namespace_of


def test_namespace_is_the_records_module() -> None:
    assert namespace_of(Log) == Log.__module__ == "rekep.models.log"


def test_dataset_name_and_namespace() -> None:
    dataset = dataset_of(Log, "input")
    assert dataset.namespace == "rekep.models.log"
    assert dataset.name == "Log"


def test_input_vs_output_dataset_type() -> None:
    from openlineage.client.event_v2 import InputDataset, OutputDataset

    assert isinstance(dataset_of(Log, "input"), InputDataset)
    assert isinstance(dataset_of(Log, "output"), OutputDataset)


def test_schema_facet_carries_every_field_typed() -> None:
    fields = {f.name: f.type for f in dataset_of(Log, "input").facets["schema"].fields}
    assert fields["unix"] == "int64"


def test_schema_facet_fields_are_ordered() -> None:
    fields = dataset_of(Log, "input").facets["schema"].fields
    assert [f.ordinal_position for f in fields] == list(range(1, len(fields) + 1))


def test_field_descriptions_carry_over_when_documented() -> None:
    described = {
        f.name: f.description
        for f in dataset_of(Log, "input").facets["schema"].fields
        if f.description
    }
    assert described, "Log documents at least one field"


def test_documentation_facet_from_the_docstring() -> None:
    dataset = dataset_of(Log, "input")
    assert dataset.facets["documentation"].description == "One parsed line of a trading log."
