"""Iceberg: catalogs, tables as datasets, and the field projection behind them."""

from rekep.iceberg.catalog import IcebergCatalog, IcebergNamespace
from rekep.iceberg.dataset import IcebergDataset
from rekep.iceberg.fields import (
    CONTRACT_KEYS,
    derived_keys,
    iceberg_contract,
    iceberg_contract_field,
    iceberg_field,
    iceberg_partition_spec,
    iceberg_schema,
    iceberg_sort_order,
    iceberg_struct_field,
    metrics_for,
    partition_keys,
    primary_keys,
    sort_keys,
)

__all__ = [
    "CONTRACT_KEYS",
    "IcebergCatalog",
    "IcebergDataset",
    "IcebergNamespace",
    "derived_keys",
    "iceberg_contract",
    "iceberg_contract_field",
    "iceberg_field",
    "iceberg_partition_spec",
    "iceberg_schema",
    "iceberg_sort_order",
    "iceberg_struct_field",
    "metrics_for",
    "partition_keys",
    "primary_keys",
    "sort_keys",
]
