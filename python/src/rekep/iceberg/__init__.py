"""Iceberg: catalogs, tables as datasets, and the field projection behind them."""

from rekep.iceberg.catalog import IcebergCatalog, IcebergNamespace
from rekep.iceberg.dataset import IcebergDataset
from rekep.iceberg.fields import (
    iceberg_field,
    iceberg_partition_spec,
    iceberg_schema,
    iceberg_sort_order,
    metrics_for,
)

__all__ = [
    "IcebergCatalog",
    "IcebergDataset",
    "IcebergNamespace",
    "iceberg_field",
    "iceberg_partition_spec",
    "iceberg_schema",
    "iceberg_sort_order",
    "metrics_for",
]
