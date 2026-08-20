"""Iceberg: the table as a dataset, and the field projection behind it."""

from rekep.iceberg.dataset import IcebergDataset
from rekep.iceberg.fields import (
    iceberg_field,
    iceberg_partition_spec,
    iceberg_schema,
    struct_field_of,
)

__all__ = [
    "IcebergDataset",
    "iceberg_field",
    "iceberg_partition_spec",
    "iceberg_schema",
    "struct_field_of",
]
