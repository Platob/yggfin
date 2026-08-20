"""The record machinery: dataclasses that are data products."""

from rekep.records.arrow import (
    Arrow,
    ArrowFieldBuilder,
    ArrowRecordBuilder,
    cast_batch,
    cast_reader,
    partition_keys,
    primary_keys,
)
from rekep.records.ddl import IcebergDdlBuilder
from rekep.records.doris import (
    DorisCatalog,
    DorisDdlBuilder,
    DorisDeployment,
    DorisNamespace,
    DorisTable,
)
from rekep.records.iceberg import (
    IcebergCatalog,
    IcebergDeployment,
    IcebergFieldBuilder,
    IcebergNamespace,
    IcebergTable,
)
from rekep.records.record import Record, record

__all__ = [
    "Arrow",
    "ArrowFieldBuilder",
    "ArrowRecordBuilder",
    "DorisCatalog",
    "DorisDdlBuilder",
    "DorisDeployment",
    "DorisNamespace",
    "DorisTable",
    "IcebergCatalog",
    "IcebergDdlBuilder",
    "IcebergDeployment",
    "IcebergFieldBuilder",
    "IcebergNamespace",
    "IcebergTable",
    "Record",
    "cast_batch",
    "cast_reader",
    "partition_keys",
    "primary_keys",
    "record",
]
