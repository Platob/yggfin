import datetime
from typing import Annotated

import pyarrow
import pytest

from rekep import Arrow, Record, record
from rekep.models import Log
from rekep.records.ddl import IcebergDdlBuilder


@record
class DdlFill(Record):
    """One fill."""

    day: Annotated[datetime.date, Arrow(partition=True)]
    """Trading day."""

    symbol: str
    """Ticker, ISO 10383 style. It's quoted."""

    qty: int
    tags: list[str]
    extra: dict[str, str]
    note: str | None = None


@pytest.fixture(scope="module")
def ddl() -> str:
    return DdlFill.into_iceberg_ddl()


def test_statement_shape(ddl: str) -> None:
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS ddl_fill (")
    assert "USING iceberg" in ddl
    assert ddl.rstrip().endswith(";")


def test_default_table_name_is_snake_case() -> None:
    assert "log (" in Log.into_iceberg_ddl()
    assert "my_fills (" in DdlFill.into_iceberg_ddl("my_fills")


def test_types_map_from_arrow(ddl: str) -> None:
    assert "day DATE NOT NULL" in ddl
    assert "symbol STRING NOT NULL" in ddl
    assert "qty BIGINT NOT NULL" in ddl
    assert "tags ARRAY<STRING> NOT NULL" in ddl
    assert "extra MAP<STRING, STRING> NOT NULL" in ddl


def test_nullable_field_has_no_not_null(ddl: str) -> None:
    (line,) = [line for line in ddl.splitlines() if line.strip().startswith("note ")]
    assert "NOT NULL" not in line


def test_descriptions_become_comments(ddl: str) -> None:
    assert "COMMENT 'Trading day.'" in ddl
    assert "COMMENT 'One fill.'" in ddl


def test_quotes_in_descriptions_are_escaped(ddl: str) -> None:
    assert "It''s quoted." in ddl


def test_partition_comes_from_field_metadata(ddl: str) -> None:
    assert "PARTITIONED BY (day)" in ddl


def test_partition_argument_overrides_metadata() -> None:
    assert "PARTITIONED BY (symbol)" in DdlFill.into_iceberg_ddl(partitioned_by=["symbol"])


def test_location_and_properties() -> None:
    ddl = DdlFill.into_iceberg_ddl(
        location="s3://bucket/fills", properties={"write.format.default": "parquet"}
    )
    assert "LOCATION 's3://bucket/fills'" in ddl
    assert "'write.format.default' = 'parquet'" in ddl


def test_if_not_exists_can_be_dropped() -> None:
    assert "CREATE TABLE ddl_fill (" in DdlFill.into_iceberg_ddl(if_not_exists=False)


def test_unmappable_type_is_refused() -> None:
    @record
    class Weird(Record):
        lag: datetime.timedelta

    with pytest.raises(TypeError, match="no Iceberg SQL type"):
        Weird.into_iceberg_ddl()


def test_builder_is_overridable() -> None:
    class Wider(IcebergDdlBuilder):
        def sql_type(self, data_type: pyarrow.DataType) -> str:
            if pyarrow.types.is_duration(data_type):
                return "BIGINT"
            return super().sql_type(data_type)

    @record
    class Weird(Record):
        DDL_BUILDER = Wider
        lag: datetime.timedelta

    assert "lag BIGINT" in Weird.into_iceberg_ddl()
