"""Projecting a record onto Iceberg DDL."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import ClassVar

import pyarrow

from rekep.records.arrow import PARTITION_KEY, PRIMARY_KEY, ArrowFieldBuilder

#: Metadata written by the Arrow projection that the DDL reads back.
DESCRIPTION = b"description"

#: Spark SQL spellings of Iceberg partition transforms that are not already
#: plain function calls.
TRANSFORM_SQL = {"day": "days", "month": "months", "year": "years", "hour": "hours"}


class IcebergDdlBuilder:
    """Renders a record's Arrow fields as a `CREATE TABLE ... USING iceberg`.

    Everything comes off the Arrow schema the record already has: names, SQL
    types, `NOT NULL` from nullability, column comments from the description
    metadata, and default partition columns from `iceberg:partition` field
    metadata (declared with `Arrow(iceberg={"partition": "true"})`).

    Types it cannot express in Iceberg SQL -- durations, unions -- raise
    rather than guess: DDL is a deploy artifact, and a silently wrong column
    type is a migration.
    """

    ARROW_BUILDER: ClassVar[type[ArrowFieldBuilder]] = ArrowFieldBuilder

    # -- entry point --------------------------------------------------------

    def create_table(
        self,
        cls: type,
        table_name: str,
        *,
        if_not_exists: bool = True,
        location: str | None = None,
        partitioned_by: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> str:
        """The CREATE TABLE statement for `cls`, one column per field."""
        schema = self.ARROW_BUILDER().schema(cls)
        columns = ",\n".join(f"    {self.column(field)}" for field in schema)
        exists = "IF NOT EXISTS " if if_not_exists else ""
        lines = [f"CREATE TABLE {exists}{table_name} (", columns, ")", "USING iceberg"]

        keys = self.keys(schema)
        partitions = list(partitioned_by) or self.partitions(schema)
        if partitions:
            lines.append(f"PARTITIONED BY ({', '.join(partitions)})")
        comment = (schema.metadata or {}).get(DESCRIPTION)
        if comment:
            lines.append(f"COMMENT '{self.quote(comment.decode())}'")
        if location:
            lines.append(f"LOCATION '{self.quote(location)}'")
        if properties:
            rendered = ",\n".join(
                f"    '{self.quote(str(key))}' = '{self.quote(str(value))}'"
                for key, value in properties.items()
            )
            lines.append(f"TBLPROPERTIES (\n{rendered}\n)")
        statement = "\n".join(lines) + ";\n"
        if keys:
            # Spark has no PRIMARY KEY clause for Iceberg; identifier fields
            # are set by a follow-up ALTER, emitted into the same file.
            fields = ", ".join(keys)
            statement += f"\nALTER TABLE {table_name} SET IDENTIFIER FIELDS {fields};\n"
        return statement

    # -- pieces -------------------------------------------------------------

    def column(self, field: pyarrow.Field) -> str:
        """`name TYPE NOT NULL COMMENT '...'`, as much of it as applies."""
        parts = [field.name, self.sql_type(field.type)]
        if not field.nullable:
            parts.append("NOT NULL")
        description = (field.metadata or {}).get(DESCRIPTION)
        if description:
            parts.append(f"COMMENT '{self.quote(description.decode())}'")
        return " ".join(parts)

    def partitions(self, schema: pyarrow.Schema) -> list[str]:
        """Partition clauses from field metadata, transforms spelled for SQL."""
        clauses = []
        for field in schema:
            transform = (field.metadata or {}).get(PARTITION_KEY, b"").decode()
            if transform:
                clauses.append(self.partition_sql(field.name, transform))
        return clauses

    def partition_sql(self, name: str, transform: str) -> str:
        """`("ts", "day")` -> `days(ts)`; identity stays the bare column."""
        if transform in ("identity", "true", "1", "yes"):
            return name
        function, _, width = transform.partition("[")
        if width:  # bucket[16] -> bucket(16, name)
            return f"{function}({width.rstrip(']')}, {name})"
        return f"{TRANSFORM_SQL.get(transform, transform)}({name})"

    def keys(self, schema: pyarrow.Schema) -> list[str]:
        """Fields declared part of the primary key, in schema order."""
        return [field.name for field in schema if (field.metadata or {}).get(PRIMARY_KEY)]

    def sql_type(self, data_type: pyarrow.DataType) -> str:
        """Iceberg SQL spelling of one Arrow type, recursing into containers."""
        types = pyarrow.types
        if types.is_boolean(data_type):
            return "BOOLEAN"
        if types.is_integer(data_type):
            return "BIGINT" if data_type.bit_width > 32 else "INT"
        if types.is_float32(data_type):
            return "FLOAT"
        if types.is_float64(data_type):
            return "DOUBLE"
        if types.is_decimal(data_type):
            return f"DECIMAL({data_type.precision}, {data_type.scale})"
        if types.is_string(data_type) or types.is_large_string(data_type):
            return "STRING"
        if types.is_binary(data_type) or types.is_large_binary(data_type):
            return "BINARY"
        if types.is_date(data_type):
            return "DATE"
        if types.is_timestamp(data_type):
            return "TIMESTAMP"
        if types.is_time(data_type):
            return "TIME"
        if types.is_list(data_type) or types.is_large_list(data_type):
            return f"ARRAY<{self.sql_type(data_type.value_type)}>"
        if types.is_map(data_type):
            return f"MAP<{self.sql_type(data_type.key_type)}, {self.sql_type(data_type.item_type)}>"
        if types.is_struct(data_type):
            fields = ", ".join(
                f"{data_type.field(i).name}: {self.sql_type(data_type.field(i).type)}"
                for i in range(data_type.num_fields)
            )
            return f"STRUCT<{fields}>"
        raise TypeError(f"no Iceberg SQL type for Arrow {data_type}; override sql_type to map it")

    def quote(self, text: str) -> str:
        """Escape a value for a single-quoted SQL string."""
        return text.replace("'", "''")
