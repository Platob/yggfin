"""ParsedMessage's own contract; the parser that fills it is tested in jobs/."""

import datetime

from rekep.models import ParsedMessage

EXPECTED_COLUMNS = ["url", "unix", "date", "hash64", "protocol", "fields"]


def test_columns_in_declaration_order() -> None:
    assert ParsedMessage.into_arrow_schema().names == EXPECTED_COLUMNS


def test_protocol_is_the_only_nullable_column() -> None:
    schema = ParsedMessage.into_arrow_schema()
    nullable = [field.name for field in schema if field.nullable]
    assert nullable == ["protocol"]


def test_every_column_is_documented() -> None:
    schema = ParsedMessage.into_arrow_schema()
    for field in schema:
        description = (field.metadata or {}).get(b"description", b"").decode()
        assert description, f"{field.name} has no description"
        assert "\n" not in description, f"{field.name} description is not one line"


def test_unix_and_hash64_are_the_primary_key() -> None:
    """A composite key: the hash identifies the line, `unix` leads so an
    engine can prune on it -- it correlates with the partition."""
    schema = ParsedMessage.into_iceberg_schema()
    assert schema.identifier_field_names() == {"unix", "hash64"}
    assert ParsedMessage.primary_keys() == ["unix", "hash64"], "declaration order"


def test_date_is_the_partition() -> None:
    metadata = ParsedMessage.into_arrow_schema().field("date").metadata
    assert metadata[b"iceberg:partition_key"] == b"identity"


def test_fields_is_a_string_to_string_map() -> None:
    import pyarrow

    field_type = ParsedMessage.into_arrow_schema().field("fields").type
    assert pyarrow.types.is_map(field_type)
    assert field_type.key_type == pyarrow.string()
    assert field_type.item_type == pyarrow.string()
    assert not field_type.item_field.nullable, "a dict[str, str] value is never None"


def test_a_row_round_trips_as_a_record() -> None:
    row = ParsedMessage(
        url="a.txt",
        unix=2,
        date=datetime.date(2026, 8, 14),
        hash64=3,
        protocol="FIX.4.4",
        fields={"9": "112", "35": "D"},
    )
    assert ParsedMessage.from_json(row.into_json()) == row


def test_protocol_and_fields_default_to_empty() -> None:
    row = ParsedMessage(url="a.txt", unix=1, date=datetime.date(2026, 8, 14), hash64=1)
    assert row.protocol is None
    assert row.fields == {}
