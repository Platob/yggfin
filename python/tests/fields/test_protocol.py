"""`ProtocolMetadata`: one protocol's `prefix:key` metadata, read and written in place."""

import pyarrow
import pytest

from rekep import Field, ProtocolMetadata


def make_field() -> Field:
    return Field(
        name="side",
        data_type=pyarrow.string(),
        nullable=True,
        metadata={"fix:tag": "54", "fix:type": "char", "description": "Side of order."},
    )


# -- reading -----------------------------------------------------------------


def test_a_get_reads_the_prefixed_key_in_place() -> None:
    built = make_field()
    assert built.fix["tag"] == "54"
    assert built.protocol("fix")["type"] == "char"


def test_two_proxies_over_one_field_always_agree() -> None:
    built = make_field()
    one, other = built.fix, built.fix
    one["version"] = "4.4"
    assert other["version"] == "4.4", "a proxy is a view, not a copy"


def test_unprefixed_keys_are_invisible_to_a_protocol() -> None:
    built = make_field()
    assert sorted(built.fix) == ["tag", "type"]
    assert len(built.fix) == 2
    assert "description" not in built.fix


def test_protocols_do_not_see_each_other() -> None:
    built = make_field()
    built.iceberg["primary_key"] = "true"
    assert "primary_key" not in built.fix
    assert built.metadata["iceberg:primary_key"] == "true"


def test_a_missing_key_raises_with_the_full_name() -> None:
    with pytest.raises(KeyError, match="fix:absent"):
        make_field().fix["absent"]
    assert make_field().fix.get("absent", "x") == "x"


# -- writing -----------------------------------------------------------------


def test_a_set_lands_under_the_prefix_and_coerces_to_text() -> None:
    built = make_field()
    built.fix["tag"] = 55
    assert built.metadata["fix:tag"] == "55"


def test_a_delete_removes_only_its_key() -> None:
    built = make_field()
    del built.fix["type"]
    assert dict(built.fix) == {"tag": "54"}
    assert built.description == "Side of order.", "other metadata is untouched"
    with pytest.raises(KeyError):
        del built.fix["type"]


def test_a_write_through_the_proxy_rebuilds_the_container() -> None:
    """The whole point of going through `metadata`: the struct above notices."""
    schema = pyarrow.schema([("side", pyarrow.string())])
    struct = Field.from_arrow_schema(schema, "Row")
    struct.field("side").fix["tag"] = "54"
    rebuilt = struct.into_arrow_schema().field("side").metadata
    assert rebuilt[b"fix:tag"] == b"54"


def test_the_key_properties_read_through_the_iceberg_protocol() -> None:
    built = Field(name="k", data_type=pyarrow.int64(), nullable=False)
    built.is_primary_key = True
    assert built.iceberg["primary_key"] == "true"
    built.iceberg["partition_key"] = "day"
    assert built.partition_transform == "day"
    assert built.is_partition_key
    built.is_partition_key = False
    assert "partition_key" not in built.iceberg


def test_the_proxy_repr_names_the_prefix() -> None:
    assert "fix" in repr(make_field().fix)
    assert isinstance(make_field().fix, ProtocolMetadata)
