"""`ProtocolMetadata`: one protocol's `prefix:key` metadata, read and written in place."""

import pyarrow
import pytest

from rekep import Field, ProtocolMetadata


def make_field() -> Field:
    return Field(
        name="side",
        dtype=pyarrow.string(),
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
    built = Field(name="k", dtype=pyarrow.int64(), nullable=False)
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


# -- the typed views ---------------------------------------------------------


def test_each_known_protocol_answers_with_its_typed_view() -> None:
    from rekep.fields import EnumMetadata, FixMetadata, IcebergMetadata

    built = make_field()
    assert isinstance(built.fix, FixMetadata)
    assert isinstance(built.iceberg, IcebergMetadata)
    assert isinstance(built.enum, EnumMetadata)
    assert isinstance(built.protocol("fix"), FixMetadata)
    assert type(built.protocol("own")) is ProtocolMetadata


def test_fix_metadata_reads_and_writes_typed_values() -> None:
    built = make_field()
    assert built.fix.tag == 54
    assert built.fix.type == "char"
    assert built.fix.version == ""
    built.fix.tag = 55
    assert built.metadata["fix:tag"] == "55"
    built.fix.tag = None
    assert "fix:tag" not in built.metadata
    built.fix.enumerated = {"1": "Buy"}
    assert built.metadata["fix:values"] == '[{"value":"1","meaning":"Buy"}]'
    assert built.fix.meanings == {"1": "Buy"}
    assert built.fix.value_of("1").meaning == "Buy"
    built.fix.versions = ["4.4", "4.2"]
    assert built.fix.versions == ("4.4", "4.2")
    built.fix.versions = ()
    assert "fix:versions" not in built.metadata


def test_fix_metadata_round_trips_the_market_enums() -> None:
    from rekep.enums import EventType, State

    built = make_field()
    built.fix.event_types = {"D": EventType.ORDER}
    assert built.metadata["fix:event_types"] == f'{{"D":{int(EventType.ORDER)}}}'
    assert built.fix.event_types == {"D": EventType.ORDER}
    built.fix.states = {"D": State.PENDING_NEW}
    assert built.fix.states == {"D": State.PENDING_NEW}


def test_iceberg_metadata_carries_the_key_declarations() -> None:
    built = Field(name="k", dtype=pyarrow.int64(), nullable=False)
    built.iceberg.primary_key = True
    assert built.is_primary_key and built.metadata["iceberg:primary_key"] == "true"
    built.iceberg.partition_key = "day"
    assert built.partition_transform == "day"
    built.iceberg.field_id = 7
    assert built.field_id == 7
    with pytest.raises(ValueError, match="from 1"):
        built.iceberg.field_id = 0
    built.iceberg.derived_from = ("unix",)
    assert built.derived_from == ("unix",)
    with pytest.raises(ValueError, match="derived from itself"):
        built.iceberg.derived_from = ("k",)
    nullable = Field(name="n", dtype=pyarrow.int64(), nullable=True)
    with pytest.raises(TypeError, match="cannot be nullable"):
        nullable.iceberg.primary_key = True


def test_a_view_write_mutates_the_original_mapping_in_place() -> None:
    """Zero copies: the dict the field was built over is the dict that grows."""
    built = make_field()
    stored = built.metadata
    built.fix.tag = 55
    built.fix["name"] = "Side"
    del built.fix["type"]
    assert built.metadata is stored
    assert stored == {"fix:tag": "55", "fix:name": "Side", "description": "Side of order."}


def test_an_in_place_write_still_rebuilds_the_container() -> None:
    schema = pyarrow.schema([("side", pyarrow.string())])
    struct = Field.from_arrow_schema(schema, "Row")
    member = struct.field("side")
    member.fix.tag = 54
    member.fix.tag = 55
    rebuilt = struct.into_arrow_schema().field("side").metadata
    assert rebuilt[b"fix:tag"] == b"55"


def test_the_enum_view_reads_a_market_declaration() -> None:
    from rekep.market import Instrument

    currency = Instrument.into_field().field("currency").enum
    assert currency.name == "Currency"
    assert currency.byte_width == 4
    assert currency.encoding == "ascii-big-endian"
    assert currency.members["0"] == "UNKNOWN"
    assert currency.aliases["$"] == "USD"


def test_the_typed_views_stay_the_mapping_they_advertise() -> None:
    """A decoded map never shadows a mapping method: `.values()` still answers."""
    built = make_field()
    assert sorted(built.fix.values()) == ["54", "char"]
    assert sorted(built.enum.values()) == []
    assert built.fix.meanings == {}, "the decoded map answers under its own name"
    assert built.fix.enumerated == (), "and the enumerated values under theirs"


def test_the_fix_view_answers_what_a_registry_record_does() -> None:
    """A Field carrying FIX metadata is the record: the spellings it answers
    to, what a value encodes to, what it means, and which kind it names."""
    from rekep.fix import FixRegistry

    registry = FixRegistry.from_builtin()
    side, msg_type = registry.scalar("Side"), registry.scalar("MsgType")
    record = registry.entry("Side")

    assert side.fix.spellings()[0] == "Side"
    assert side.fix.meaning("1") == record.meaning("1")
    assert side.fix.encode("Buy") == record.encode("Buy") == "1"
    assert not hasattr(side.fix, "decode"), "one direction: the wire value is the fact"
    assert side.fix.declares("4.4") and not side.fix.declares("9.9")
    assert msg_type.fix.event_type("D") is registry.entry("MsgType").event_type("D")
    assert msg_type.fix.event_type("nothing-declares-this").name == "UNKNOWN"
