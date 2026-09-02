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
    # By member name: a packed ASCII code is a nineteen-digit integer, and
    # these documents are read and edited by hand.
    assert built.metadata["fix:event_types"] == '{"D":"ORDER"}'
    assert built.fix.event_types == {"D": EventType.ORDER}
    built.fix.states = {"D": State.PENDING_NEW}
    assert built.metadata["fix:states"] == '{"D":"PENDING_NEW"}'
    assert built.fix.states == {"D": State.PENDING_NEW}

    # A stored spelling no member has raises rather than degrading to UNKNOWN.
    with pytest.raises(ValueError, match="unknown EventType"):
        built.fix.event_types = {"D": "invented"}
    with pytest.raises(ValueError, match="unknown State"):
        built.fix.states = {"D": 999}


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
    record = registry.field("Side")

    assert side.fix.spellings()[0] == "Side"
    assert side.fix.meaning("1") == record.fix.meaning("1")
    assert side.fix.meaning("Buy") == "Buy"
    assert side.fix.encode("Buy") == record.fix.encode("Buy") == "1"
    assert side.fix.decode("1") == record.fix.decode("1") == "Buy"
    assert side.fix.declares("4.4") and not side.fix.declares("9.9")
    assert msg_type.fix.event_type("D") is registry.field("MsgType").fix.event_type("D")
    assert msg_type.fix.event_type("nothing-declares-this").name == "UNKNOWN"


def test_fix_value_codecs_apply_the_same_mapping_to_arrow_columns() -> None:
    from rekep.fix import FixRegistry

    side = FixRegistry.from_builtin().field("Side").fix
    source = pyarrow.chunked_array([["Buy", "2"], [None, "future"]])
    encoded = side.arrow_encode(source)
    assert encoded.to_pylist() == ["1", "2", None, "future"]
    assert side.arrow_decode(encoded).to_pylist() == ["Buy", "Sell", None, "future"]


def test_an_identity_fix_value_mapping_skips_arrow_work() -> None:
    built = make_field()
    built.fix.enumerated = {"A": "A"}
    source = pyarrow.array(["A", None])
    assert built.fix.arrow_encode(source) is source
    assert built.fix.arrow_decode(source) is source


# -- one identity read twice -------------------------------------------------


def _reading(name: str, tag: int, **fix: str) -> Field:
    built = Field(name=name.lower(), dtype=pyarrow.string(), nullable=True)
    built.fix.name = name
    built.fix.tag = tag
    built.fix.type = "String"
    for key, value in fix.items():
        built.fix[key] = value
    return built


def test_merge_unions_the_spellings_tags_and_values_of_one_identity() -> None:
    """A venue's own tag for a standard field is another slot to read it at,
    and its own name another spelling -- neither replaces what is held."""
    held = _reading("SettlDate", 64)
    held.fix.versions = ("4.4", "5.0")
    held.fix.sources = ("fix-latest",)
    held.fix.enumerated = {"0": "Regular"}
    other = _reading("TradeDate", 5020)
    other.fix.versions = ("4.2",)
    other.fix.sources = ("venue",)
    other.fix.enumerated = {"9": "Venue"}

    held.fix.merge(other.fix, source="venue")

    assert held.fix.tag == 64, "the canonical tag does not move"
    assert held.fix.tag_priority == (64, 5020)
    assert held.fix.spellings() == ("SettlDate", "TradeDate")
    assert [(one.name, one.source) for one in held.fix.named_aliases] == [("TradeDate", "venue")]
    assert held.fix.meanings == {"0": "Regular", "9": "Venue"}
    assert held.fix.sources == ("fix-latest", "venue")
    assert set(held.fix.versions) == {"4.4", "5.0", "4.2"}


def test_merge_keeps_the_datatype_spelling_it_holds_and_never_compares_it() -> None:
    """`String` against `Qty` is two dictionaries writing one datatype, not a
    disagreement. The stored word is descriptive, so the held one survives and
    only a record carrying none takes the incoming spelling."""
    held = _reading("MaxShow", 210)
    other = _reading("TradeType", 9001)
    other.fix.type = "Qty"

    held.fix.merge(other.fix)

    assert held.fix.type == "String"
    assert held.fix.tag_priority == (210, 9001)

    unspelled = _reading("MaxShow", 210)
    del unspelled.fix["type"]
    unspelled.fix.merge(other.fix)

    assert unspelled.fix.type == "Qty", "nothing held, so the reading given fills it"


def test_merge_keeps_the_first_provenance_of_a_spelling() -> None:
    held = _reading("SettlDate", 64)
    held.fix.named_aliases = [{"name": "FutSettDate", "source": "4.0", "occurrences": 3}]
    other = _reading("FutSettDate", 5020)

    held.fix.merge(other.fix, source="venue")

    assert [(one.name, one.source, one.occurrences) for one in held.fix.named_aliases] == [
        ("FutSettDate", "4.0", 3)
    ], "a later sighting of a known spelling says nothing new about where it came from"


def test_field_merge_adds_to_the_spellings_it_does_not_replace_them() -> None:
    """The overlay case: a declaration naming a type must not silence the
    aliases the registry gathered for the same identity."""
    registry = _reading("SettlDate", 64)
    registry.fix.named_aliases = [{"name": "FutSettDate", "source": "4.0"}]
    override = _reading("SettlDate", 64)
    override.fix.type = "UTCDateOnly"
    override.fix.named_aliases = [{"name": "TradeDate", "source": "venue"}]

    merged = registry.merge(override)

    assert merged.fix.type == "UTCDateOnly", "the later declaration still wins a scalar"
    assert merged.fix.spellings() == ("SettlDate", "TradeDate", "FutSettDate")
