"""What a job declares about a field, and what it reaches.

A rule is a document, so every one of these builds it the way a task YAML
would -- through `from_dict` -- rather than by constructing the dataclass.
"""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fix import FieldRule, FieldRules, FixCodec, FixRegistry
from rekep.text import TextFile

#: One order, spelled the way a bridge prints one, with a vendor tag on it.
LINE = (
    "8=FIX.4.4|9=100|35=D|11=CL-1|55=AAPL|54=Buy|38=10|44=1.5|"
    "60=20260821-10:00:00.123|9999=20260822-11:00:00|10=001"
)


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    return FixRegistry.from_builtin()


def codec_of(registry: FixRegistry, declared: dict | None = None) -> FixCodec:
    return FixCodec(
        registry=registry,
        fields=FieldRules() if declared is None else FieldRules.from_dict(declared),
    )


def lifted(codec: FixCodec, line: str = LINE) -> dict:
    column = pyarrow.array([line])
    _, columns = codec.into_fixmessage_columns(codec.into_pairs(column, "FIX"), "4.4")
    return columns


def resolved(codec: FixCodec, line: str = LINE) -> list[tuple[int, str]]:
    column = pyarrow.array([line])
    done = codec.complete_kwargs(codec.into_message_columns(column)["kwargs"], "4.4")
    return [(one["tag"], one["value"]) for one in done.to_pylist()[0]]


# -- the type a field reads as -----------------------------------------------


def test_a_declared_type_changes_how_the_text_is_read(registry: FixRegistry) -> None:
    """`TransactTime` read as a day is that day's midnight, not its clock."""
    plain = lifted(codec_of(registry))["transact_time"].to_pylist()[0]
    assert plain.hour == 10
    declared = {"rules": [{"field": "TransactTime", "type": "date32[day]"}]}
    dated = lifted(codec_of(registry, declared))["transact_time"].to_pylist()[0]
    assert (dated.hour, dated.minute, dated.day) == (0, 0, 21)


def test_a_rule_may_name_its_field_by_tag_or_by_name(registry: FixRegistry) -> None:
    by_tag = codec_of(registry, {"rules": [{"field": "60", "type": "date32[day]"}]})
    by_name = codec_of(registry, {"rules": [{"field": "TransactTime", "type": "date32[day]"}]})
    assert lifted(by_tag)["transact_time"] == lifted(by_name)["transact_time"]


def test_a_declared_reading_that_leaves_text_still_lands_in_its_column(
    registry: FixRegistry,
) -> None:
    """The column keeps its contract type, so the second cast must not raise."""
    declared = {"rules": [{"field": "60", "type": "string"}]}
    found = lifted(codec_of(registry, declared))["transact_time"]
    assert found.to_pylist()[0].hour == 10


def test_a_tag_the_dictionary_does_not_number_reads_as_declared(registry: FixRegistry) -> None:
    """Which is the whole point: a vendor tag no dictionary will ever carry."""
    codec = codec_of(registry, {"rules": [{"field": "9999", "type": "utctimestamp"}]})
    field = codec.tag_field(9999, "4.4")
    assert field is not None and pyarrow.types.is_timestamp(field.arrow_type)
    assert codec_of(registry).tag_field(9999, "4.4") is None


def test_a_type_that_is_neither_arrow_nor_fix_is_refused() -> None:
    with pytest.raises(ValueError, match="neither an Arrow type nor a FIX datatype"):
        FieldRule(field="Side", type="whatever")


def test_a_rule_that_names_no_field_is_refused() -> None:
    with pytest.raises(ValueError, match="names no field"):
        FieldRule(field="")


# -- the values a field reads --------------------------------------------------


def test_a_declared_spelling_is_translated_like_the_dictionary_s(
    registry: FixRegistry,
) -> None:
    declared = {"rules": [{"field": "Side", "values": {"BUYSIDE": "1"}}]}
    line = LINE.replace("54=Buy", "54=BUYSIDE")
    assert (54, "1") in resolved(codec_of(registry, declared), line)
    assert (54, "BUYSIDE") in resolved(codec_of(registry), line)


def test_a_declared_spelling_wins_the_dictionary_s(registry: FixRegistry) -> None:
    """A job knows its own estate; the dictionary is the fallback, not the authority."""
    declared = {"rules": [{"field": "Side", "values": {"Buy": "9"}}]}
    assert (54, "9") in resolved(codec_of(registry, declared))
    assert (54, "1") in resolved(codec_of(registry))


# -- the document ---------------------------------------------------------------


def test_declared_readings_round_trip_through_a_document() -> None:
    rules = FieldRules(
        rules=[
            FieldRule(field="60", type="date32[day]"),
            FieldRule(field="Side", values={"BUYSIDE": "1"}),
        ]
    )
    back = FieldRules.from_dict(rules.into_dict())
    assert [one.field for one in back] == ["60", "Side"]
    assert back.rules[0].arrow_type == pyarrow.date32()
    assert back.rules[1].values == {"BUYSIDE": "1"}


def test_a_codec_carries_its_declared_readings(registry: FixRegistry) -> None:
    codec = codec_of(registry, {"rules": [{"field": "60", "type": "date32[day]"}]})
    assert codec.into_dict()["fields"]["rules"][0]["type"] == "date32[day]"


# -- the header a line opens with ----------------------------------------------

#: A capture whose header is not the one this package ships: a pipe-delimited
#: preamble, which no shipped pattern reads.
VENDOR_HEADER = (
    r"^(?P<timestamp>[0-9]{8}-[0-9:.]+)\|(?P<thread_name>[^|]*)\|"
    r"(?P<plugin_code>[^|]*)\|(?P<message>.*)$"
)


def test_a_job_may_declare_the_header_its_capture_writes(tmp_path) -> None:
    path = tmp_path / "vendor.log"
    path.write_text(f"20260821-10:00:00.123|t-1|VendorBridge|{LINE}\n")
    with TextFile.from_path(path, header_pattern=VENDOR_HEADER, resolved=False) as log:
        rows = log.into_arrow_table()
    assert rows.num_rows == 1
    assert rows.column("plugin_code").to_pylist() == ["VendorBridge"]
    assert rows.column("msg_type").to_pylist() == ["D"]


def test_the_shipped_header_reads_that_capture_as_no_row_at_all(tmp_path) -> None:
    """Which is what makes the declaration load-bearing rather than decoration."""
    path = tmp_path / "vendor.log"
    path.write_text(f"20260821-10:00:00.123|t-1|VendorBridge|{LINE}\n")
    with TextFile.from_path(path, resolved=False) as log:
        assert log.into_arrow_table().num_rows == 0
