"""The message layer end to end: a capture in, categories and tags out."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import FixCodec, FixRegistry, Rule, Rules
from rekep.text import HEADER_PATTERN, TextFile, TextFiles

SOH = "\x01"
SAMPLE = Path(__file__).parent.parent / "data" / "app_messages_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: The dictionary this repository publishes, beside `python/`.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: Derived from the fixture, then pinned, so a regression in `HEADER_PATTERN`
#: cannot move both sides of an assertion together.
RECORDS = [line for line in SAMPLE_BYTES.split(b"\n") if HEADER_PATTERN.match(line)]
CONTINUATIONS = [
    line for line in SAMPLE_BYTES.split(b"\n") if line and not HEADER_PATTERN.match(line)
]
EXPECTED_RECORDS = 10
EXPECTED_CONTINUATIONS = 3

#: Which category each record is, in file order. The whole point of the
#: fixture, so it is spelled out rather than derived from the rules it checks.
EXPECTED_CATEGORIES = [
    "OTHER",
    "OTHER",
    "FIX",
    "FIX",
    "FIX",
    "UL",
    "UL",
    "OTHER",
    "OTHER",
    "OTHER",
]

#: Derived from the bridge line, then pinned: eleven tokens, two of which carry
#: three members each, so a parser that lost one cannot move both sides.
EXPECTED_BRIDGE_PAIRS = 15

#: Row indexes worth naming. `WRAPPED` is a bridge message inside a FIX
#: envelope -- a wire header and a `#NAME=` body on one line, which answers to
#: both tells and so is the one the rule order exists for.
PIPED, CARET, SOHED, BRIDGE, WRAPPED, REJECTED = 2, 3, 4, 5, 6, 7


def test_the_sample_is_the_shape_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS
    assert len(CONTINUATIONS) == EXPECTED_CONTINUATIONS
    assert SAMPLE_BYTES.count(b"\x01") > 0, "the SOH lines are bytes, not four characters"
    assert b"^A9=61" in SAMPLE_BYTES, "and the caret-A line is the two characters"


@pytest.fixture
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True), fix_version="4.4")


@pytest.fixture
def table(codec: FixCodec) -> pyarrow.Table:
    with TextFile.from_path(SAMPLE, codec=codec) as log:
        return log.read_arrow_table()


# -- the categories ----------------------------------------------------------


def test_every_line_lands_in_the_category_the_rules_claim(table: pyarrow.Table) -> None:
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("category_name").to_pylist() == EXPECTED_CATEGORIES
    assert table.column("category_id").to_pylist() == [
        Rules.DEFAULT.rule(0).category_id if name == "OTHER" else {"FIX": 1, "UL": 2}[name]
        for name in EXPECTED_CATEGORIES
    ]


def test_the_column_and_the_line_agree_on_every_row(table: pyarrow.Table) -> None:
    """Scalar and vectorised, column for column, on the capture itself."""
    scalar = [Rules.DEFAULT.categorise(one) for one in table.column("message").to_pylist()]
    assert [rule.category_id for rule in scalar] == table.column("category_id").to_pylist()
    assert [rule.name for rule in scalar] == table.column("category_name").to_pylist()


# -- what a line carries -----------------------------------------------------


def test_a_wire_message_yields_its_tags_and_nothing_around_them(table: pyarrow.Table) -> None:
    """The line has a prefix *and* a suffix, and neither is in the message."""
    tags = dict(table.column("fix_tags")[PIPED].as_py())
    assert tags[8] == "FIX.4.2" and tags[10] == "203"
    assert tags[35] == "D" and tags[55] == "TTF" and tags[44] == "41.2500"
    assert "sending" not in str(tags) and "queued" not in str(tags)
    assert table.column("keyval")[PIPED].as_py() == []


def test_the_caret_and_the_soh_lines_parse_to_the_same_shape(table: pyarrow.Table) -> None:
    """One capture, three separators, and a batch is sampled once by contract."""
    for row in (CARET, SOHED):
        tags = [tag for tag, _ in table.column("fix_tags")[row].as_py()]
        assert tags[0] == 8 and tags[-1] == 10, "every one ends at its CheckSum"
        assert 35 in tags and 34 in tags
    assert dict(table.column("fix_tags")[CARET].as_py())[35] == "0"
    assert dict(table.column("fix_tags")[SOHED].as_py())[35] == "8"


def test_every_field_of_the_bridge_line_lands_in_one_map_or_the_other(
    table: pyarrow.Table,
) -> None:
    """Nothing dropped, nothing counted twice -- the whole of the split."""
    resolved = len(table.column("fix_tags")[BRIDGE].as_py())
    rest = len(table.column("keyval")[BRIDGE].as_py())
    assert resolved + rest == EXPECTED_BRIDGE_PAIRS


def test_the_bridge_line_resolves_the_names_the_dictionary_knows(table: pyarrow.Table) -> None:
    tags = dict(table.column("fix_tags")[BRIDGE].as_py())
    assert tags[55] == "TTF"
    assert tags[54] == "1"
    assert tags[38] == "1200"
    assert tags[453] == "2"


def test_the_bridge_group_keeps_both_entries_in_wire_order(table: pyarrow.Table) -> None:
    tags = [tag for tag, _ in table.column("fix_tags")[BRIDGE].as_py()]
    entries = tags[tags.index(453) :]
    assert entries[:7] == [453, 448, 447, 452, 448, 447, 452]
    assert entries[7:] == [60], "and what followed the group is still after it"
    parties = [value for tag, value in table.column("fix_tags")[BRIDGE].as_py() if tag == 448]
    assert parties == ["BUYSIDE", "XPAR"]


def test_a_bridge_message_in_a_fix_envelope_keeps_both_halves(table: pyarrow.Table) -> None:
    """`8=FIX.4.2|35=UL|#SYMBOL=TTF` is one message with two spellings in it.

    Read as a wire message every named field is noise; read from the `#` the
    header that says what it is gets cut off with the log's prefix. So it is
    its own rule, and the message still starts at its BeginString.
    """
    tags = dict(table.column("fix_tags")[WRAPPED].as_py())
    assert tags[8] == "FIX.4.2" and tags[35] == "UL", "the wire header survives"
    assert tags[55] == "TTF" and tags[54] == "1" and tags[38] == "1200", "and so do the names"
    assert dict(table.column("keyval")[WRAPPED].as_py()) == {"ISINCODE": "XX0000084733"}


def test_a_wire_message_that_only_mentions_a_marker_stays_a_wire_message() -> None:
    """The discriminator is what the sender said it is, not a hash in a Text field."""
    quoted = "8=FIX.4.4|35=8|58=see #A=1 and #B=2|10=1|"
    assert Rules.DEFAULT.categorise(quoted).name == "FIX"
    assert Rules.DEFAULT.categorise("8=FIX.4.2|35=ULX|#A=1|#B=2").name == "FIX"


def test_a_name_no_dictionary_has_is_kept_and_never_guessed(table: pyarrow.Table) -> None:
    assert dict(table.column("keyval")[BRIDGE].as_py()) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }


def test_a_line_carrying_no_message_has_no_pairs_at_all(table: pyarrow.Table) -> None:
    """Null, not an empty map: it was never a message."""
    for row, name in enumerate(EXPECTED_CATEGORIES):
        if name != "OTHER":
            continue
        assert table.column("category_id")[row].as_py() == 0
        assert table.column("fix_tags")[row].as_py() is None
        assert table.column("keyval")[row].as_py() is None


def test_a_stack_trace_still_folds_into_the_row_above_it(table: pyarrow.Table) -> None:
    """The message layer must not have cost the parser its continuations."""
    (folded,) = [
        one for one in table.column("message").to_pylist() if "IllegalStateException" in one
    ]
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


# -- millis and micros in one capture ----------------------------------------


def test_both_stamp_widths_read_as_the_instants_they_spell(table: pyarrow.Table) -> None:
    unix = table.column("unix").to_pylist()
    assert unix[0] == 1_786_665_901_147_250_000, "micros, with a separator"
    assert unix[1] == 1_786_665_901_147_000_000, "millis only"
    assert unix[PIPED] == 1_786_665_901_148_000_000, "millis, comma-separated"
    assert unix[0] > unix[1], "a padded 147 is 147 ms, not 147 us -- and so is earlier"


def test_the_capture_reparses_to_the_same_instants(tmp_path: Path, table: pyarrow.Table) -> None:
    copy = tmp_path / "copy.txt"
    TextFile.from_path(copy).write_arrow(table)
    with TextFile.from_path(copy) as again:
        written = again.read_arrow_table()
    assert written.column("unix").to_pylist() == table.column("unix").to_pylist()
    assert written.column("category_name").to_pylist() == EXPECTED_CATEGORIES


# -- what a rule set decides -------------------------------------------------


def test_a_rule_set_from_a_document_reclassifies_a_line(tmp_path: Path, codec: FixCodec) -> None:
    path = tmp_path / "rules.yml"
    Rules(
        rules=[
            Rule(name="BRIDGE", category_id=42, driver_pattern="^ULBridge$", codec="ul"),
            Rules.DEFAULT.rule(0),
        ]
    ).into_yaml(path)
    own = FixCodec(registry=codec.registry, rules=Rules.from_yaml(path), fix_version="4.4")
    with TextFile.from_path(SAMPLE, codec=own) as log:
        table = log.read_arrow_table()
    names = table.column("category_name").to_pylist()
    assert names[BRIDGE] == "BRIDGE", "the driver decides now, not the message"
    assert names[PIPED] == "OTHER", "and the wire messages are nobody's category"
    assert names[REJECTED] == "BRIDGE", "including the bridge's own prose line"
    assert table.column("fix_tags")[PIPED].as_py() is None


def test_a_file_that_declares_no_rules_parses_as_it_always_did(codec: FixCodec) -> None:
    """The whole of the compatibility promise, in one assertion."""
    quiet = FixCodec(registry=codec.registry, rules=Rules(rules=[]))
    with TextFile.from_path(SAMPLE, codec=quiet) as log:
        table = log.read_arrow_table()
    assert table.column("category_id").to_pylist() == [0] * EXPECTED_RECORDS
    assert table.column("category_name").to_pylist() == ["OTHER"] * EXPECTED_RECORDS
    assert table.column("fix_tags").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("keyval").to_pylist() == [None] * EXPECTED_RECORDS


def test_a_cold_dictionary_costs_the_tags_and_never_the_capture(tmp_path: Path) -> None:
    cold = FixCodec(registry=FixRegistry(cache_dir=tmp_path, offline=True), fix_version="4.4")
    with TextFile.from_path(SAMPLE, codec=cold) as log:
        table = log.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS
    assert dict(table.column("fix_tags")[PIPED].as_py())[35] == "D", "a tag is a tag regardless"
    assert table.column("fix_tags")[BRIDGE].as_py() == [], "and a name resolves to nothing"
    assert len(table.column("keyval")[BRIDGE].as_py()) == EXPECTED_BRIDGE_PAIRS


# -- the same, over a set ----------------------------------------------------


def test_a_folder_of_captures_reads_the_messages_too(tmp_path: Path, codec: FixCodec) -> None:
    """`TextFiles` hands its codec to every file it opens, like everything else."""
    for name in ("app.1.txt", "app.2.txt"):
        (tmp_path / name).write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(tmp_path, codec=codec, static_values={"bridge": "bridge-1"})
    table = files.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS * 2
    assert table.column("category_name").to_pylist() == EXPECTED_CATEGORIES * 2
    assert dict(table.column("fix_tags")[BRIDGE].as_py())[55] == "TTF"
    assert table.schema.names[-1] == "bridge", "and a static column still lands last"
    assert table.column("bridge").to_pylist() == ["bridge-1"] * (EXPECTED_RECORDS * 2)


# -- values that mean nothing ------------------------------------------------


def test_absent_values_never_reach_a_column(tmp_path: Path, codec: FixCodec) -> None:
    line = (
        "2026-08-14 00:05:01.147 [t] [ULBridge] (INFO) toBridge "
        "#SYMBOL=TTF|#SIDE=<null>|#ACCOUNT=|#TEXT=N/A|#ORDERQTY=1200\n"
    )
    path = tmp_path / "absent.txt"
    path.write_text(line)
    with TextFile.from_path(path, codec=codec) as log:
        table = log.read_arrow_table()
    assert dict(table.column("fix_tags")[0].as_py()) == {55: "TTF", 38: "1200"}
    assert table.column("keyval")[0].as_py() == []

    keeping = FixCodec(registry=codec.registry, fix_version="4.4", null_values=frozenset())
    with TextFile.from_path(path, codec=keeping) as log:
        kept = log.read_arrow_table()
    assert len(kept.column("fix_tags")[0].as_py()) == 5
