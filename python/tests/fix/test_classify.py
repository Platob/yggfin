"""Counting a capture's key names, and saying which of three problems each is.

The fixture is synthetic and every value in it is a placeholder: what is under
test is which *names* a capture spells and what the dictionary makes of them,
and a real value would be neither needed nor safe here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow
import pytest

from rekep.fix.classify import (
    ALIASED,
    EXACT,
    NAMESPACE,
    NEAR,
    Classified,
    KeyCount,
    KeyCounts,
    KeyReport,
    apply_report,
    classify,
    count_files,
    count_reader,
)
from rekep.fix.entries import ANY_VERSION, Alias, record_kind
from rekep.fix.fields import namespaced_field
from rekep.fix.registry import FixRegistry

FIXTURES = Path(__file__).parent / "fixtures"

#: The published dictionary, which is what a real run classifies against.
PUBLISHED = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: Derived from `bridge_keys.txt`, then pinned: eight lines, seven of which
#: carry a bridge message, spelling twenty-one distinct key names -- distinct
#: as the capture spells them, so `ORDERQTY`, `ORDER_QTY`, `CLORDID` and
#: `clordid` are four of them and not two.
EXPECTED_LINES = 8
EXPECTED_MESSAGES = 7
EXPECTED_NAMES = 21


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    return FixRegistry(cache_dir=PUBLISHED, offline=True)


@pytest.fixture
def counts() -> KeyCounts:
    return count_files(str(FIXTURES), pattern="bridge_keys.txt")


@pytest.fixture
def report(counts: KeyCounts, registry: FixRegistry) -> KeyReport:
    return classify(counts, registry)


def _row(report: KeyReport, name: str) -> Classified:
    (found,) = [row for row in report.rows if row.name == name]
    return found


# -- counting ----------------------------------------------------------------


def test_the_fixture_is_the_shape_the_tests_assume(counts: KeyCounts) -> None:
    assert counts.lines == EXPECTED_LINES
    assert counts.messages == EXPECTED_MESSAGES, "the prose line carries none"
    assert len(counts.counts) == EXPECTED_NAMES


def test_a_capture_is_counted_under_the_spelling_it_used(counts: KeyCounts) -> None:
    """`ORDER_QTY` and `ORDERQTY` are one field and two spellings.

    Folding them at count time would answer the question the report exists to
    ask -- which spelling a bridge actually writes, and how often -- so they
    are counted apart and resolved together.
    """
    spelled = {count.name for count in counts.ordered()}
    assert {"ORDERQTY", "ORDER_QTY", "CLORDID", "clordid"} <= spelled


def test_counting_is_by_name_and_never_by_value() -> None:
    """The whole contract: a value never leaves the batch it was counted in."""
    lines = pyarrow.array(
        [
            "toBridge #CLORDID=ORD-TEST-01|#SIDE=1",
            "toBridge #CLORDID=ORD-TEST-02|#SIDE=2",
        ]
    )
    counted = KeyCounts().add_messages(lines, source="fake")
    document = json.dumps(counted.into_dict())
    assert "ORD-TEST-01" not in document and "ORD-TEST-02" not in document
    assert {count.name for count in counted.ordered()} == {"CLORDID", "SIDE"}
    assert all(count.total == 2 for count in counted.ordered())


def test_a_name_written_both_ways_is_counted_both_ways(counts: KeyCounts) -> None:
    """`#Foo` and `Foo` are one name and two namespaces, and both are the data.

    Asymmetric on purpose: a few names appear as both, a larger set only
    marked, and another set only bare. A report that summed them would say
    nothing about which.
    """
    found = {count.name: count for count in counts.ordered()}
    assert (found["SIDE"].marked, found["SIDE"].bare) == (3, 1)
    assert (found["CLORDID"].marked, found["CLORDID"].bare) == (3, 1)
    assert (found["ORDERQTY"].marked, found["ORDERQTY"].bare) == (3, 0), "marked only"
    assert (found["SIDDE"].marked, found["SIDDE"].bare) == (1, 0)
    both = [count.name for count in counts.ordered() if count.marked and count.bare]
    assert sorted(both) == ["CLORDID", "SIDE"], "and only a couple are ever both"


def test_counts_carry_which_capture_they_came_from(counts: KeyCounts) -> None:
    """Provenance, because a name counted in one bridge is not counted in three."""
    assert {source for count in counts.ordered() for source in count.sources} == {"bridge_keys.txt"}


def test_two_runs_merge_into_one(counts: KeyCounts) -> None:
    merged = counts.merged(counts)
    assert merged.lines == 2 * EXPECTED_LINES
    assert len(merged.counts) == EXPECTED_NAMES
    assert merged.ordered()[0].total == 2 * counts.ordered()[0].total


def test_counts_round_trip_through_their_document(counts: KeyCounts) -> None:
    """So a run over three captures is three runs and one merge."""
    read = KeyCounts.from_dict(json.loads(json.dumps(counts.into_dict())))
    assert read.lines == counts.lines and read.messages == counts.messages
    assert {c.name: c.total for c in read.ordered()} == {c.name: c.total for c in counts.ordered()}


def test_a_line_carrying_no_message_is_read_and_not_counted() -> None:
    counted = KeyCounts().add_messages(pyarrow.array(["just prose, and no pairs at all"]))
    assert counted.lines == 1 and counted.messages == 0 and counted.counts == {}


def test_an_empty_batch_changes_nothing() -> None:
    counted = KeyCounts().add_messages(pyarrow.array([], pyarrow.string()))
    assert counted.lines == 0 and counted.counts == {}


def test_a_reader_of_batches_is_counted_one_batch_at_a_time() -> None:
    """The streaming contract: batches in, counts out, nothing held between."""
    batch = pyarrow.record_batch(
        {
            "message": pyarrow.array(["toBridge #CLORDID=ORD-TEST-01|#SIDE=1"] * 3),
            "plugincode": pyarrow.array(["ULBridge", "ULFilter", "OMSSales"]),
        }
    )
    assert count_reader(batch).lines == 3
    filtered = count_reader(batch, plugins="^UL")
    assert filtered.lines == 2, "the plugin filter is applied before anything is parsed"


def test_a_batch_with_no_message_column_is_refused() -> None:
    batch = pyarrow.record_batch({"plugincode": pyarrow.array(["ULBridge"])})
    with pytest.raises(ValueError, match="needs a 'message' column"):
        count_reader(batch)


# -- classifying -------------------------------------------------------------


def test_every_counted_name_is_one_of_the_four_kinds(report: KeyReport) -> None:
    assert len(report.rows) == EXPECTED_NAMES
    assert sum(report.names().values()) == EXPECTED_NAMES
    assert sum(report.totals().values()) == sum(row.count.total for row in report.rows)


def test_a_name_the_dictionary_has_is_exact(report: KeyReport) -> None:
    assert _row(report, "SIDE").kind == EXACT
    assert _row(report, "SIDE").resolved == "Side"
    assert _row(report, "ISINCODE").resolved == "ISINCODE", "including one FIX never numbered"


def test_a_renderers_own_casing_is_the_same_name(report: KeyReport) -> None:
    """`clordid` and `ClOrdID` carry the same folded name."""
    assert _row(report, "clordid").kind == EXACT
    assert _row(report, "clordid").resolved == "ClOrdID"


def test_a_separator_spelling_is_the_same_name(report: KeyReport) -> None:
    assert _row(report, "ORDER_QTY").kind == EXACT
    assert _row(report, "ORDER_QTY").resolved == "OrderQty"


def test_a_recorded_spelling_is_reported_as_one_rather_than_as_a_match(
    report: KeyReport,
) -> None:
    """Separately, so a backlog can tell what it already decided from what it knew."""
    assert _row(report, "AMON.ISINCODE").kind == ALIASED
    assert _row(report, "AMON.ISINCODE").resolved == "ISINCODE"


def test_a_near_miss_is_flagged_and_never_treated_as_the_name_it_is_near(
    report: KeyReport,
) -> None:
    """Evidence that two names are one field, and a person decides, not this."""
    assert _row(report, "PARTYROLLE").kind == NEAR
    assert (_row(report, "PARTYROLLE").resolved, _row(report, "PARTYROLLE").distance) == (
        "PartyRole",
        1,
    )
    assert _row(report, "SIDDE").kind == NEAR and _row(report, "SIDDE").resolved == "Side"


def test_a_component_path_is_the_field_at_its_end(report: KeyReport) -> None:
    """`NOPARTYIDS[0].PARTYID` is `PartyID` inside a group, index and all."""
    assert _row(report, "NOPARTYIDS.PARTYID").kind == EXACT
    assert _row(report, "NOPARTYIDS.PARTYID").resolved == "PartyID"
    assert "NOPARTYIDS[0].PARTYID" not in {row.name for row in report.rows}, (
        "one name written twice is one name"
    )


def test_a_vendor_namespace_is_not_read_as_the_field_its_tail_names(
    report: KeyReport,
) -> None:
    """`TECH.CLIENTID` is a vendor's enrichment, not `ClientID <109>`.

    What tells it from `NOPARTYIDS.PARTYID` is whether the segments in front
    name anything the dictionary has -- and `TECH` names nothing.
    """
    assert _row(report, "TECH.CLIENTID").kind == NAMESPACE
    assert _row(report, "TECH.CLIENTID").resolved == ""
    assert _row(report, "VENDOR.SOURCE").kind == NAMESPACE


def test_a_name_nothing_here_has_is_a_vendor_field(report: KeyReport) -> None:
    vendor = {row.name for row in report.of(NAMESPACE)}
    assert {"ULFROMSESSIONNAME", "CONVERSATIONID", "UNRESOLVEDROUTING", "BLOOMBERGCODE"} <= vendor


def test_a_bare_tag_is_not_a_rendered_name(registry: FixRegistry) -> None:
    """A wire message is tags, and whether the registry has one is another question."""
    counted = KeyCounts().add_messages(pyarrow.array(["8=FIX.4.4\x0154=1\x0110=000\x01"]))
    assert {count.name for count in counted.ordered()} == {"8", "54", "10"}
    assert classify(counted, registry).rows == ()


def test_the_report_is_ordered_by_how_much_traffic_each_name_is(report: KeyReport) -> None:
    """A backlog is a priority order, so the biggest thing to fix comes first."""
    totals = [row.count.total for row in report.rows]
    assert totals == sorted(totals, reverse=True)


def test_the_report_round_trips_through_the_document_a_review_reads(
    report: KeyReport,
) -> None:
    read = KeyReport.from_json(report.into_json())
    assert read.rows == report.rows
    assert (read.lines, read.messages) == (report.lines, report.messages)


def test_the_report_is_json_and_carries_names_and_counts_only(report: KeyReport) -> None:
    """One serialization, and never a value out of a capture."""
    written = report.into_json().decode()
    assert json.loads(written)["lines"] == EXPECTED_LINES
    assert "PARTYROLLE" in written
    assert "ORD-TEST-01" not in written, "names and counts, never a value"


# -- and into the registry ---------------------------------------------------


@pytest.fixture
def editable(tmp_path: Path, registry: FixRegistry) -> FixRegistry:
    """A writable copy of the published dictionary, for the apply path."""
    return FixRegistry(cache_dir=registry.into_zip(tmp_path / "fix.zip"), offline=True)


def test_nothing_is_applied_unless_it_is_asked_for(
    editable: FixRegistry, report: KeyReport
) -> None:
    """A near miss is evidence, not proof, and this is what makes that true."""
    assert apply_report(editable, report) == []
    assert editable.resolve("PARTYROLLE") is None


def test_a_near_miss_becomes_an_alias_with_the_capture_that_earned_it(
    editable: FixRegistry, report: KeyReport
) -> None:
    applied = apply_report(editable, report, aliases=True)
    assert any("PARTYROLLE -> PartyRole" in line for line in applied)
    entry = editable.resolve("PARTYROLLE")
    assert entry.fix.canonical == "PartyRole"
    (alias,) = [found for found in entry.fix.named_aliases if found.name == "PARTYROLLE"]
    assert alias.source == "bridge_keys.txt" and alias.occurrences == 1


def test_a_vendor_name_becomes_a_declared_field(editable: FixRegistry, report: KeyReport) -> None:
    applied = apply_report(editable, report, namespace=True)
    assert any("TECH.CLIENTID" in line for line in applied)
    entry = editable.resolve("TECH.CLIENTID")
    assert record_kind(entry) == NAMESPACE and entry.fix.tag is None
    assert entry.fix.versions == (ANY_VERSION,)
    assert not entry.fix.column, "a column is a change to a published contract, not to a dictionary"
    assert editable.check() == []


def test_only_what_was_counted_often_enough_is_applied(
    editable: FixRegistry, report: KeyReport
) -> None:
    """One occurrence in one capture is a typo; a thousand is a field."""
    assert apply_report(editable, report, aliases=True, namespace=True, minimum=2) == []
    assert editable.resolve("TECH.CLIENTID") is None


def test_applying_a_report_twice_over_is_refused_rather_than_duplicated(
    editable: FixRegistry, report: KeyReport
) -> None:
    apply_report(editable, report, namespace=True)
    with pytest.raises(KeyError, match="already stored"):
        apply_report(editable, report, namespace=True)


def test_an_applied_alias_makes_the_next_run_call_the_name_known(
    editable: FixRegistry, counts: KeyCounts, report: KeyReport
) -> None:
    """The loop closes: what was a near miss is a recorded spelling next time."""
    apply_report(editable, report, aliases=True)
    again = classify(counts, editable)
    assert _row(again, "PARTYROLLE").kind == ALIASED
    assert not [row for row in again.of(NEAR) if row.name == "PARTYROLLE"]


def test_a_declared_vendor_field_leaves_the_vendor_backlog(
    editable: FixRegistry, counts: KeyCounts, report: KeyReport
) -> None:
    apply_report(editable, report, namespace=True)
    again = classify(counts, editable)
    assert again.of(NAMESPACE) == (), "every one of them was declared"
    assert _row(again, "TECH.CLIENTID").kind == EXACT


def test_a_report_read_back_from_disk_applies_the_same(
    editable: FixRegistry, report: KeyReport, tmp_path: Path
) -> None:
    """Counting and deciding are separable: a run produces a file a person reviews."""
    written = tmp_path / "report.json"
    report.into_json(written)
    read = KeyReport.from_json(written)
    assert apply_report(editable, read, aliases=True) == apply_report(
        FixRegistry(cache_dir=editable.into_zip(tmp_path / "again.zip"), offline=True),
        report,
        aliases=True,
    )


def test_a_counted_name_declares_itself_as_the_entry_it_would_be() -> None:
    """The bridge between a count and a registry verb, on its own."""
    count = KeyCount(name="FAKE.VENDOR.CODE", marked=7, sources=("brk",))
    row = Classified(count, NAMESPACE)
    assert row.into_entry() == namespaced_field("FAKE.VENDOR.CODE", "String")
    assert row.into_entry(column="fakevendorcode").fix.column == "fakevendorcode", (
        "a caller that already knows the column declares it in the same record"
    )
    assert Classified(count, NEAR, "FakeCode", 1).into_alias() == Alias(
        name="FAKE.VENDOR.CODE", source="brk", occurrences=7
    )
