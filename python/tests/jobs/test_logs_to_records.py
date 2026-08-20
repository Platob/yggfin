import pathlib

import pyarrow

from rekep.jobs import FilesToLogs, LogsToRecords, parse_fields
from rekep.models import Log, ParsedMessage

FIX_SAMPLE = pathlib.Path(__file__).parent.parent / "data" / "fix_sample.txt"


# -- parse_fields: the regex parser on its own ------------------------------


def test_parses_pipe_delimited_key_value_pairs() -> None:
    assert parse_fields("8=FIX.4.4|9=112|35=D|") == {"8": "FIX.4.4", "9": "112", "35": "D"}


def test_strips_a_leading_hash_from_the_key() -> None:
    assert parse_fields("#8=FIX.4.4|9=113|") == {"8": "FIX.4.4", "9": "113"}


def test_empty_segments_are_skipped_not_matched() -> None:
    """A trailing `|` (or a doubled one) leaves an empty segment, not a match."""
    assert parse_fields("8=FIX.4.4||9=112|") == {"8": "FIX.4.4", "9": "112"}


def test_a_value_may_itself_be_empty() -> None:
    assert parse_fields("150=|39=0|") == {"150": "", "39": "0"}


def test_non_key_value_text_parses_to_nothing() -> None:
    assert parse_fields("plain text, no pairs here") == {}


def test_a_single_segment_with_no_pipe_can_still_match() -> None:
    """No `|` at all means one segment, the whole message -- still checked
    against the pattern, not skipped just for lacking a delimiter."""
    assert parse_fields("key=value") == {"key": "value"}


def test_empty_message_parses_to_nothing() -> None:
    assert parse_fields("") == {}


# -- LogsToRecords: the job, batches in, batches out -------------------------


def test_consumes_and_produces_default_correctly() -> None:
    job = LogsToRecords(uri="rekep:/jobs/l2r")
    assert job.consumed_records() == [Log]
    assert job.produced_records() == [ParsedMessage]


def test_arrow_transform_structures_fix_messages() -> None:
    f2l = FilesToLogs(uri="rekep:/jobs/f2l", source=FIX_SAMPLE.as_uri())
    log_batches = list(f2l.arrow_transform(f2l.extract()))

    l2r = LogsToRecords(uri="rekep:/jobs/l2r")
    record_batches = list(l2r.arrow_transform(iter(log_batches)))
    rows = pyarrow.Table.from_batches(record_batches).to_pylist()

    assert len(rows) == 4, "one ParsedMessage per Log row, none dropped"

    opens_plain, hash_prefixed, plain_text, different_protocol = rows

    assert opens_plain["protocol"] == "FIX.4.4"
    assert dict(opens_plain["fields"])["11"] == "ORD001"

    assert hash_prefixed["protocol"] == "FIX.4.4", "the '#8=' row's protocol still parses"
    assert "8" in dict(hash_prefixed["fields"]), "the '#' was stripped from the key"
    assert "#8" not in dict(hash_prefixed["fields"])

    assert plain_text["protocol"] is None
    assert dict(plain_text["fields"]) == {}

    assert different_protocol["protocol"] == "FIX.4.2"


def test_row_count_and_identity_survive_the_transform() -> None:
    f2l = FilesToLogs(uri="rekep:/jobs/f2l", source=FIX_SAMPLE.as_uri())
    log_rows = pyarrow.Table.from_batches(list(f2l.arrow_transform(f2l.extract()))).to_pylist()

    l2r = LogsToRecords(uri="rekep:/jobs/l2r")
    record_rows = pyarrow.Table.from_batches(
        list(l2r.arrow_transform(f2l.arrow_transform(f2l.extract())))
    ).to_pylist()

    assert [r["hash64"] for r in log_rows] == [r["hash64"] for r in record_rows]
    assert [r["url"] for r in log_rows] == [r["url"] for r in record_rows]
