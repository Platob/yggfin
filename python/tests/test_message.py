"""The raw Message Arrow boundary."""

import datetime

import pyarrow
import pytest

from rekep import Message


def raw_batch(timestamps: list[str | None] | pyarrow.Array) -> pyarrow.RecordBatch:
    """One Yggdryl-shaped text batch with the required raw columns."""
    rows = len(timestamps)
    return pyarrow.record_batch(
        {
            "url": [f"file:///capture-{index}.log" for index in range(rows)],
            "rownum": list(range(1, rows + 1)),
            "timestamp": timestamps,
            "threadname": [f"worker-{index}" for index in range(rows)],
            "branch": [f"branch-{index}" for index in range(rows)],
            "level": ["INFO" if index % 2 == 0 else "WARN" for index in range(rows)],
            "body": [f"body-{index}".encode() for index in range(rows)],
        }
    )


def test_message_timestamp_is_microsecond_utc() -> None:
    field = Message.field().field("timestamp").into_arrow()

    assert field.type == pyarrow.timestamp("us", tz="UTC")
    assert field.nullable

    partition = Message.field().field("timepartition")
    assert partition.dtype == Message.field().field("timestamp").dtype
    assert partition.nullable
    assert partition.partition.sources == ["timestamp"]
    assert partition.partition.transform is None
    assert partition.iceberg["partition_key"] == "hour"


def test_message_batch_applies_zero_rows_to_the_exact_schema() -> None:
    batch = Message.apply_arrow_batch(raw_batch([]))

    assert batch.num_rows == 0
    assert batch.schema.equals(Message.field().into_arrow_schema(), check_metadata=True)


def test_message_batch_applies_every_header_timestamp_spelling_without_row_loops() -> None:
    batch = Message.apply_arrow_batch(
        raw_batch(
            [
                "2026-08-14 00:05:01.147_250",
                "2026-08-14 00:05:01,148",
                "2026-08-14 00:05:01.150.456",
                "2026-08-14 00:05:01.151,456",
                "2026-08-14 00:05:01,152.456",
                "20260814-00:05:01.149123789",
                "20260828135029258000",
                "20260828135029",
                None,
            ]
        )
    )

    assert batch.schema.equals(Message.field().into_arrow_schema(), check_metadata=True)
    assert batch.column("timestamp").to_pylist() == [
        datetime.datetime(2026, 8, 14, 0, 5, 1, 147250, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, 150456, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, 151456, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, 152456, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, 149123, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 28, 13, 50, 29, 258000, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 28, 13, 50, 29, tzinfo=datetime.UTC),
        None,
    ]
    assert batch.column("timepartition").equals(batch.column("timestamp"))


def test_message_batch_rejects_an_invalid_present_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match=r"Failed to parse string: .*timestamp\[us, tz=UTC\]",
    ):
        Message.apply_arrow_batch(raw_batch(["not-a-timestamp"]))


def test_message_batch_rejects_an_invalid_calendar_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match=r"Failed to parse string: .*timestamp\[us, tz=UTC\]",
    ):
        Message.apply_arrow_batch(raw_batch(["2026-02-30 00:05:01.123_456"]))


@pytest.mark.parametrize("storage", ["dictionary", "large_string", "string_view"])
def test_message_batch_accepts_alternate_arrow_text_storage(storage: str) -> None:
    values = ["2026-08-14 00:05:01.147_250", None]
    if storage == "dictionary":
        timestamps = pyarrow.array(values).dictionary_encode()
    elif storage == "large_string":
        timestamps = pyarrow.array(values, type=pyarrow.large_string())
    else:
        if not hasattr(pyarrow, "string_view"):
            pytest.skip("this PyArrow has no string_view type")
        timestamps = pyarrow.array(values, type=pyarrow.string_view())

    batch = Message.apply_arrow_batch(raw_batch(timestamps))

    assert batch.column("timestamp").to_pylist() == [
        datetime.datetime(2026, 8, 14, 0, 5, 1, 147250, tzinfo=datetime.UTC),
        None,
    ]


def test_message_batch_truncates_nanoseconds_and_preserves_every_other_column() -> None:
    source = raw_batch(
        [
            "2026-08-14 00:05:01.123456789",
            "2026-08-14 00:05:01.000000999",
            None,
        ]
    )

    batch = Message.apply_arrow_batch(source)

    assert batch.column("timestamp").to_pylist() == [
        datetime.datetime(2026, 8, 14, 0, 5, 1, 123456, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 14, 0, 5, 1, tzinfo=datetime.UTC),
        None,
    ]
    assert batch.column("timepartition").equals(batch.column("timestamp"))
    for name in ("url", "rownum", "threadname", "branch", "level", "body"):
        assert batch.column(name).to_pylist() == source.column(name).to_pylist()


def test_message_instance_normalizes_a_timestamp_to_utc() -> None:
    message = Message.from_text(
        b"body",
        timestamp="2026-08-14 02:05:01.147250+02:00",
    )

    assert message.timestamp == datetime.datetime(
        2026,
        8,
        14,
        0,
        5,
        1,
        147250,
        tzinfo=datetime.UTC,
    )


def classified(bodies: list[bytes], direction: str = "sent") -> pyarrow.RecordBatch:
    """One text batch carrying the given payloads, through the raw boundary."""
    rows = len(bodies)
    return Message.apply_arrow_batch(
        pyarrow.record_batch(
            {
                "url": ["file:///capture.log"] * rows,
                "rownum": list(range(1, rows + 1)),
                "timestamp": [None] * rows,
                "threadname": [None] * rows,
                "branch": [None] * rows,
                "level": [None] * rows,
                "body": bodies,
            }
        ),
        direction,
    )


def test_message_names_what_a_body_is_without_parsing_it() -> None:
    batch = classified(
        [
            b"sending >> 8=FIX.4.4|35=D|55=TTF|10=203|",
            b"recv 8=FIX4^A35=0^A10=017^A on session 3",
            b"toBridge #SYMBOL=TTF|#SIDE=1",
            b"no level printed by this driver",
        ]
    )

    assert batch.column("mimetype").to_pylist() == [
        "text/fix",
        "text/fix",
        "text/ullink",
        "application/octet-stream",
    ]
    assert batch.column("msgtype").to_pylist() == ["D", "0", "unknown", "unknown"]
    # A verb beats the default; a line carrying none takes it.
    assert batch.column("msgdirection").to_pylist() == ["SENT", "RECV", "SENT", "SENT"]


def test_message_direction_default_is_the_captures_own_side() -> None:
    unmarked = [b"8=FIX.4.4|35=D|10=0|"]

    assert classified(unmarked, "sent").column("msgdirection").to_pylist() == ["SENT"]
    assert classified(unmarked, "recv").column("msgdirection").to_pylist() == ["RECV"]
    # `unknown` leaves the reading null, which the declaration fills.
    assert classified(unmarked, "unknown").column("msgdirection").to_pylist() == ["unknown"]

    with pytest.raises(ValueError, match="sent, recv, unknown"):
        classified(unmarked, "sideways")


def test_message_digests_the_body_and_nothing_else() -> None:
    holder = Message.field()["msghash"]
    assert holder.digest.is_holder()
    assert holder.digest.sources == ["body"]
    assert holder.digest.algorithm == "xxh3-128"

    batch = classified([b"8=FIX.4.4|35=D|10=0|", b"8=FIX.4.4|35=D|10=0|", b"other"])
    digests = batch.column("msghash").to_pylist()

    assert all(isinstance(digest, bytes) and len(digest) == 16 for digest in digests)
    # The same bytes digest the same however the row reached the boundary, and
    # different bytes do not.
    assert digests[0] == digests[1]
    assert digests[0] != digests[2]
