"""The raw Message contract is the native ULBridge text read."""

import datetime

from yggdryl import IOBase

from rekep import Message
from rekep.times import MESSAGE_HEADER


def test_message_declares_the_text_row_and_its_storage_columns() -> None:
    field = Message.field()

    assert [member.name for member in field] == [
        "url",
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "sessionUid",
        "msgCtxId",
        "seqNum",
        "plugin",
        "level",
        "bodyhash",
        "body",
    ]
    assert field["timestamp"].into_arrow().type.unit == "us"
    assert field["timepartition"].partition.sources == ["timestamp"]
    assert field["timepartition"].iceberg["partition_key"] == "hour"
    assert field["bodyhash"].digest.sources == ["body"]
    assert field["bodyhash"].digest.algorithm == "xxh3-128"


def test_message_text_options_own_the_complete_native_read() -> None:
    options = Message.text_options()

    assert options.start_rownum == 1
    assert options.parse_mtime is False
    assert options.rowheader == MESSAGE_HEADER
    assert str(options.timezone) == "UTC"
    assert options.safe is False
    assert options.field == Message.field()
    assert options.capture_names == (
        "timestamp",
        "threadId",
        "sessionUid",
        "msgCtxId",
        "seqNum",
        "plugin",
        "level",
    )


def test_the_text_reader_produces_messages_without_a_python_row_pass(tmp_path) -> None:
    source = tmp_path / "bridge.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] "
        b"[ULBridge] (INFO) Sending : 8=FIX.4.4|35=D|10=0|\n"
        b"2026-08-14 00:05:01.148 [653] [Spot_FX_TradeCapture] (WARN) prose\n"
    )

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table.schema.equals(Message.field().into_arrow_schema(), check_metadata=True)
    assert table.select(
        ("rownum", "timestamp", "threadId", "sessionUid", "msgCtxId", "seqNum", "plugin")
    ).to_pylist() == [
        {
            "rownum": 1,
            "timestamp": datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=datetime.UTC),
            "threadId": 250,
            "sessionUid": "e7256476",
            "msgCtxId": "9effef3e6a",
            "seqNum": 72504,
            "plugin": "ULBridge",
        },
        {
            "rownum": 2,
            "timestamp": datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=datetime.UTC),
            "threadId": 653,
            "sessionUid": None,
            "msgCtxId": None,
            "seqNum": None,
            "plugin": "Spot_FX_TradeCapture",
        },
    ]
    assert table.column("timepartition").equals(table.column("timestamp"))
    assert table.column("body").to_pylist() == [
        b"Sending : 8=FIX.4.4|35=D|10=0|",
        b"prose",
    ]
    assert all(len(value) == 16 for value in table.column("bodyhash").to_pylist())


def test_a_line_without_the_bridge_header_is_kept_as_an_unstamped_message(tmp_path) -> None:
    source = tmp_path / "unframed.log"
    source.write_bytes(b"one physical line\n")

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        row = reader.read_all().to_pylist()[0]
    finally:
        reader.close()

    assert row["body"] == b"one physical line"
    assert row["rownum"] == 1
    assert all(
        row[name] is None
        for name in ("timestamp", "timepartition", "threadId", "sessionUid", "msgCtxId", "seqNum")
    )


def test_message_instance_normalizes_scalar_inputs() -> None:
    message = Message.from_text(
        b"body",
        timestamp="2026-08-14 02:05:01.147250+02:00",
        threadId="250",
        seqNum="72504",
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
    assert (message.threadId, message.seqNum) == (250, 72504)
