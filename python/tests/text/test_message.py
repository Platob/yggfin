"""The protocol-neutral row produced by text files."""

from pathlib import Path

from rekep import Message, TextFile
from rekep.enums import EventType
from rekep.market import Event


def test_a_message_adds_only_log_provenance_and_raw_content() -> None:
    assert issubclass(Message, Event)
    assert Message.into_field().names == [
        *Event.into_field().names,
        "source_url",
        "source_rownum",
        "thread_name",
        "plugin_code",
        "message",
    ]
    assert all(
        not any(key.startswith("fix:") for key in field.metadata)
        for field in Message.into_field().fields
    )


def test_raw_identity_is_scoped_to_the_payload_and_its_source() -> None:
    first = Message(message="same", source_url="one.log", source_rownum=2).identify()
    again = Message(message="same", source_url="one.log", source_rownum=2).identify()
    copied = Message(message="same", source_url="two.log", source_rownum=2).identify()

    assert first.hash == again.hash == first.xhash
    assert copied.hash != first.hash


def test_a_text_file_does_not_interpret_fix_fields(tmp_path: Path) -> None:
    path = tmp_path / "capture.log"
    payload = "8=FIX.4.4|35=D|49=XPAR|56=BUY|55=IBM|10=000"
    path.write_text(f"2026-08-14 09:30:00.123 [thread] [bridge] (INFO) {payload}\n")

    with TextFile.from_path(path) as source:
        table = source.read_arrow_table()

    assert table.schema.names == Message.into_field().names
    assert table.column("message").to_pylist() == [payload]
    assert table.column("etype").to_pylist() == [int(EventType.UNKNOWN)]
    assert table.column("mic").to_pylist() == [None]
    assert table.column("hash").to_pylist() == table.column("xhash").to_pylist()
    assert table.column("hash")[0].as_py() == Message.hash_of(
        payload, table.column("source_url")[0].as_py(), 1
    )
