"""Refined reads only its lifecycle context and writes only its own window."""

from __future__ import annotations

import datetime
from contextlib import ExitStack
from pathlib import Path

import pyarrow
import pytest

from rekep import IOBase, Message
from rekep.fields import stored_arrow_reader
from rekep.fix import fix_codec, fix_message_field, fix_parse_arrow_reader
from rekep.pipeline import parse_fix_refined
from rekep.times import window_of

from .test_pipeline import Warehouse

UTC = datetime.timezone.utc
pytestmark = pytest.mark.integration

#: The hour the refined run covers; the walk reads the one before it too.
HOUR = window_of("2026-08-14T10:00:00Z", "2026-08-14T11:00:00Z")


@pytest.mark.parametrize("expires", ["10:40:00", "11:40:00"])
@pytest.mark.parametrize("previous_clock", ["09:59:00", "08:59:00"])
def test_refined_uses_previous_hour_and_filters_history_and_future_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expires: str,
    previous_clock: str,
) -> None:
    from pyiceberg.io.pyarrow import PyArrowFile

    warehouse = Warehouse(tmp_path, HOUR)
    capture = tmp_path / "hours.log"
    # Out-of-order hours ensure physical arrival order cannot stand in for
    # event order. Distinct older/newer chains must be pruned before reading.
    capture.write_text(
        "\n".join(
            f"2026-08-14 {clock}.000 [77] [ULBridge] (INFO) Sending : "
            f"8=FIX.4.4|35=8|49=BUY|56=SELL|34={index}|52=20260814-{clock}|"
            f"37={order}|11={order}|39=0|150=0|55=AAPL|54=1|38=10|151=10|"
            f"126=20260814-{expires}|10=0|"
            for index, (clock, order) in enumerate(
                [
                    ("11:00:00", "later"),
                    ("10:10:00", "order-1"),
                    ("08:59:59", "older"),
                    (previous_clock, "order-1"),
                ],
                1,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    with ExitStack() as opened:
        store = opened.enter_context(warehouse.opened())
        codec = fix_codec(threads=1)
        field = fix_message_field(codec)
        source = IOBase.from_uri(capture.as_uri())
        opened.callback(source.close)
        lines = source.read_arrow_reader(options=Message.text_options())
        opened.callback(lines.close)
        parsed = fix_parse_arrow_reader(codec, lines)
        opened.callback(parsed.close)
        rows = stored_arrow_reader(parsed, field)
        opened.callback(rows.close)
        raw = store.dataset("fix.raw", field=field)
        opened.callback(raw.close)
        assert raw.append_arrow_reader(rows, field) == 4

    paths: list[str] = []
    original = PyArrowFile.open

    def tracked(self, *args, **kwargs):
        if self.location.endswith(".parquet") and "raw" in self.location:
            paths.append(self.location)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", tracked)
    landed = warehouse.run(parse_fix_refined)
    has_history = previous_clock == "09:59:00"
    assert landed.read == (2 if has_history else 1)
    assert landed.written == (2 if expires == "10:40:00" else 1)
    assert paths and all(
        "currunix_hour=2026-08-14-09" in path or "currunix_hour=2026-08-14-10" in path
        for path in paths
    )
    first_hour = "09" if has_history else "10"
    assert f"currunix_hour=2026-08-14-{first_hour}" in paths[0]

    def refined_rows() -> pyarrow.Table:
        with ExitStack() as opened:
            store = opened.enter_context(warehouse.opened())
            refined = store.dataset("fix.refined")
            opened.callback(refined.close)
            reader = refined.read_arrow_reader(order_by="currunix")
            opened.callback(reader.close)
            return reader.read_all()

    held = refined_rows()
    rows = held.to_pylist()
    assert rows[0]["currunix"] == datetime.datetime(2026, 8, 14, 10, 10, tzinfo=UTC)
    if has_history:
        assert rows[0]["prevuuid"] is not None
        assert rows[0]["prevunix"] == datetime.datetime(2026, 8, 14, 9, 59, tzinfo=UTC)
        assert rows[0]["seqnum"] == 1
    else:
        # The one-hour context is an explicit horizon, not a claim that a
        # business chain cannot have an older predecessor.
        assert rows[0]["prevuuid"] is None
        assert rows[0]["prevunix"] is None
        assert rows[0]["seqnum"] is None
    if expires == "10:40:00":
        assert rows[1]["currunix"] == datetime.datetime(2026, 8, 14, 10, 40, tzinfo=UTC)
        assert rows[1]["state"] == "95EXPIRED"
    assert warehouse.run(parse_fix_refined) == landed
    assert refined_rows().equals(held)
