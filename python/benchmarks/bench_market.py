"""Measure columnar event flattening against a verified Python row reference."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser, report  # noqa: E402

from rekep.fields import stored_arrow_reader  # noqa: E402
from rekep.fix import FixCodec  # noqa: E402
from rekep.market import (  # noqa: E402
    EVENT_KINDS,
    FLATTENED_COLUMNS,
    book_event_arrow_reader,
    book_field,
)


def main() -> None:
    options = parser(__doc__, rows=10_000).parse_args()
    count = 64 if options.quick else options.rows
    repeat = 1 if options.quick else options.repeat
    codec = FixCodec.from_env()
    message = codec.parse_fix_line(
        b"8=FIX.4.4|35=W|52=20260921-10:00:00|55=AAPL|268=3|"
        b"269=0|278=B1|270=100|271=10|269=1|278=A1|270=102|271=12|"
        b"269=2|278=T1|270=101|271=2|10=0|"
    )
    native = codec.book_arrow_reader([message])
    row = stored_arrow_reader(native, book_field()).read_all()
    corpus = pa.concat_tables([row] * count).combine_chunks()

    for kind in ("quotes", "executions"):

        def columnar(kind=kind):
            return book_event_arrow_reader(corpus.to_reader(), kind).read_all()

        # Arrow builds no `uuid` nested in a list from Python values, so the
        # reference answers the rows themselves.
        def reference(kind=kind):
            return [
                event
                for book in corpus.to_pylist()
                for event in book[FLATTENED_COLUMNS[kind]]
                if event["marketdatakind"] == EVENT_KINDS[kind]
            ]

        expected = reference()

        def ordering(event):
            return event["curruuid"], event["side"]

        assert sorted(expected, key=ordering) == sorted(columnar().to_pylist(), key=ordering)
        baseline = best_of(reference, repeat)
        report(f"{kind}: Python rows", baseline, len(expected))
        report(f"{kind}: Arrow flatten", best_of(columnar, repeat), len(expected), baseline)


if __name__ == "__main__":
    main()
