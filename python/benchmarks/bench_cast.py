"""Benchmark casting a batch onto a field: rows/s, against Arrow's own cast."""

from __future__ import annotations

import datetime
import pathlib
import sys
from collections.abc import Callable

import pyarrow

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of as best  # noqa: E402
from _bench import parser

from rekep.fields import Field  # noqa: E402

VENUE = pyarrow.struct([("mic", pyarrow.string()), ("desk", pyarrow.string())])
WIDER_VENUE = pyarrow.struct(
    [("desk", pyarrow.string()), ("mic", pyarrow.string()), ("pod", pyarrow.int32())]
)


def source_batch(rows: int) -> pyarrow.RecordBatch:
    """A batch shaped the way an upstream transform tends to hand one over."""
    day = datetime.date(2026, 8, 14)
    return pyarrow.RecordBatch.from_pydict(
        {
            "symbol": [f"SYM{i % 512}" for i in range(rows)],
            "size": pyarrow.array([i % 4096 for i in range(rows)], type=pyarrow.int64()),
            "day": [day] * rows,
            "venue": pyarrow.array([{"mic": "XPAR", "desk": "EQ"}] * rows, type=VENUE),
            "legs": pyarrow.array([[{"mic": "XPAR", "desk": "EQ"}]] * rows, pyarrow.list_(VENUE)),
            "tags": pyarrow.array(
                [[("a", 1)]] * rows, type=pyarrow.map_(pyarrow.string(), pyarrow.int64())
            ),
            "noise": [i for i in range(rows)],
        }
    )


def target_schema() -> pyarrow.Schema:
    """The declared shape: narrower, reordered, one column the source lacks."""
    return pyarrow.schema(
        [
            pyarrow.field("day", pyarrow.date32(), nullable=False),
            pyarrow.field("symbol", pyarrow.string(), nullable=False),
            pyarrow.field("size", pyarrow.int32(), nullable=False),
            pyarrow.field("venue", WIDER_VENUE),
            pyarrow.field("legs", pyarrow.list_(pyarrow.field("item", WIDER_VENUE))),
            pyarrow.field("tags", pyarrow.map_(pyarrow.string(), pyarrow.int32())),
            pyarrow.field("desk", pyarrow.string()),  # the source never produced it
        ]
    )


def cases(rows: int) -> list[tuple[str, Callable[[], object], Callable[[], object] | None]]:
    """`(name, ours, arrow's own)` -- the second is None when Arrow cannot."""
    batch = source_batch(rows)
    schema = target_schema()
    field = Field.from_arrow_schema(schema)
    matching = field.cast_arrow_batch(batch)  # already the target shape

    fillable = pyarrow.schema([f for f in schema if f.name != "desk"])
    arrow_batch = batch.select([f.name for f in fillable])

    struct_array = batch.column("venue")
    struct_field = Field(name="venue", arrow_type=WIDER_VENUE, nullable=True)
    list_array = batch.column("legs")
    list_field = Field(
        name="legs", arrow_type=pyarrow.list_(pyarrow.field("item", WIDER_VENUE)), nullable=True
    )
    map_array = batch.column("tags")
    map_field = Field(
        name="tags", arrow_type=pyarrow.map_(pyarrow.string(), pyarrow.int32()), nullable=True
    )

    # A batch far wider than the target: the columns nobody asked for must not
    # be paid for. Building a dict of every column here cost 5x this.
    wide = pyarrow.RecordBatch.from_arrays(
        [*batch.columns, *([batch.column("noise")] * 40)],
        schema=pyarrow.schema(
            [*batch.schema, *[pyarrow.field(f"extra{i}", pyarrow.int64()) for i in range(40)]]
        ),
    )

    map_array = batch.column("tags")
    struct_of_map = Field(
        name="tags",
        arrow_type=pyarrow.struct([("a", pyarrow.int64()), ("b", pyarrow.int64())]),
        nullable=True,
    )
    map_of_struct = Field(
        name="venue", arrow_type=pyarrow.map_(pyarrow.string(), pyarrow.string()), nullable=True
    )
    list_of_struct = Field(name="venue", arrow_type=pyarrow.list_(pyarrow.string()), nullable=True)
    large_list = Field(name="legs", arrow_type=pyarrow.large_list(WIDER_VENUE), nullable=True)

    return [
        ("batch, already shaped", lambda: field.cast_arrow_batch(matching), None),
        ("batch, 40 columns over", lambda: field.cast_arrow_batch(wide), None),
        (
            "batch, full reshape",
            lambda: field.cast_arrow_batch(batch),
            lambda: arrow_batch.cast(fillable),
        ),
        (
            "struct, member added",
            lambda: struct_field.cast_arrow_array(struct_array),
            lambda: struct_array.cast(WIDER_VENUE),
        ),
        (
            "list of structs",
            lambda: list_field.cast_arrow_array(list_array),
            lambda: list_array.cast(list_field.arrow_type),
        ),
        (
            "map, narrowed value",
            lambda: map_field.cast_arrow_array(map_array),
            lambda: map_array.cast(map_field.arrow_type),
        ),
        (
            "stream of 16 batches",
            lambda: field.cast_arrow_reader(iter([batch] * 16)).read_all(),
            None,
        ),
        # Shape changes Arrow's own cast refuses outright, so there is nothing
        # to compare them against -- only to keep honest about their cost.
        ("map -> struct", lambda: struct_of_map.cast_arrow_array(map_array), None),
        ("struct -> map", lambda: map_of_struct.cast_arrow_array(struct_array), None),
        ("struct -> list", lambda: list_of_struct.cast_arrow_array(struct_array), None),
        (
            "list -> large list",
            lambda: large_list.cast_arrow_array(list_array),
            lambda: list_array.cast(large_list.arrow_type),
        ),
    ]


def verify(rows: int) -> None:
    """A benchmark that measures the wrong answer measures nothing."""
    batch = source_batch(rows)
    schema = target_schema()
    field = Field.from_arrow_schema(schema)
    cast = field.cast_arrow_batch(batch)
    assert cast.schema.equals(schema), "the batch did not land on the target shape"
    assert cast.column("desk").null_count == rows, "the missing column was not filled"
    for name in ("venue", "legs", "tags"):
        ours = field.field(name).cast_arrow_array(batch.column(name))
        theirs = batch.column(name).cast(field.field(name).arrow_type)
        assert ours.equals(theirs), f"{name}: recursion and arrow disagree"


def sweep(rows: int, repeat: int) -> None:
    verify(min(rows, 1_000))
    print(f"{rows:,} rows per batch, best of {repeat}")
    columns = ("case", "seconds", "rows/s", "arrow s", "vs arrow")
    widths = (22, 9, 12, 9, 9)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))
    for name, ours, theirs in cases(rows):
        scale = 16 if name.startswith("stream") else 1
        seconds = best(ours, repeat)
        arrow_seconds = best(theirs, repeat) if theirs else None
        ratio = f"{arrow_seconds / seconds:>8.2f}x" if arrow_seconds else " " * 9
        arrow_text = f"{arrow_seconds:>9.4f}" if arrow_seconds else " " * 9
        print(f"{name:>22} {seconds:>9.4f} {rows * scale / seconds:>12,.0f} {arrow_text} {ratio}")


def main() -> int:
    arguments = parser(__doc__, rows=200_000, repeat=7).parse_args()
    rows = 50_000 if arguments.quick else arguments.rows
    sweep(rows, 3 if arguments.quick else arguments.repeat)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
