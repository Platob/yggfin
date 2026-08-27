"""Instances into columns: what `into_arrow_array` promises.

The declaration is the authority on the type, and the objects are the values.
So every check here is the same one twice: the column the builder assembles
member by member must be the column a document projection produced row by row,
byte for byte -- and it must be exactly the type the class declares.
"""

from __future__ import annotations

import dataclasses
import io

import pyarrow
import pyarrow.ipc
import pyarrow.parquet
import pytest

from rekep import Convertible, Field, scalar


def written(batch: pyarrow.RecordBatch) -> bytes:
    """One batch's bytes, so NaN compares as the buffer it is and not as a value."""
    sink = io.BytesIO()
    with pyarrow.ipc.new_stream(sink, batch.schema) as writer:
        writer.write(batch)
    return sink.getvalue()


@scalar
class Level(Convertible):
    """One price and the size resting on it."""

    px: float | None = None
    qty: float | None = None


@scalar
class Venue(Convertible):
    """Where a quote came from."""

    mic: str = ""
    tier: int | None = None


@scalar
class Quote(Convertible):
    """A quote, with a nested venue and two repeating groups."""

    symbol: str = ""
    unix: int = 0
    venue: Venue | None = None
    bids: list[Level] | None = None
    tags: dict[str, str] | None = None
    payload: bytes | None = None


ROWS = [
    Quote(
        symbol="BTC-USD",
        unix=7,
        venue=Venue(mic="XCME", tier=1),
        bids=[Level(px=100.0, qty=1.0), Level(px=99.5, qty=2.0)],
        tags={"src": "feed"},
        payload=b"\x01\x02",
    ),
    Quote(symbol="ETH-USD", unix=8),
    Quote(symbol="SOL-USD", unix=9, venue=Venue(mic="XNAS"), bids=[]),
]


def document(rows: list[Quote]) -> pyarrow.RecordBatch:
    """The projection this replaces: one dictionary per row."""
    schema = Quote.into_field().into_arrow_schema()
    return pyarrow.RecordBatch.from_pylist([row.into_dict() for row in rows], schema=schema)


def test_a_batch_is_the_document_projection() -> None:
    """The same array by Arrow's own definition, and the same values read back.

    Not byte for byte: under a row that is null a member holds no value, and
    whether the buffer records that as a null or as the type's zero is not
    observable through the mask above it. Everything a reader can see is equal.
    """
    built, expected = Quote.into_arrow_batch(ROWS), document(ROWS)

    assert built.equals(expected)
    assert built.to_pylist() == expected.to_pylist()


def test_a_required_member_carries_no_null_a_writer_would_refuse() -> None:
    """Parquet refuses a `NOT NULL` column holding one, and Iceberg writes parquet."""
    sink = io.BytesIO()
    pyarrow.parquet.write_table(pyarrow.Table.from_batches([Quote.into_arrow_batch(ROWS)]), sink)
    sink.seek(0)

    assert pyarrow.parquet.read_table(sink).to_pylist() == document(ROWS).to_pylist()


def test_a_batch_carries_the_schema_the_class_declares() -> None:
    """Including its metadata: a batch that lost it no longer says whose it is."""
    built = Quote.into_arrow_batch(ROWS)

    assert built.schema.equals(Quote.into_field().into_arrow_schema())
    assert built.num_rows == len(ROWS)


def test_an_array_is_the_struct_the_class_is() -> None:
    array = Quote.into_arrow_array(ROWS)

    assert array.type == Quote.into_field().dtype
    assert array.to_pylist() == document(ROWS).to_struct_array().to_pylist()


def test_no_rows_still_answer_with_the_declared_type() -> None:
    """An empty batch is a schema, and a reader downstream still needs one."""
    built = Quote.into_arrow_batch([])

    assert built.num_rows == 0
    assert built.schema.equals(Quote.into_field().into_arrow_schema())


def test_a_null_row_is_null_in_every_member() -> None:
    array = Quote.into_arrow_array([ROWS[0], None, ROWS[1]])

    assert array.is_valid().to_pylist() == [True, False, True]
    assert array.to_pylist()[1] is None


def test_a_nested_class_and_a_group_recurse_rather_than_flatten() -> None:
    built = Quote.into_arrow_batch(ROWS)

    assert built.column("venue").to_pylist() == [
        {"mic": "XCME", "tier": 1},
        None,
        {"mic": "XNAS", "tier": None},
    ]
    assert built.column("bids").to_pylist() == [
        [{"px": 100.0, "qty": 1.0}, {"px": 99.5, "qty": 2.0}],
        None,
        [],
    ]


def test_a_class_spells_a_member_its_column_holds_differently() -> None:
    """The one hook: asked per member, never per row."""

    @scalar
    class Stamped(Convertible):
        """A row whose identity is an integer in hand and bytes in a column."""

        ident: bytes | None = None
        label: str = ""

        @staticmethod
        def into_column_value(name: str, value: object) -> object:
            return value.to_bytes(4, "big") if name == "ident" and value is not None else value

    built = Stamped.into_arrow_batch(
        [Stamped(ident=1, label="one"), Stamped(label="none")]  # type: ignore[arg-type]
    )

    assert built.column("ident").to_pylist() == [b"\x00\x00\x00\x01", None]


def test_a_column_python_cannot_spell_is_read_off_its_attribute() -> None:
    """The rename `into_field_columns` records is the one the builder reads."""
    schema = pyarrow.schema([("yield", pyarrow.float64()), ("symbol", pyarrow.string())])
    built = Field.from_arrow_schema(schema).into_dataclass("Bond")

    batch = built.into_arrow_batch([built(yield_=1.5, symbol="US"), built(symbol="FR")])

    assert batch.column("yield").to_pylist() == [1.5, None]
    assert batch.schema.names == ["yield", "symbol"]


@pytest.mark.parametrize("size", [1, 2, 64])
def test_the_row_count_is_whatever_was_handed_over(size: int) -> None:
    rows = [ROWS[index % len(ROWS)] for index in range(size)]

    assert Quote.into_arrow_batch(rows).num_rows == size


def test_a_plain_dataclass_field_still_projects() -> None:
    """A member Arrow reads itself -- a map, a list of leaves -- is not walked."""

    @scalar
    class Plain(Convertible):
        """Shapes the builder hands straight to Arrow."""

        names: list[str] | None = None
        counts: dict[str, int] | None = None

    rows = [Plain(names=["a", "b"], counts={"a": 1}), Plain()]
    schema = Plain.into_field().into_arrow_schema()

    assert written(Plain.into_arrow_batch(rows)) == written(
        pyarrow.RecordBatch.from_pylist([row.into_dict() for row in rows], schema=schema)
    )


def test_the_declared_members_are_what_is_read_not_the_attributes_present() -> None:
    """A private attribute a class hides is not a column and is not read."""
    declared = {member.name for member in dataclasses.fields(Quote)}

    assert set(Quote.into_arrow_batch(ROWS).schema.names) == declared
