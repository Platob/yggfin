"""Putting a split back together: `scattered_columns` against `scattered`.

`scattered` is the reference. `scattered_columns` answers the same question for
every column of one split at once, inverting the permutation once instead of
once per column, so the two must agree column for column.
"""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fields.arrays import scattered, scattered_columns

#: One column per Arrow shape a split has to carry back: a value, a null, a list.
COLUMNS: dict[str, tuple[pyarrow.DataType, list]] = {
    "left": (pyarrow.string(), ["a", "b", "c", "d", "e", "f"]),
    "right": (pyarrow.int64(), [1, None, 3, None, 5, 6]),
    "many": (pyarrow.list_(pyarrow.int32()), [[1], [], None, [2, 3], [], [4]]),
}


def split(keys: list[int]) -> tuple[list[dict], list]:
    """`COLUMNS` cut into one part per distinct key, with each part's row positions."""
    parts, positions = [], []
    for key in sorted(set(keys)):
        where = [row for row, one in enumerate(keys) if one == key]
        parts.append(
            {
                name: pyarrow.array([values[row] for row in where], dtype)
                for name, (dtype, values) in COLUMNS.items()
            }
        )
        positions.append(pyarrow.array(where, pyarrow.int64()))
    return parts, positions


@pytest.mark.parametrize(
    "keys",
    [
        [0] * 6,  # one part: nothing moves
        [1, 0, 1, 0, 1, 0],
        [2, 0, 1, 2, 0, 1],
        [0, 1, 2, 3, 4, 5],  # one row per part
    ],
)
def test_scattered_columns_matches_scattered(keys: list[int]) -> None:
    parts, positions = split(keys)
    restored = scattered_columns(parts, positions)
    assert restored == {
        name: scattered([part[name] for part in parts], positions) for name in COLUMNS
    }
    assert {name: column.to_pylist() for name, column in restored.items()} == {
        name: values for name, (_, values) in COLUMNS.items()
    }


def test_scattered_columns_holds_no_row() -> None:
    parts = [{"one": pyarrow.array([], pyarrow.int64())}]
    restored = scattered_columns(parts, [pyarrow.array([], pyarrow.int64())])
    assert restored["one"].to_pylist() == []
