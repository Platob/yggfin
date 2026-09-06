"""JSON conversion for generic Rekep configuration dataclasses."""

import dataclasses
import datetime
import enum
import io
import json
import pathlib
from typing import Any

import pyarrow.fs
import pytest

from rekep import Convertible


class Side(enum.StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclasses.dataclass
class Venue(Convertible):
    """A trading venue."""

    mic: str
    timeout: float | None = None


@dataclasses.dataclass
class Book(Convertible):
    """A book of orders."""

    name: str
    opened: datetime.date
    side: Side
    root: pathlib.Path | None = None
    venues: list[Venue] = dataclasses.field(default_factory=list)
    limits: dict[str, int] = dataclasses.field(default_factory=dict)


@pytest.fixture
def book() -> Book:
    return Book(
        name="eu-equities",
        opened=datetime.date(2026, 8, 14),
        side=Side.BUY,
        root=pathlib.Path("/srv/books"),
        venues=[Venue(mic="XPAR", timeout=2.5), Venue(mic="XETR")],
        limits={"gross": 1_000_000, "net": 250_000},
    )


def test_round_trip_through_a_file(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.json"
    book.into_json(path)
    assert Book.from_json(path) == book


def test_round_trip_through_the_generic_forms(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.json"
    book.into_(path)
    assert Book.from_(path) == book


def test_round_trip_through_a_dict(book: Book) -> None:
    assert Book.from_dict(book.into_dict()) == book
    assert Book.from_(book.into_(dict)) == book


def test_nested_and_scalar_types_are_rebuilt(book: Book) -> None:
    loaded = Book.from_json(book.into_json())
    assert all(isinstance(venue, Venue) for venue in loaded.venues)
    assert loaded.venues[0].timeout == 2.5
    assert isinstance(loaded.opened, datetime.date)
    assert isinstance(loaded.side, Side)
    assert isinstance(loaded.root, pathlib.Path)
    assert loaded.limits == {"gross": 1_000_000, "net": 250_000}


@pytest.mark.parametrize("target", [None, str, bytes])
def test_no_destination_returns_the_bytes(book: Book, target: type | None) -> None:
    payload = book.into_json(target)
    assert isinstance(payload, bytes)
    assert payload == book.into_json()
    assert Book.from_json(payload) == book


def test_writing_to_a_destination_returns_nothing(book: Book, tmp_path: pathlib.Path) -> None:
    assert book.into_json(tmp_path / "book.json") is None
    assert book.into_json(io.BytesIO()) is None


def test_none_is_omitted_not_written_as_null() -> None:
    assert "timeout" not in Venue(mic="XETR").into_dict()
    assert json.loads(Venue(mic="XETR").into_json()) == {"mic": "XETR"}
    assert Venue.from_json(Venue(mic="XETR").into_json()).timeout is None


def test_none_inside_a_container_is_kept() -> None:
    @dataclasses.dataclass
    class Slots(Convertible):
        values: list[str | None]
        lookup: dict[str, str | None]
        flags: tuple[int, int | None, int]

    value = Slots(["a", None, "c"], {"k1": "v", "k2": None}, (1, None, 3))
    assert Slots.from_json(value.into_json()) == value


def test_arrow_struct_spellings_decode_to_declared_tuples() -> None:
    @dataclasses.dataclass
    class Pairs(Convertible):
        position: tuple[int, str]
        entries: list[tuple[int, str]]

    assert Pairs.from_dict(
        {
            "position": {"f0": 7, "f1": "seven"},
            "entries": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
        }
    ) == Pairs(position=(7, "seven"), entries=[(1, "one"), (2, "two")])


def test_unknown_keys_are_ignored() -> None:
    document = json.dumps({"mic": "XPAR", "retired": True}).encode()
    assert Venue.from_json(document) == Venue("XPAR")


def test_a_dict_of_any_round_trips_untyped_values() -> None:
    @dataclasses.dataclass
    class Config(Convertible):
        properties: dict[str, Any] = dataclasses.field(default_factory=dict)

    config = Config(properties={"retries": 2, "pool": "default", "enabled": True})
    assert Config.from_json(config.into_json()) == config


def test_enum_dates_and_paths_are_written_as_values(book: Book) -> None:
    payload = book.into_dict()
    assert payload["side"] == "BUY"
    assert payload["opened"] == "2026-08-14"
    assert isinstance(payload["root"], str)


def test_into_dict_needs_a_dataclass() -> None:
    class Loose(Convertible):
        pass

    with pytest.raises(TypeError, match="must be a dataclass"):
        Loose().into_dict()


def test_from_dict_needs_a_mapping() -> None:
    with pytest.raises(TypeError, match="expects a mapping"):
        Venue.from_dict([("mic", "XPAR")])


def test_text_and_binary_file_objects(book: Book) -> None:
    text = io.StringIO()
    binary = io.BytesIO()
    book.into_json(text)
    book.into_json(binary)
    assert Book.from_json(io.StringIO(text.getvalue())) == book
    assert json.loads(binary.getvalue())["name"] == "eu-equities"


def test_accepts_a_string_path_and_file_uri(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.json"
    book.into_json(str(path))
    assert Book.from_json(path.as_uri()) == book


def test_accepts_an_explicit_filesystem(book: Book, tmp_path: pathlib.Path) -> None:
    filesystem = pyarrow.fs.LocalFileSystem()
    path = str(tmp_path / "book.json")
    book.into_json(path, filesystem)
    assert Book.from_json(path, filesystem) == book


def test_json_extension_picks_the_format() -> None:
    assert Convertible.redirect_of("s3://bucket/book.json") == "json"
    assert Convertible.redirect_of(dict) == "dict"


def test_dispatch_uses_a_file_objects_name(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.json"
    with path.open("wb") as handle:
        book.into_(handle)
    assert Book.from_(path) == book


def test_dispatch_refuses_an_unknown_extension(book: Book, tmp_path: pathlib.Path) -> None:
    with pytest.raises(TypeError, match="cannot infer"):
        book.into_(tmp_path / "book.yaml")


def test_yaml_codec_is_not_part_of_the_document_api() -> None:
    assert not hasattr(Convertible, "into_yaml")
    assert not hasattr(Convertible, "from_yaml")
