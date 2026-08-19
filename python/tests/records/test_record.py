import dataclasses
import datetime
import enum
import io
import json
import pathlib
import sys
import tomllib

import pyarrow.fs
import pytest
import yaml

from rekep import Record, record

FORMATS = ["yaml", "toml", "json"]


class Side(enum.StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@record
class Venue(Record):
    """A trading venue.

    Attributes:
        mic: ISO 10383 market identifier.
        timeout: Seconds before giving up on a quote.
    """

    mic: str
    timeout: float | None = None


@record
class Book(Record):
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


# -- the record decorator ---------------------------------------------------


def test_record_makes_a_dataclass() -> None:
    assert dataclasses.is_dataclass(Venue)
    assert [f.name for f in dataclasses.fields(Venue)] == ["mic", "timeout"]


def test_double_underscore_annotations_are_not_fields() -> None:
    @record
    class Cached(Record):
        mic: str
        __cache: dict = {}

    assert [f.name for f in dataclasses.fields(Cached)] == ["mic"]
    assert Cached(mic="XPAR").into_dict() == {"mic": "XPAR"}


def test_hidden_annotation_keeps_its_value_as_a_class_attribute() -> None:
    @record
    class Cached(Record):
        mic: str
        __cache: dict = {}

    assert Cached._Cached__cache == {}


def test_a_mutable_default_would_have_broken_a_plain_dataclass() -> None:
    """`__`-hiding happens before dataclass runs, so `{}` never reaches it."""
    with pytest.raises(ValueError, match="mutable default"):

        @dataclasses.dataclass
        class Plain:
            cache: dict = {}


def test_record_forwards_dataclass_keywords() -> None:
    @record(frozen=True, order=True)
    class Frozen(Record):
        mic: str

    assert Frozen(mic="A") < Frozen(mic="B")
    with pytest.raises(dataclasses.FrozenInstanceError):
        Frozen(mic="A").mic = "B"


def test_record_works_bare_and_called() -> None:
    @record
    class Bare(Record):
        mic: str

    @record()
    class Called(Record):
        mic: str

    assert dataclasses.is_dataclass(Bare)
    assert dataclasses.is_dataclass(Called)


# -- round trips ------------------------------------------------------------


@pytest.mark.parametrize("fmt", FORMATS)
def test_round_trip_through_a_file(book: Book, tmp_path: pathlib.Path, fmt: str) -> None:
    path = tmp_path / f"book.{fmt}"
    getattr(book, f"into_{fmt}")(path)
    assert getattr(Book, f"from_{fmt}")(path) == book


@pytest.mark.parametrize("fmt", FORMATS)
def test_round_trip_through_the_generic_forms(book: Book, tmp_path: pathlib.Path, fmt: str) -> None:
    path = tmp_path / f"book.{fmt}"
    book.into_(path)
    assert Book.from_(path) == book


def test_round_trip_through_a_dict(book: Book) -> None:
    assert Book.from_dict(book.into_dict()) == book
    assert Book.from_(book.into_(dict)) == book


def test_nesting_is_rebuilt_not_left_as_dicts(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.yaml"
    book.into_yaml(path)
    loaded = Book.from_yaml(path)
    assert all(isinstance(venue, Venue) for venue in loaded.venues)
    assert loaded.venues[0].timeout == 2.5


@pytest.mark.parametrize("fmt", FORMATS)
def test_types_survive_the_round_trip(book: Book, tmp_path: pathlib.Path, fmt: str) -> None:
    path = tmp_path / f"book.{fmt}"
    book.into_(path)
    loaded = Book.from_(path)
    assert isinstance(loaded.opened, datetime.date)
    assert isinstance(loaded.side, Side)
    assert isinstance(loaded.root, pathlib.Path)
    assert loaded.limits == {"gross": 1_000_000, "net": 250_000}


# -- bytes in, bytes out ----------------------------------------------------


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("target", [None, str, bytes])
def test_no_destination_returns_the_bytes(book: Book, fmt: str, target: type | None) -> None:
    payload = getattr(book, f"into_{fmt}")(target)
    assert isinstance(payload, bytes)
    assert payload == getattr(book, f"into_{fmt}")()


@pytest.mark.parametrize("fmt", FORMATS)
def test_bytes_round_trip_without_touching_a_filesystem(book: Book, fmt: str) -> None:
    payload = getattr(book, f"into_{fmt}")()
    assert getattr(Book, f"from_{fmt}")(payload) == book


def test_writing_to_a_destination_returns_nothing(book: Book, tmp_path: pathlib.Path) -> None:
    assert book.into_json(tmp_path / "book.json") is None
    assert book.into_json(io.BytesIO()) is None


def test_returned_bytes_match_what_is_written(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.yaml"
    book.into_yaml(path)
    assert path.read_bytes() == book.into_yaml()


# -- encoding rules ---------------------------------------------------------


def test_none_is_omitted_not_written_as_null(book: Book) -> None:
    """TOML has no null; a missing key is what lets the default apply on load."""
    assert "timeout" not in book.into_dict()["venues"][1]
    assert tomllib.loads(Venue(mic="XETR").into_toml().decode()) == {"mic": "XETR"}
    assert Venue.from_toml(Venue(mic="XETR").into_toml()).timeout is None


def test_scalars_precede_tables_in_toml() -> None:
    """A bare key after a table header would silently land inside that table."""

    @record
    class TableFirst(Record):
        venues: list[Venue]
        name: str

    payload = TableFirst(venues=[Venue(mic="XPAR")], name="after-the-table").into_toml()
    assert tomllib.loads(payload.decode())["name"] == "after-the-table"


def test_unknown_keys_are_ignored() -> None:
    assert Venue.from_json(json.dumps({"mic": "XPAR", "retired": True}).encode()) == Venue("XPAR")


def test_enum_is_written_as_its_value(book: Book) -> None:
    assert book.into_dict()["side"] == "BUY"


def test_dates_and_paths_are_written_as_text(book: Book) -> None:
    payload = book.into_dict()
    assert payload["opened"] == "2026-08-14"
    assert isinstance(payload["root"], str)


def test_into_dict_needs_a_dataclass() -> None:
    class Loose(Record):
        pass

    with pytest.raises(TypeError, match="must be a dataclass"):
        Loose().into_dict()


# -- targets ----------------------------------------------------------------


def test_writes_to_a_text_file_object(book: Book) -> None:
    buffer = io.StringIO()
    book.into_yaml(buffer)
    assert yaml.safe_load(buffer.getvalue())["name"] == "eu-equities"


def test_writes_to_a_binary_file_object(book: Book) -> None:
    buffer = io.BytesIO()
    book.into_json(buffer)
    assert json.loads(buffer.getvalue())["name"] == "eu-equities"


def test_reads_from_a_file_object(book: Book) -> None:
    assert Book.from_json(io.StringIO(book.into_json().decode())) == book


def test_accepts_a_string_path(book: Book, tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / "book.json")
    book.into_json(path)
    assert Book.from_json(path) == book


def test_accepts_a_file_uri(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.json"
    book.into_json(path.as_uri())
    assert Book.from_json(path.as_uri()) == book


def test_accepts_an_explicit_filesystem(book: Book, tmp_path: pathlib.Path) -> None:
    filesystem = pyarrow.fs.LocalFileSystem()
    path = str(tmp_path / "book.yaml")
    book.into_yaml(path, filesystem)
    assert Book.from_yaml(path, filesystem) == book


# -- dispatch ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "stem"),
    [
        ("book.yaml", "yaml"),
        ("book.yml", "yaml"),
        ("book.toml", "toml"),
        ("book.json", "json"),
    ],
)
def test_extension_picks_the_format(name: str, stem: str) -> None:
    assert Record.redirect_of(name) == stem


def test_dispatch_uses_a_file_objects_name(book: Book, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "book.toml"
    with path.open("wb") as handle:
        book.into_(handle)
    assert Book.from_(path) == book


def test_dispatch_refuses_an_unknown_extension(book: Book, tmp_path: pathlib.Path) -> None:
    with pytest.raises(TypeError, match="cannot infer"):
        book.into_(tmp_path / "book.ini")


# -- optional formats -------------------------------------------------------


def test_json_needs_no_optional_dependency(book: Book) -> None:
    """Blocking both extras must not stop JSON, which is stdlib all the way."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, "yaml", None)
        patch.setitem(sys.modules, "tomli_w", None)
        assert Book.from_json(book.into_json()) == book


def test_reading_toml_needs_no_optional_dependency(book: Book) -> None:
    """`tomllib` is stdlib; only writing TOML pulls a package."""
    payload = book.into_toml()
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, "tomli_w", None)
        assert Book.from_toml(payload) == book


@pytest.mark.parametrize(
    ("module", "extra", "call"),
    [
        ("yaml", "yaml", lambda b: b.into_yaml()),
        ("yaml", "yaml", lambda b: Book.from_yaml(b"name: x")),
        ("tomli_w", "toml", lambda b: b.into_toml()),
    ],
)
def test_a_missing_extra_is_named_in_the_error(book: Book, module: str, extra: str, call) -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, module, None)
        with pytest.raises(ImportError, match=rf"pip install rekep\[{extra}\]"):
            call(book)
