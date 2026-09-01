"""Every FIX tag the market shapes declare, checked against the published dictionary.

A tag is a number typed from memory, and a transposed one does not look wrong:
`ClOrdID <11>` written as `<14>` labels the column `CumQty` and nothing in the
code, the schema or the contract ever says so. Names and tags are checked
against `data/fix.zip` plus the two FIX Latest extension fields pinned here --
read without the registry, so the test does not depend on the code it checks.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix.columns import NAMESPACE_FIELDS
from rekep.fix.entries import NAMESPACE, record_kind
from rekep.fix.fields import arrow_type_of
from rekep.market import Book, Execution, Instrument, Level, MarketEvent, Order

#: The archive this repository publishes, from the repository and not the wheel.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

SHAPES = (MarketEvent, Order, Execution, Book, Instrument, Level)

#: FIX datatypes this package deliberately stores as a different Arrow type.
#: An enumerated `char` or `int` becomes a packed ASCII code, as wide as the
#: enum that declares it -- four bytes for `Side`, eight for `State` -- and
#: Currency packs its short code the same way. Each remains lossless for its
#: domain.
NARROWED = {
    "char": (pyarrow.int32(), pyarrow.int64()),
    "currency": (pyarrow.int32(),),
    "int": (pyarrow.int32(), pyarrow.int64()),
    # FIX-backed persisted temporals use Iceberg's microsecond timestamp width.
    # A local market date starts at midnight without claiming a timezone, and
    # the timestamp can retain a clock when a venue's rule supplies one.
    "localmktdate": (pyarrow.timestamp("us"),),
}

#: Fields narrowed by name rather than by datatype. `SecurityIDSource <22>` is
#: enumerated exactly like the `char` fields above, while `LastMkt <30>` carries
#: an ISO 10383 MIC. FIX types both as general text, so admitting their broad
#: datatypes here would let unrelated strings narrow silently.
NARROWED_FIELDS = {
    "SecurityIDSource": (pyarrow.int32(),),
    "LastMkt": (pyarrow.int32(),),
}


def dictionary() -> dict[str, dict[str, Any]]:
    """Every field record by every spelling it answers to, canonical names first.

    A record already holds the **newest** reading -- which matters and was got
    wrong once: FIX 4.0 typed `OrderQty` as `int` and `ExecID` as `int`, and
    both became `Qty` and `String` in 4.2, so a lookup that took the oldest
    definition called every quantity column in this package mistyped.

    Read straight out of the archive's own shards: a test that went through
    `FixRegistry` would pass whenever the registry and the declaration were
    wrong together.
    """
    by_name: dict[str, dict[str, Any]] = {}
    with zipfile.ZipFile(DATA) as archive:
        records = [
            record
            for name in sorted(archive.namelist())
            if name.startswith("fields/")
            for record in json.loads(archive.read(name).decode("utf-8")).values()
        ]
    for aliased in (False, True):
        for record in records:
            # A stored record is a field document: the Arrow reading at the
            # top and the protocol's own, including nested metadata, under
            # `fix`.
            fix = record["fix"]
            if "tag" not in fix:
                continue  # A field FIX never numbered; nothing here declares one.
            metadata = {"fix:tag": fix["tag"]}
            if fix.get("type"):
                metadata["fix:type"] = fix["type"]
            spellings = (
                [alias["name"] for alias in fix.get("aliases", [])] if aliased else [record["name"]]
            )
            for spelled in spellings:
                by_name.setdefault(spelled, {"name": spelled, "metadata": metadata})
    return by_name


def tagged(shape: type) -> list[tuple[str, Field]]:
    """Every member of `shape`, at any depth, that names a FIX field."""
    found: list[tuple[str, Field]] = []

    def walk(prefix: str, members: tuple[Field, ...]) -> None:
        for member in members:
            path = f"{prefix}{member.name}"
            if member.fix.type:
                found.append((path, member))
            walk(f"{path}.", member.fields)

    walk(f"{shape.__name__}.", shape.into_field().fields)
    return found


# The archive is the last versioned baseline (FIX 5.0 SP2); these fields were
# added by extension packs and are checked against FIX 5.0 SP2 independently.
LATEST_FIELDS = {
    "ExposureDuration": {
        "name": "ExposureDuration",
        "type": "int",
        "nullable": True,
        "description": "Duration for which an order remains exposed.",
        "metadata": {
            "fix:tag": "1629",
            "fix:type": "int",
            "fix:version": "5.0.SP2",
        },
    },
    "ExposureDurationUnit": {
        "name": "ExposureDurationUnit",
        "type": "int",
        "nullable": True,
        "description": "Time unit in which ExposureDuration is expressed.",
        "metadata": {
            "fix:tag": "1916",
            "fix:type": "int",
            "fix:version": "5.0.SP2",
        },
    },
}

FIELDS = {**dictionary(), **LATEST_FIELDS}
DECLARED = [(path, member) for shape in SHAPES for path, member in tagged(shape)]


def test_the_dictionary_is_read_newest_first() -> None:
    """The lookup this file depends on, pinned against the change that broke it."""
    assert FIELDS["OrderQty"]["metadata"]["fix:type"] == "Qty", "4.0 said int"
    assert FIELDS["ExecID"]["metadata"]["fix:type"] == "String", "4.0 said int"
    assert FIELDS["MsgSeqNum"]["metadata"]["fix:type"] == "SeqNum", "4.0 said int"


def test_the_shapes_declare_fix_fields_at_all() -> None:
    """A walk that found nothing would make every test below pass vacuously."""
    assert len(DECLARED) > 40, len(DECLARED)
    assert any(path.count(".") > 1 for path, _ in DECLARED), "and it reached a nested one"


@pytest.mark.parametrize("path,member", DECLARED, ids=[path for path, _ in DECLARED])
def test_every_declared_tag_is_the_one_the_dictionary_gives_that_name(
    path: str, member: Field
) -> None:
    name = member.fix["name"]
    if record_kind(member) == NAMESPACE:
        # A rendered identity FIX never numbered: the registry's namespace
        # record is its declaration, and claiming a tag would be the mislabel.
        assert name in NAMESPACE_FIELDS, f"{path} names {name!r}, which no record declares"
        assert "tag" not in member.fix, path
        return
    assert name in FIELDS, f"{path} names {name!r}, which is in no FIX version"
    assert member.fix["tag"] == FIELDS[name]["metadata"]["fix:tag"], path


@pytest.mark.parametrize("path,member", DECLARED, ids=[path for path, _ in DECLARED])
def test_every_declared_type_is_the_fix_one_or_a_deliberate_narrowing(
    path: str, member: Field
) -> None:
    """The other half of a mislabel: the right tag on a column of the wrong type."""
    if record_kind(member) == NAMESPACE:
        declared = NAMESPACE_FIELDS[member.fix["name"]]
        assert member.dtype == declared.dtype, path
        return
    datatype = FIELDS[member.fix["name"]]["metadata"].get("fix:type", "")
    expected = arrow_type_of(datatype)
    if member.dtype == expected:
        return
    narrowed = NARROWED_FIELDS.get(member.fix["name"]) or NARROWED.get(datatype.lower(), ())
    assert member.dtype in narrowed, (
        f"{path} is {member.dtype} where FIX {datatype!r} is {expected}"
    )


def test_an_int32_narrowing_is_explicit_for_its_fix_datatype() -> None:
    """Every narrowed protocol value must be named in the compatibility table."""
    for path, member in DECLARED:
        if member.dtype != pyarrow.int32():
            continue
        if member.fix["name"] in NARROWED_FIELDS:
            continue
        datatype = FIELDS[member.fix["name"]]["metadata"].get("fix:type", "").lower()
        assert datatype in NARROWED, f"{path} narrowed a FIX {datatype!r}"


def test_the_tags_that_only_a_late_version_defines_are_still_found() -> None:
    """Several columns here are FIX 5.0 fields; a 4.4-only lookup would miss them."""
    late = {
        "TradeID": "1003",
        "AggressorIndicator": "1057",
        "MinPriceIncrement": "969",
        "ExposureDuration": "1629",
        "ExposureDurationUnit": "1916",
    }
    for name, tag in late.items():
        assert FIELDS[name]["metadata"]["fix:tag"] == tag
    declared = {member.fix["name"] for _, member in DECLARED}
    assert late.keys() & declared, "and the shapes actually use one of them"


def test_one_fix_field_is_spelled_the_same_wherever_it_appears() -> None:
    """`ClOrdID` on an order and on an execution must be one column, not two."""
    by_name: dict[str, set[tuple[str, Any]]] = {}
    for _, member in DECLARED:
        by_name.setdefault(member.fix["name"], set()).add((member.name, member.dtype))
    for name, spellings in by_name.items():
        if name == "Symbol":
            assert spellings == {("symbol", pyarrow.string())}, (
                "one spelling now: Symbol belongs to the Instrument contract"
            )
            continue
        if name == "Currency":
            assert spellings == {
                ("currency", pyarrow.int32()),
                ("currency", pyarrow.int32()),
            }
            continue
        assert len(spellings) == 1, f"{name} is spelled {sorted(map(str, spellings))}"
