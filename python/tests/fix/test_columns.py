"""The tags worth a column of their own, held to the standard that defines them.

`rekep.fix.columns` is a declaration -- a schema cannot be fetched, so the list
is written out. What can be checked is that it says what the standard says, and
the standard is now in the store: `FixRegistry.session` is the QuickFIX spec's
own `StandardHeader` and `StandardTrailer`, kept beside the fields.

So these are the tests that stop the declaration and the standard drifting
apart, in the one direction that matters -- a column claiming to be session
layer when the spec never said so.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix.columns import COLUMNS, COMMON, FLAT, QUOTE, SESSION, STAMPS, TAGS
from rekep.fix.registry import FixRegistry

#: The dictionary this repository publishes, beside `python/`.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: Derived from the module, then pinned, so a tag added or dropped fails here
#: rather than quietly changing the shape of every stored log.
EXPECTED_SESSION = 33
EXPECTED_COMMON = 26
EXPECTED_QUOTE = 18


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    return FixRegistry(cache_dir=DATA, offline=True)


def test_the_table_is_the_shape_the_schema_assumes() -> None:
    assert len(SESSION) == EXPECTED_SESSION
    assert len(COMMON) == EXPECTED_COMMON
    assert len(QUOTE) == EXPECTED_QUOTE
    assert (
        len(FLAT)
        == len(COLUMNS)
        == len(TAGS)
        == (EXPECTED_SESSION + EXPECTED_COMMON + EXPECTED_QUOTE)
    )
    assert len({name for _, name in FLAT}) == len(FLAT), "no two tags claim one column"
    assert len({tag for tag, _ in FLAT}) == len(FLAT), "and no tag is listed twice"


def test_every_session_column_is_one_the_standard_puts_in_every_message(
    registry: FixRegistry,
) -> None:
    """The declaration says "session layer"; the spec is what decides that.

    Checked across every version the dictionary holds rather than one, because
    a field the standard moved into the header at 4.4 is still session layer
    and one it never had is not.
    """
    known = {name for version in registry.versions for name, _ in registry.session(version)}
    assert known, "the published dictionary carries the spec's session layer"
    for tag, column in SESSION:
        assert registry.field(tag).name in known, f"{column} ({tag}) is not in any header"


def test_every_field_the_standard_requires_has_a_column(registry: FixRegistry) -> None:
    """The other direction: a mandatory field with nowhere to land is a hole."""
    required = {
        name for version in registry.versions for name, must in registry.session(version) if must
    }
    assert required == {
        "BeginString",
        "BodyLength",
        "MsgType",
        "SenderCompID",
        "TargetCompID",
        "MsgSeqNum",
        "SendingTime",
        "CheckSum",
    }
    declared = {registry.field(tag).name for tag, _ in SESSION}
    assert required <= declared


def test_no_common_field_is_really_session_layer(registry: FixRegistry) -> None:
    """The two sets answer different questions, so nothing belongs to both."""
    session = {name for version in registry.versions for name, _ in registry.session(version)}
    for tag, column in COMMON:
        assert registry.field(tag).name not in session, f"{column} ({tag}) is a header field"


def test_a_lifted_stamp_is_a_field_the_dictionary_calls_a_timestamp(
    registry: FixRegistry,
) -> None:
    """The physical UTC projection and registry timestamp types agree."""
    for tag in STAMPS:
        assert registry.field(tag).dtype == pyarrow.timestamp("ns"), tag
    timestamps = {tag for tag, _ in FLAT if registry.field(tag).dtype == pyarrow.timestamp("ns")}
    assert timestamps == STAMPS, "and every lifted timestamp is in it"


# -- a model annotates by name, and the tag stays in the dictionary -----------


def test_no_model_writes_a_fix_tag_down() -> None:
    """A column used to be annotated `DECLARATIONS[35]`, which states name-to-tag
    a second time: the attribute beside it is already the FIX name, and a
    mistyped number annotated a different field with nothing to catch it.

    `DECLARED["MsgType"]` cannot be mistyped into another field -- a name this
    registry has not got raises at import.
    """
    import re

    literal = re.compile(r"DECLARATIONS\[\s*\d+\s*\]")
    root = Path(__file__).resolve().parents[2] / "src" / "rekep"
    found = {
        f"{path.relative_to(root)}:{number}"
        for path in sorted(root.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if literal.search(line)
    }
    assert found == set()


def test_every_annotated_name_is_the_field_the_registry_answers_for() -> None:
    """Which is what makes the name a safe key: it resolves, or the import fails."""
    from rekep.fix.columns import DECLARATIONS, DECLARED

    assert set(DECLARED) == {member.fix.canonical for member in DECLARATIONS.values()}
    assert len(DECLARED) == len(DECLARATIONS), "one name per tag, and no name twice"
    for name, member in DECLARED.items():
        assert member.fix.canonical == name
        assert DECLARATIONS[member.fix.tag] is member


def test_the_scheme_of_an_identifier_is_the_dictionary_s_answer() -> None:
    """`SecurityIDSource <22>` enumerates every scheme, so this package compiles none."""
    from rekep.fix.columns import ISIN_SCHEME, id_scheme, id_schemes

    assert len(id_schemes()) > 30, "the whole enumeration, not a subset written here"
    assert id_scheme("4") == ISIN_SCHEME
    assert id_scheme(ISIN_SCHEME) == ISIN_SCHEME, "by name as well as by wire value"
    assert id_scheme("~") == "", "a scheme the dictionary does not declare names nothing"
