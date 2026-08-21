"""`SqliteFixRegistry`: the same answers as `FixRegistry`, out of an index.

The reference is the JSON registry, and most of what is asserted here is
"these two agree" -- every question, over the same fields, including the ones
whose answer is spelled twice: the four search ranks, the newest-version-first
walk, and "the first declaration of a name wins" are Python there and SQL
here, and a test that only checked the SQL against itself would pass while
they disagreed.

The rest is what only the indexed registry has: the file it keeps, when it
opens it, what a refresh does to the rows that were there, and the boundaries
SQL introduces -- a LIKE metacharacter in a search, a tag compared as text, a
name declared twice inside one version.
"""

import json
import sqlite3
import threading
from pathlib import Path

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix import FixRegistry
from rekep.fix.fields import fix_field
from rekep.fix.sqlite import DATABASE_NAME, SqliteFixRegistry

from .conftest import fixture_page

#: The questions both registries answer, as the calls a caller makes.
QUESTIONS = {
    "field by name": lambda registry: registry.field("Side"),
    "field by tag": lambda registry: registry.field(54, "4.4"),
    "lookup by name": lambda registry: registry.lookup("Side"),
    "lookup by tag in one version": lambda registry: registry.lookup(103, "4.4"),
    "fields of a version": lambda registry: registry.fields("4.4"),
    "tags of everything": lambda registry: registry.tags(),
    "tags of one version": lambda registry: registry.tags("4.4"),
    "search by name": lambda registry: registry.search("side"),
    "search by description": lambda registry: registry.search("rejection"),
    "search by tag": lambda registry: registry.search(54),
    "search, nothing matches": lambda registry: registry.search("zzzzzz"),
    "search, levenshtein": lambda registry: registry.search("Sied"),
}


class FixtureSqliteRegistry(SqliteFixRegistry):
    """The indexed registry over the fixture pages instead of the network."""

    fetched: list[str]

    def _fetch(self, url: str) -> str:
        self.__dict__.setdefault("fetched", []).append(url)
        return fixture_page(url)


class OfflineSqliteRegistry(SqliteFixRegistry):
    """An indexed registry with no network at all.

    Every fetch is refused as `OSError`, which is what being offline looks
    like to a walk over versions -- and recorded, so a test can say not only
    that the answer was right but that nothing was fetched to get it.
    """

    fetched: list[str]

    def _fetch(self, url: str) -> str:
        self.__dict__.setdefault("fetched", []).append(url)
        raise OSError(f"offline: {url}")


class _FixtureJson(FixRegistry):
    """The JSON registry over the fixture pages: the reference, and the dump."""

    def _fetch(self, url: str) -> str:
        return fixture_page(url)


@pytest.fixture
def dumped(tmp_path: Path) -> Path:
    """A JSON dump of the fixture dictionary: the input the index is built from.

    Only `4.4` has fixture pages, so only `4.4` is in the dump -- which is
    also the shape a real half-primed cache has, and the one a walk over
    versions has to skip its way through.
    """
    directory = tmp_path / "dump"
    registry = _FixtureJson(cache_dir=directory)
    assert registry.versions, "the version index lands in the dump too"
    registry.load("4.4")
    return directory


@pytest.fixture
def reference(dumped: Path) -> FixRegistry:
    """The JSON registry over that dump -- what every answer is compared to."""
    return _FixtureJson(cache_dir=dumped)


@pytest.fixture
def indexed(dumped: Path, tmp_path: Path) -> SqliteFixRegistry:
    """The indexed registry over the same dump, in its own file."""
    return OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")


# -- the same answers --------------------------------------------------------


@pytest.mark.parametrize("question", QUESTIONS, ids=list(QUESTIONS))
def test_every_question_is_answered_as_the_json_registry_answers_it(
    question: str, reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    ask = QUESTIONS[question]
    assert ask(indexed) == ask(reference)


def test_the_answers_are_fields_and_not_lookalikes(
    reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    """Equality is the check above; this is what equality is *of*."""
    side = indexed.field("Side")
    assert isinstance(side, Field)
    assert side.name == "Side"
    assert side.arrow_type == pyarrow.string()
    assert side.nullable
    assert side.description == "Side of order."
    assert json.loads(side.fix["values"])["1"] == "Buy"
    assert side.fix["version"] == "4.4"
    assert side.into_dict() == reference.field("Side").into_dict()


def test_a_boolean_field_keeps_its_arrow_type_through_the_index(
    indexed: SqliteFixRegistry,
) -> None:
    """The Arrow type is stored and read back, never re-derived from the name."""
    assert indexed.field(43, "4.4").arrow_type == pyarrow.bool_()


def test_versions_come_back_in_the_same_order(
    reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    assert indexed.versions == reference.versions
    assert indexed.versions[0] == "5.0.SP2", "newest first, out of the version table"


# -- the boundaries SQL introduces -------------------------------------------


@pytest.mark.parametrize("text", ["100%", "px_", "%", "_", "\\", "a%b"])
def test_a_search_term_that_is_a_like_pattern_is_still_a_search_term(
    text: str, reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    """`%` and `_` are wildcards in LIKE and plain characters in a search."""
    assert indexed.search(text) == reference.search(text)


def test_a_wildcard_search_does_not_match_everything(indexed: SqliteFixRegistry) -> None:
    """The escaping above, stated as the failure it prevents."""
    assert indexed.search("%") == [], "an unescaped % would have matched every field"


@pytest.mark.parametrize("text", ["54", "054", "0054", "43"])
def test_a_tag_searched_as_text_matches_the_way_python_matches_it(
    text: str, reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    """`054` is not tag 54: the comparison is on the text, not on the integer."""
    assert indexed.search(text) == reference.search(text)


def test_a_name_declared_twice_in_one_version_resolves_to_the_first(
    dumped: Path, tmp_path: Path
) -> None:
    """The tie that `tags()` and `lookup` both have to break the same way.

    No FIX version declares one name twice, so the case is built here rather
    than waited for: the JSON registry keeps the first declaration, and the
    folded `min()` the index uses has to keep that one too.
    """
    twice = [
        fix_field("Side", 54, "char", version="9.9"),
        fix_field("Side", 4054, "char", version="9.9"),
        fix_field("Other", 55, "String", version="9.9"),
    ]
    (dumped / "9.9.json").write_text(
        json.dumps(
            {"version": "9.9", "url": "", "fields": [member.into_dict() for member in twice]}
        ),
        "utf-8",
    )
    (dumped / "versions.json").write_text(json.dumps({"versions": ["9.9"]}), "utf-8")
    reference = _FixtureJson(cache_dir=dumped)
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert indexed.tags() == reference.tags()
    assert indexed.tags()["side"] == 54, "the first declaration, not the last"
    assert indexed.lookup("Side") == reference.lookup("Side")
    assert len(indexed.lookup("Side")) == 1, "one definition per version, as the index gives"


@pytest.mark.parametrize("limit", [0, 1, 3, 10, 100, -1])
def test_a_limit_cuts_the_same_list_on_both(
    limit: int, reference: FixRegistry, indexed: SqliteFixRegistry
) -> None:
    """Zero, one, more than there are -- and the negative nobody means to pass.

    `LIMIT -1` is *no* limit in SQL and `[: -1]` is all-but-the-last in
    Python, so the one input nobody asks for is the one that would disagree.
    """
    assert indexed.search("s", limit=limit) == reference.search("s", limit=limit)
    assert indexed.search("Sied", limit=limit) == reference.search("Sied", limit=limit)


def test_one_tag_declared_twice_in_a_version_is_refused_by_name(
    dumped: Path, tmp_path: Path
) -> None:
    """Two definitions of one tag in one version is not a version.

    The primary key refuses it either way; this is about the refusal naming
    the dictionary rather than a constraint.
    """
    twice = [fix_field("One", 54, "char"), fix_field("Other", 54, "char")]
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    with pytest.raises(ValueError, match=r"9.9 declares \[54\] more than once"):
        indexed._store_fields("9.9", twice)


def test_a_version_added_later_renumbers_the_ranks(
    dumped: Path, tmp_path: Path, reference: FixRegistry
) -> None:
    """Rank is a position in a list, so a longer list moves it.

    The field rows carry no rank of their own for exactly this reason: a copy
    made today would order tomorrow's lookups by yesterday's list.
    """
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed.load("4.4")
    before = indexed.versions
    indexed._store_fields("6.0", [fix_field("Side", 54, "char", version="6.0")])
    ranked = [name for (name,) in indexed._rows("SELECT name FROM version ORDER BY rank")]
    assert ranked == ["6.0", *before], "the newest version ranks first"
    # The walk itself is the version list this registry already resolved --
    # the same on the JSON registry, where a file appearing mid-run does not
    # join the walk either. A registry opened after it does see it.
    reopened = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert reopened.lookup("Side")[0].fix["version"] == "6.0", "and answers first"


def test_a_field_carrying_metadata_the_columns_do_not_name_survives(
    dumped: Path, tmp_path: Path
) -> None:
    """Anything the schema has no column for rides in `extra`, or is lost."""
    odd = fix_field("Odd", 9001, "String", version="9.9", metadata={"owner": "desk-1"})
    odd.fix["successor"] = "9002"
    (dumped / "9.9.json").write_text(
        json.dumps({"version": "9.9", "url": "", "fields": [odd.into_dict()]}), "utf-8"
    )
    (dumped / "versions.json").write_text(json.dumps({"versions": ["9.9"]}), "utf-8")
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    stored = indexed.field("Odd")
    assert stored == odd
    assert stored.metadata["owner"] == "desk-1"
    assert stored.fix["successor"] == "9002"


def test_a_field_that_is_not_nullable_stays_that_way(dumped: Path, tmp_path: Path) -> None:
    """FIX fields are nullable, but the store must not be the thing deciding."""
    strict = Field(name="Strict", arrow_type=pyarrow.int64(), nullable=False)
    strict.fix["tag"] = "9002"
    (dumped / "9.9.json").write_text(
        json.dumps({"version": "9.9", "url": "", "fields": [strict.into_dict()]}), "utf-8"
    )
    (dumped / "versions.json").write_text(json.dumps({"versions": ["9.9"]}), "utf-8")
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert indexed.field("Strict") == strict
    assert not indexed.field("Strict").nullable


# -- the file ----------------------------------------------------------------


def test_nothing_is_opened_until_something_is_asked(dumped: Path, tmp_path: Path) -> None:
    """A registry is a handle, and building one must not touch the disk."""
    database = tmp_path / "nested" / "fix.db"
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=database)
    assert indexed._connections == [], "no connection until a question is asked"
    assert not database.exists(), "and no file, not even an empty one"
    indexed.field("Side")
    assert database.exists()
    assert len(indexed._connections) == 1


def test_the_database_defaults_to_one_file_beside_the_dump(dumped: Path) -> None:
    indexed = OfflineSqliteRegistry(cache_dir=dumped)
    assert Path(indexed.database) == dumped / DATABASE_NAME


def test_closing_releases_the_file_and_never_opens_one(dumped: Path, tmp_path: Path) -> None:
    """Teardown must not trigger the lazy property, and must be repeatable."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed.close()
    assert not (tmp_path / "fix.db").exists(), "closing an unopened registry opens nothing"
    indexed.field("Side")
    indexed.close()
    indexed.close()
    assert indexed.field("Side").name == "Side", "and asking again opens it back up"


def test_the_registry_is_a_context_manager(dumped: Path, tmp_path: Path) -> None:
    with OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db") as indexed:
        assert indexed.field("Side").name == "Side"
    assert indexed._connections == [], "leaving the block closed the file"


def test_the_repr_says_where_things_are_without_opening_them(dumped: Path, tmp_path: Path) -> None:
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert "fix.db" in repr(indexed)
    assert indexed._connections == [], "a repr that opened the file would be a bug"


def test_two_threads_share_one_registry(dumped: Path, tmp_path: Path) -> None:
    """A parsing pipeline resolves names from whatever thread it is on."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed.load("4.4")
    answers: list[int] = []

    def ask() -> None:
        answers.append(indexed.tags()["side"])

    threads = [threading.Thread(target=ask) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert answers == [54] * 8, "one connection per thread, and no misuse of any of them"
    assert len(indexed._connections) > 1, "each thread opened its own"
    indexed.close()
    assert indexed._connections == [], "and closing closes the ones other threads hold"


# -- indexing, importing, refreshing -----------------------------------------


def test_a_dump_is_imported_without_a_single_fetch(indexed: OfflineSqliteRegistry) -> None:
    """The dump beside the database is read instead of the site, or not at all."""
    assert indexed.load("4.4") == {"4.4": 4}
    assert sorted(indexed.indexed()) == ["4.4"]
    assert indexed.__dict__.get("fetched", []) == [], "nothing was fetched to index it"


def test_only_the_version_that_was_asked_for_is_imported(indexed: SqliteFixRegistry) -> None:
    """The import is per version and on demand, not a whole-directory load."""
    indexed.field(54, "4.4")
    assert sorted(indexed.indexed()) == ["4.4"]


def test_a_version_the_dump_does_not_have_is_scraped_and_stored(
    dumped: Path, tmp_path: Path
) -> None:
    (dumped / "4.4.json").unlink()
    indexed = FixtureSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert len(indexed.fields("4.4")) == 4
    assert indexed.fetched, "the pages were fetched"
    assert indexed.indexed()["4.4"] > 0
    reopened = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert len(reopened.fields("4.4")) == 4, "and stored, so the next registry never fetches"


def test_a_refresh_replaces_the_version_rather_than_merging_into_it(
    dumped: Path, tmp_path: Path
) -> None:
    """A field the site dropped has to disappear here, not linger as a row."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert len(indexed.fields("4.4")) == 4
    indexed._store_fields("4.4", [fix_field("Side", 54, "char", version="4.4")])
    assert [member.name for member in indexed.fields("4.4")] == ["Side"]
    assert indexed.lookup(103, "4.4") == [], "the dropped field is gone, not stale"


def test_verifying_a_whole_index_builds_no_fields_at_all(dumped: Path, tmp_path: Path) -> None:
    """`load()` over an index that is already whole is counting, not reading.

    Counted structurally rather than timed: a `Field` built here is a field
    built to be discarded, and the count of them is what the change was for.
    """

    class Counting(OfflineSqliteRegistry):
        built = 0

        def _field_of(self, row: tuple[object, ...]) -> Field:
            type(self).built += 1
            return super()._field_of(row)

    indexed = Counting(cache_dir=dumped, database=tmp_path / "fix.db")
    assert indexed.load("4.4") == {"4.4": 4}
    assert list(indexed.indexed()) == ["4.4"], "the first call imported the version"
    Counting.built = 0
    assert indexed.load("4.4") == {"4.4": 4}
    assert Counting.built == 0, "verifying it built nothing"
    assert len(indexed.fields("4.4")) == 4
    assert Counting.built == 4, "and reading it does -- so the counter counts"


def test_a_version_stored_with_no_fields_is_not_fetched_again(dumped: Path, tmp_path: Path) -> None:
    """Stored-and-empty is an answer; "no rows" alone would look like a miss."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed._store_fields("9.9", [])
    assert indexed.fields("9.9") == [], "and not a scrape of a version with no pages"
    assert "9.9" in indexed.indexed()


def test_the_index_survives_being_reopened(dumped: Path, tmp_path: Path) -> None:
    """What the file is for: a second process asks and nothing is rebuilt."""
    database = tmp_path / "fix.db"
    first = OfflineSqliteRegistry(cache_dir=dumped, database=database)
    assert first.versions, "the version list is stored the first time it is asked for"
    first.load("4.4")
    first.close()
    for path in dumped.glob("*.json"):
        path.unlink()  # the dump is gone; the index is the whole dictionary now
    second = OfflineSqliteRegistry(cache_dir=dumped, database=database)
    assert second.versions == ("5.0.SP2", "5.0", "4.4", "4.2", "4.0", "FIXT1.1")
    assert second.field("Side").fix["tag"] == "54"
    assert second.tags()["ordrejreason"] == 103


def test_a_version_is_addressed_in_any_case(dumped: Path, tmp_path: Path) -> None:
    """`4.4` in the index is `4.4` however the caller spells the version."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed.load("4.4")
    (dumped / "versions.json").unlink()
    reopened = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    assert [member.name for member in reopened.fields("4.4")] == [
        member.name for member in reopened.fields("4.4".upper())
    ]
    assert reopened.field("side", "4.4").name == "Side"


def test_a_version_that_would_be_a_path_is_still_refused(indexed: SqliteFixRegistry) -> None:
    """The version reaches a query rather than a file name here, and is checked anyway."""
    for hostile in ("../evil", "a/b", "..", "", "4.4'; DROP TABLE field; --"):
        with pytest.raises(ValueError, match="does not name a FIX version"):
            indexed.fields(hostile)


def test_a_version_that_cannot_be_had_is_skipped_by_a_walk(dumped: Path, tmp_path: Path) -> None:
    """Offline over a partial index answers from what it holds, like the JSON one."""
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=tmp_path / "fix.db")
    indexed.fields("4.4")
    for path in dumped.glob("*.json"):
        if path.stem not in ("versions", "4.4"):
            path.unlink()
    # Every other version is now unreachable: no rows, no dump, no network.
    assert [member.fix["version"] for member in indexed.lookup("Side")] == ["4.4"]
    assert indexed.tags()["side"] == 54


def test_indexed_reports_what_the_index_holds(indexed: SqliteFixRegistry) -> None:
    assert indexed.indexed() == {}
    indexed.fields("4.4")
    assert list(indexed.indexed()) == ["4.4"]
    assert indexed.indexed()["4.4"] > 0, "unix seconds, so a stale index can be told"


def test_the_schema_is_what_the_queries_assume(indexed: SqliteFixRegistry) -> None:
    """Derived from the database, then pinned: two tables and two indexes."""
    indexed.load("4.4")
    named = {
        name
        for (name,) in indexed._rows(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index') "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert named == {"version", "field", "field_by_name", "field_by_tag"}
    plan = indexed._rows(
        "EXPLAIN QUERY PLAN SELECT f.tag FROM field f JOIN version v ON v.name = f.version "
        "WHERE f.name_lower = 'side' ORDER BY v.rank, f.tag"
    )
    assert any("field_by_name" in str(row[-1]) for row in plan), "the by-name lookup is indexed"


def test_a_torn_database_is_not_read_as_a_dictionary(dumped: Path, tmp_path: Path) -> None:
    """A file that is not a database says so, rather than answering nothing."""
    database = tmp_path / "fix.db"
    database.write_bytes(b"not a database, just bytes")
    indexed = OfflineSqliteRegistry(cache_dir=dumped, database=database)
    with pytest.raises(sqlite3.DatabaseError):
        indexed.field("Side")
