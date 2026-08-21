"""Benchmark the FIX registry: a directory of JSON against an indexed file.

Run from `python/`::

    uv run python benchmarks/bench_fix_registry.py            # full sweep
    uv run python benchmarks/bench_fix_registry.py --quick    # fewer repeats

Both registries answer the same questions from the same published dump
(`data/fix/`, nine versions and 6,479 fields), and every answer is asserted
*equal* before anything is timed -- a benchmark that measures the wrong answer
measures nothing.

Four questions:

1. What does the index buy per question? `field(name)` walks every version,
   `field(tag, version)` walks one, `tags()` folds all of them into one
   mapping, `search` ranks over descriptions, and `fields(version)` hands back
   a whole version. The last one is where the index buys least, and it is
   reported rather than hidden: the work there is building `Field` objects,
   which both registries pay for.
2. What does it cost to hold? The database's bytes against the JSON's, and
   the resident objects each registry needs to answer `tags()`.
3. What does building it cost? Importing the whole dump, swept over the page
   sizes, because that is the one number that decides whether the index is
   built on demand or shipped.
4. Which optimisations were *not* taken, and why. A trigram FTS5 index for
   `search`, a window function for `tags()`, and parsing the Arrow type per
   row rather than per distinct spelling: all three are measured here, and
   two of them are faster. The reasons they are not in `rekep.fix.sqlite`
   are in the numbers beside them.

Every case is warmed once and reported as the best of `--repeat` runs; run the
script twice before quoting a number anywhere.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Callable

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.fields import Field  # noqa: E402
from rekep.fix import FixRegistry  # noqa: E402
from rekep.fix.sqlite import COLUMNS, SqliteFixRegistry  # noqa: E402

#: The published dump this sweeps over: the repository's own `data/fix/`.
DUMP = pathlib.Path(__file__).resolve().parents[2] / "data" / "fix"

#: The questions, as a callable on either registry. `field`/`lookup`/`tags`/
#: `search` are what a job asks; `fields` is what a bulk load asks.
QUESTIONS: dict[str, Callable[[FixRegistry], object]] = {
    "field('Side')  every version": lambda registry: registry.field("Side"),
    "field(54, '4.4')  one version": lambda registry: registry.field(54, "4.4"),
    "lookup('Side')  every version": lambda registry: registry.lookup("Side"),
    "tags()  every version": lambda registry: registry.tags(),
    "tags('4.4')  one version": lambda registry: registry.tags("4.4"),
    "search('reject')": lambda registry: registry.search("reject"),
    "search('Sied')  levenshtein": lambda registry: registry.search("Sied"),
    "fields('4.4')  whole version": lambda registry: registry.fields("4.4"),
}


def best_of(function: Callable[[], object], repeat: int) -> float:
    """Fastest of `repeat` timed calls, after one untimed warm-up."""
    function()
    fastest = float("inf")
    for _ in range(repeat):
        started = time.perf_counter()
        function()
        fastest = min(fastest, time.perf_counter() - started)
    return fastest


def check(json_registry: FixRegistry, sql: SqliteFixRegistry) -> None:
    """The indexed answer *is* the JSON answer, asserted before timing.

    Every question, plus the searches whose ranking is spelled twice -- once
    in Python and once in SQL -- and the LIKE metacharacters that a naive
    pattern would turn into wildcards.
    """
    for label, question in QUESTIONS.items():
        assert question(sql) == question(json_registry), label
    for text in ("reject", "side", 54, "Sied", "100%", "px_", "", "zzzzzz", "TIME", "party"):
        assert sql.search(text) == json_registry.search(text), text
    assert sql.versions == json_registry.versions
    for version in json_registry.versions:
        assert sql.fields(version) == json_registry.fields(version), version


def sweep_questions(json_registry: FixRegistry, database: pathlib.Path, repeat: int) -> None:
    """Cold and warm, question by question. Cold is a registry with nothing in it."""
    print(f"\nquestions, best of {repeat} (ms)")
    print(
        f"{'':>32} {'JSON cold':>12} {'SQLite cold':>12} {'SQLite warm':>12} {'cold speedup':>13}"
    )
    warm = SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0)
    for label, question in QUESTIONS.items():
        cold_json = best_of(lambda q=question: q(FixRegistry(cache_dir=DUMP, retries=0)), repeat)
        cold_sql = best_of(
            lambda q=question: q(SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0)),
            repeat,
        )
        hot = best_of(lambda q=question: q(warm), repeat)
        print(
            f"{label:>32} {cold_json * 1000:>12.3f} {cold_sql * 1000:>12.3f} "
            f"{hot * 1000:>12.3f} {cold_json / cold_sql:>12.1f}x"
        )
    warm.close()


def sweep_footprint(database: pathlib.Path) -> None:
    """What each store weighs on disk, and what answering `tags()` weighs in memory."""
    print("\nfootprint")
    dumped = sum(path.stat().st_size for path in DUMP.glob("*.json"))
    print(f"{'JSON dump':>32} {dumped / 1e6:>8.2f} MB")
    print(f"{'SQLite index':>32} {database.stat().st_size / 1e6:>8.2f} MB")
    for label, build in (
        ("JSON registry", lambda: FixRegistry(cache_dir=DUMP, retries=0)),
        (
            "SQLite registry",
            lambda: SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0),
        ),
    ):
        tracemalloc.start()
        registry = build()
        registry.tags()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        held = f"{label}, resident after tags()"
        print(f"{held:>32} {current / 1e6:>8.2f} MB  peak {peak / 1e6:.2f}")


def sweep_build(repeat: int) -> None:
    """Importing the whole dump, swept over page sizes -- including the bad ones."""
    print(f"\nbuilding the index from the dump, best of {repeat}")
    for page_size in (1024, 4096, 8192, 16384):
        with tempfile.TemporaryDirectory() as scratch:
            database = pathlib.Path(scratch) / "fix.db"

            def build(database: pathlib.Path = database, page_size: int = page_size) -> None:
                if database.exists():
                    database.unlink()
                # The page size is a property of the file, fixed by the first
                # page written -- so it is set here, before the registry opens
                # it and finds a schema to keep rather than one to create.
                made = sqlite3.connect(database)
                made.execute(f"PRAGMA page_size={page_size}")
                made.execute("CREATE TABLE sized(x)")
                made.execute("DROP TABLE sized")
                made.commit()
                made.close()
                registry = SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0)
                registry.load()
                registry.close()

            seconds = best_of(build, repeat)
            size = database.stat().st_size
            sized = f"page_size {page_size}"
            print(f"{sized:>32} {seconds * 1000:>8.1f} ms {size / 1e6:>8.2f} MB")


def sweep_not_taken(database: pathlib.Path, repeat: int) -> None:
    """The three optimisations that were measured and left out."""
    print(f"\nnot taken, best of {repeat} (ms)")
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.execute("PRAGMA mmap_size=268435456")

    like = (
        "SELECT f.name FROM field f JOIN version v ON v.name = f.version "
        "WHERE f.name_lower LIKE :contains OR f.description LIKE :contains "
        "ORDER BY CASE WHEN f.name_lower = :text THEN 0 WHEN f.name_lower LIKE :prefix THEN 1 "
        "WHEN f.name_lower LIKE :contains THEN 2 ELSE 3 END, v.rank, f.tag LIMIT 10"
    )
    fold = (
        "SELECT f.name_lower, min(v.rank * 4294967296 + f.tag) % 4294967296 "
        "FROM field f JOIN version v ON v.name = f.version GROUP BY f.name_lower"
    )
    window = (
        "SELECT name_lower, tag FROM (SELECT f.name_lower, f.tag, ROW_NUMBER() OVER "
        "(PARTITION BY f.name_lower ORDER BY v.rank, f.tag) AS n "
        "FROM field f JOIN version v ON v.name = f.version) WHERE n = 1"
    )
    for label, query, parameters in (
        ("tags(): folded min (taken)", fold, {}),
        ("tags(): window function", window, {}),
        (
            "search: LIKE scan (taken)",
            like,
            {"text": "reject", "prefix": "reject%", "contains": "%reject%"},
        ),
    ):
        seconds = best_of(lambda q=query, p=parameters: connection.execute(q, p).fetchall(), repeat)
        print(f"{label:>32} {seconds * 1000:>8.3f}")

    with tempfile.TemporaryDirectory() as scratch:
        trigram = pathlib.Path(scratch) / "trigram.db"
        shutil.copy(database, trigram)
        indexed = sqlite3.connect(trigram)
        started = time.perf_counter()
        indexed.executescript(
            "CREATE VIRTUAL TABLE field_text USING fts5("
            "  name_lower, description, tokenize='trigram');"
            "INSERT INTO field_text(rowid, name_lower, description) "
            "  SELECT rowid, name_lower, description FROM ("
            "    SELECT ROW_NUMBER() OVER () AS rowid, name_lower, description FROM field);"
        )
        indexed.commit()
        built = time.perf_counter() - started
        matched = best_of(
            lambda: indexed.execute(
                "SELECT rowid FROM field_text WHERE field_text MATCH '\"reject\"' LIMIT 10"
            ).fetchall(),
            repeat,
        )
        short = indexed.execute(
            "SELECT count(*) FROM field_text WHERE field_text MATCH '\"px\"'"
        ).fetchone()[0]
        indexed.close()
        print(
            f"{'search: trigram FTS5 match':>32} {matched * 1000:>8.3f}"
            f"   (+{built * 1000:.0f} ms to build, {trigram.stat().st_size / 1e6:.2f} MB total,"
            f" and {short} rows for a two-letter query -- the reason it is not taken)"
        )

    rows = connection.execute(f"SELECT {COLUMNS} FROM field WHERE version = '4.4'").fetchall()
    types: dict[str, object] = {}

    def per_distinct() -> list[object]:
        types.clear()
        built = []
        for row in rows:
            kind = types.get(row[4])
            if kind is None:
                kind = types[row[4]] = Field.from_dict({"name": row[2], "type": row[4]}).arrow_type
            built.append(kind)
        return built

    def per_row() -> list[object]:
        return [Field.from_dict({"name": row[2], "type": row[4]}).arrow_type for row in rows]

    print(
        f"{'arrow type: one parse per spelling':>32} {best_of(per_distinct, repeat) * 1000:>8.3f}"
    )
    print(f"{'arrow type: one parse per row':>32} {best_of(per_row, repeat) * 1000:>8.3f}")
    connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--quick", action="store_true")
    arguments = parser.parse_args()
    repeat = 3 if arguments.quick else arguments.repeat

    if not (DUMP / "versions.json").exists():
        raise SystemExit(f"no dump to measure: {DUMP} holds no versions.json")
    versions = json.loads((DUMP / "versions.json").read_text("utf-8"))["versions"]
    print(f"{DUMP}: {len(versions)} versions")

    with tempfile.TemporaryDirectory() as scratch:
        database = pathlib.Path(scratch) / "fix.db"
        json_registry = FixRegistry(cache_dir=DUMP, retries=0)
        indexing = time.perf_counter()
        registry = SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0)
        counted = registry.load()
        registry.close()
        print(
            f"indexed {sum(counted.values()):,} fields in "
            f"{(time.perf_counter() - indexing) * 1000:.0f} ms"
        )

        with SqliteFixRegistry(cache_dir=DUMP, database=database, retries=0) as sql:
            check(json_registry, sql)
        print("every answer matches the JSON registry")

        sweep_questions(json_registry, database, repeat)
        sweep_footprint(database)
        sweep_not_taken(database, repeat)
        sweep_build(repeat)


if __name__ == "__main__":
    main()
