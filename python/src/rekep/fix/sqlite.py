"""The FIX dictionary as one indexed file: a question is a query, not a load."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import pathlib
import sqlite3
import threading
import time
from functools import cached_property
from typing import Any

import pyarrow

from rekep.fields import Field
from rekep.fix.registry import FixRegistry, _is_tag, _levenshtein, _version_key

#: What a registry indexes into when it is handed a directory: one database
#: beside the JSON it was built from, so a dump directory and its index are
#: one thing to copy, and the JSON stays the diffable document it is.
DATABASE_NAME = "fix.db"

#: Set before the schema, because `page_size` is only honoured on an empty
#: database (or across a VACUUM). Measured over the whole dictionary, cold
#: connection per query: 8192 holds it in 2.7 MB against 3.0 MB at the 4096
#: default, and reads it a shade faster; `mmap_size` is worth ~20% on the
#: queries that scan the table and a rounding error on the ones that do not.
#: `cache_size` and `temp_store=MEMORY` were measured too, and changed
#: nothing here -- the whole file is smaller than the default cache.
#: `busy_timeout` is not about speed: it is what makes a second process
#: scraping into the same index wait for the writer rather than raise.
PRAGMAS = """
PRAGMA page_size=8192;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA mmap_size=268435456;
PRAGMA busy_timeout=5000;
"""

#: Two tables, and both are read far more often than they are written.
#:
#: `field` is keyed by `(version, tag)` -- the pair a definition *is* -- and
#: `WITHOUT ROWID` so that key holds the row rather than pointing at it. The
#: version's rank lives in `version` alone and is joined for, never copied
#: onto the field rows: a new FIX version shifts every rank, and a copy would
#: silently order a whole index by the ranking of the day it was written.
#:
#: `name_lower` is stored rather than computed, because every lookup by name
#: and every search goes through it; `values`, `used_in` and `extra` are the
#: JSON the metadata already carries, kept as text so reading a field costs
#: no parsing at all.
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS version(
  name      TEXT NOT NULL PRIMARY KEY,
  rank      INTEGER NOT NULL,
  url       TEXT NOT NULL DEFAULT '',
  stored_at REAL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS field(
  version     TEXT NOT NULL,
  tag         INTEGER NOT NULL,
  name        TEXT NOT NULL,
  name_lower  TEXT NOT NULL,
  arrow_type  TEXT NOT NULL,
  nullable    INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  fix_type    TEXT,
  fix_version TEXT,
  values_json TEXT,
  used_in     TEXT,
  note        TEXT,
  extra       TEXT,
  PRIMARY KEY (version, tag)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS field_by_name ON field(name_lower, tag);
CREATE INDEX IF NOT EXISTS field_by_tag ON field(tag);
"""

#: The columns a `Field` is rebuilt from, in one place: the select list, the
#: insert's placeholders and `_field_of`'s unpacking are the same order or
#: they are a bug.
COLUMNS = (
    "version, tag, name, name_lower, arrow_type, nullable, description, "
    "fix_type, fix_version, values_json, used_in, note, extra"
)

#: One placeholder per column, counted from the columns so the two cannot drift.
_PLACEHOLDERS = ",".join("?" * len(COLUMNS.split(",")))

#: The metadata keys that have a column of their own; anything else a field
#: carries rides in `extra`, so indexing a hand-edited dump loses nothing.
KNOWN = frozenset(
    {"description", "fix:tag", "fix:type", "fix:version", "fix:values", "fix:used_in", "fix:note"}
)

#: What this module's `SCHEMA` is, written into `meta` when a file is made.
#: `CREATE TABLE IF NOT EXISTS` is happy to open a database whose tables are
#: the wrong shape, and the first query then fails on a missing column; the
#: version is what turns that into one sentence saying which file to delete.
SCHEMA_VERSION = "1"

#: Version rank and tag folded into one sortable integer, so "the newest
#: version's first declaration" is one `min()` rather than a window function
#: (measured over the whole dictionary: ~2.2 ms against ~8 ms) and does not
#: rest on SQLite's bare-column tie-breaking. 2**32 because a FIX tag is four
#: figures and the product still has fifty bits of headroom.
_FOLD = 4294967296

#: Pairs bound into one `IN (VALUES ...)`; see `_nearest`.
_CHUNK = 400


@dataclasses.dataclass(eq=False)
class SqliteFixRegistry(FixRegistry):
    """The same dictionary as `FixRegistry`, indexed: one query per question.

    Same scrape, same fields, same answers -- a different place to keep them.
    The JSON registry answers a lookup by parsing every version it holds and
    building a `Field` for every field in them; this one asks SQLite for the
    handful of rows the question is about, and builds those.

    Over the published dump -- nine versions, 6,479 fields, measured twice by
    `benchmarks/bench_fix_registry.py` on a registry with nothing cached:

    | question | JSON | indexed |
    | --- | --- | --- |
    | `lookup("Side")`, every version | ~75 ms | ~0.45 ms |
    | `field(54, "4.4")`, one version | ~10.6 ms | ~0.31 ms |
    | `tags()`, every version | ~79 ms | ~4.4 ms |
    | `search("reject")` | ~82 ms | ~3.9 ms |
    | `fields("4.4")`, a whole version | ~10 ms | ~9.4 ms |
    | `load()`, verifying every version | ~68 ms | ~0.8 ms |

    and 6.4 MB of resident objects against 0.09 MB. The last row is the
    honest one: handing back a whole version is `Field` construction, which
    both registries pay in full, so the index buys nothing there. Everything
    above it is a question the JSON registry answers by building thousands of
    fields to look at a handful.

    `database` is the file; a `cache_dir` with no `database` indexes into
    `fix.db` inside it. A directory that already holds a JSON dump is
    imported version by version, on demand and without a network call --
    which is what makes `SqliteFixRegistry(cache_dir="data/fix")` answer
    everything, offline, from a fresh checkout:

        registry = SqliteFixRegistry(cache_dir="data/fix")
        registry.field("Side").fix["values"]   # imported once, queried after
        registry.load()                        # or import every version now

    Where no dump sits beside it, a version that is asked for is scraped and
    stored exactly as `FixRegistry` would have -- the store is what changes
    here, never the source.

    One difference is deliberate: every answer is a *fresh* `Field`. The JSON
    registry hands back objects out of an in-memory index, so two lookups of
    one field are the same mutable object and writing to it edits what the
    next lookup returns. Here they are equal and separate, which is what a
    store handing out copies can promise.
    """

    #: The database. Empty means `fix.db` inside `cache_dir`.
    database: str | os.PathLike[str] = ""

    #: Held around every write, and around opening a connection. One writer
    #: at a time inside the process, so two threads never open a transaction
    #: over each other; between processes that is WAL's job, and
    #: `busy_timeout` is what makes the loser wait rather than raise.
    _writing: threading.RLock = dataclasses.field(
        default_factory=threading.RLock, init=False, repr=False, compare=False
    )

    #: One connection per thread, and the list of every one handed out so
    #: `close()` can close them all. A connection is *not* shared: Python's
    #: `sqlite3` keeps a statement cache on it, and two threads reading
    #: through one raised `InterfaceError: bad parameter or other API misuse`
    #: -- not on the writes, which were already locked, but on the reads.
    _local: threading.local = dataclasses.field(
        default_factory=threading.local, init=False, repr=False, compare=False
    )
    _connections: list[sqlite3.Connection] = dataclasses.field(
        default_factory=list, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        """Resolve the database beside the cache directory, once."""
        super().__post_init__()
        self.database = pathlib.Path(self.database or pathlib.Path(self.cache_dir) / DATABASE_NAME)

    # -- the file --------------------------------------------------------------

    @property
    def connection(self) -> sqlite3.Connection:
        """This thread's connection to the file, opened on first use.

        Per thread rather than per registry: a registry is a read-mostly
        handle a pipeline shares, and Python's `sqlite3` connection carries a
        statement cache that two threads reading through it corrupt. Opening
        is cheap -- the file is memory-mapped and the schema is already there
        -- and every connection opened is remembered, so `close()` closes the
        ones other threads are holding too.
        """
        opened: sqlite3.Connection | None = getattr(self._local, "connection", None)
        if opened is not None:
            return opened
        with self._writing:
            path = pathlib.Path(self.database)
            path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(path, check_same_thread=False)
            connection.executescript(PRAGMAS)
            connection.executescript(SCHEMA)
            with connection:
                connection.execute(
                    "INSERT INTO meta(key, value) VALUES ('schema', ?) ON CONFLICT(key) DO NOTHING",
                    (SCHEMA_VERSION,),
                )
            (found,) = connection.execute("SELECT value FROM meta WHERE key = 'schema'").fetchone()
            if found != SCHEMA_VERSION:
                connection.close()
                raise ValueError(
                    f"{path} is a FIX index of schema {found}, and this is {SCHEMA_VERSION}: "
                    "delete it and it will be built again from the dump beside it, or from "
                    "the site"
                )
            self._connections.append(connection)
        self._local.connection = connection
        return connection

    def close(self) -> None:
        """Close every connection this registry opened. Never opens one.

        Asking again afterwards opens a new one: the file is the registry,
        and closing it releases a handle rather than ending anything.
        """
        with self._writing:
            connections, self._connections = self._connections, []
            self._local = threading.local()
        for index, connection in enumerate(connections):
            if index == 0:
                # WAL keeps committed rows in `fix.db-wal` until something
                # checkpoints, and SQLite only folds them in when the *last*
                # connection closes cleanly. Copying a directory is how this
                # dictionary travels, so the file is made whole here rather
                # than left to a clean exit that may not happen.
                with contextlib.suppress(sqlite3.Error):
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.close()

    def __enter__(self) -> SqliteFixRegistry:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        # Defensive twice over: `__del__` runs when `__init__` did not finish,
        # so there may be no lock and no list to close anything with, and it
        # runs during interpreter shutdown, where the modules `close()` needs
        # may already be gone. A handle that cannot be released quietly is
        # not worth a traceback on the way out.
        if not self.__dict__.get("_connections"):
            return
        try:
            self.close()
        except Exception:  # noqa: BLE001 - teardown, and the process is leaving
            pass

    def __repr__(self) -> str:
        """Says where it keeps things without opening anything to find out."""
        return f"{type(self).__name__}(database={str(self.database)!r})"

    def indexed(self) -> dict[str, float]:
        """`{version: when its fields were last stored here}`, unix seconds.

        What tells a cold index from a stale one, and the cheap check
        `lookup` and `search` make before they walk versions: it is one
        query over a table of nine rows, where asking `field` the same
        question is a scan.
        """
        return {
            name: stored_at
            for name, stored_at in self._rows(
                "SELECT name, stored_at FROM version WHERE stored_at IS NOT NULL"
            )
        }

    # -- the store -------------------------------------------------------------

    def _stored_versions(self) -> tuple[str, ...]:
        """The version list the database holds, newest first.

        A `versions.json` sitting beside the database -- a published dump the
        caller pointed at -- is taken as the list and stored, so the front
        page is not fetched for something already on disk.
        """
        stored = tuple(name for (name,) in self._rows("SELECT name FROM version ORDER BY rank"))
        if stored:
            return stored
        listed = super()._stored_versions()
        if listed:
            self._store_versions(listed)
        return listed

    def _store_versions(self, versions: tuple[str, ...]) -> None:
        """Keep the version list, and renumber the ranks the whole set implies."""
        with self._writing, self.connection as db:
            db.executemany(
                "INSERT INTO version(name, rank) VALUES (?, 0) ON CONFLICT(name) DO NOTHING",
                [(version,) for version in versions],
            )
            _renumber(db)

    def _stored_spellings(self) -> tuple[str, ...]:
        """Every version this registry has fields for, as it spelled them.

        The database first, then the JSON files beside it: a dump that has not
        been imported yet is still a version this registry can serve, and
        `_spelling` has to resolve `fixt1.1` to `FIXT1.1` before either of
        them is read.
        """
        stored = [name for (name,) in self._rows("SELECT DISTINCT version FROM field")]
        return tuple(sorted({*stored, *super()._stored_spellings()}))

    def _stored_fields(self, version: str) -> list[Field] | None:
        """One version's fields, from the index -- or from the dump beside it.

        None means neither has it, which is what makes the caller scrape.
        """
        rows = self._rows(f"SELECT {COLUMNS} FROM field WHERE version = ? ORDER BY tag", (version,))
        if rows:
            return [self._field_of(row) for row in rows]
        if version in self.indexed():
            # Stored, and stored empty. Rare enough to be worth one extra
            # query on the miss: without it a version that legitimately holds
            # nothing would be scraped again on every single call.
            return []
        return self._import(version)

    def _store_fields(self, version: str, fields: list[Field]) -> None:
        """Replace one whole version, in one transaction.

        Delete then insert rather than upsert: a refreshed version that lost a
        field must lose it here too, and a reader either sees the version it
        saw before or the new one -- never half of each.
        """
        rows = [_row_of(version, member) for member in fields]
        tags = {row[1] for row in rows}
        if len(tags) != len(rows):
            # The primary key would refuse this anyway, and with a message
            # about a constraint rather than about the dictionary: two
            # definitions of one tag in one version is not a version.
            repeated = sorted({row[1] for row in rows if _repeats(rows, row[1])})
            raise ValueError(f"FIX {version} declares {repeated} more than once")
        with self._writing, self.connection as db:
            db.execute(
                "INSERT INTO version(name, rank) VALUES (?, 0) ON CONFLICT(name) DO NOTHING",
                (version,),
            )
            db.execute("DELETE FROM field WHERE version = ?", (version,))
            db.executemany(f"INSERT INTO field({COLUMNS}) VALUES ({_PLACEHOLDERS})", rows)
            db.execute(
                "UPDATE version SET url = ?, stored_at = ? WHERE name = ?",
                (f"{self.base_url}/{version}/", time.time(), version),
            )
            _renumber(db)

    def _import(self, version: str) -> list[Field] | None:
        """Index the JSON dump for one version, if there is one to index.

        The published dump is the document form of exactly these rows, so a
        registry pointed at `data/fix/` indexes it rather than scraping the
        site again -- the whole dictionary in about 60 ms, which is less than
        the JSON registry pays for a single cold lookup.
        """
        dumped = super()._stored_fields(version)
        if dumped is None:
            return None
        self._store_fields(version, dumped)
        return dumped

    def load(self, *versions: str, refresh: bool = False) -> dict[str, int]:
        """Index (or verify) whole versions: `{version: fields}`.

        Counted in SQL where the index already holds the version, because
        verifying a whole dictionary is nine `count(*)`s and not 6,479 objects
        built to be thrown away. What is missing is still imported or scraped,
        and `refresh` still scrapes over what is there.
        """
        counted: dict[str, int] = {}
        indexed = self.indexed()
        for named in versions or self.versions:
            spelled = self._spelling(named)
            if refresh or spelled not in indexed:
                counted[named] = len(self.fields(named, refresh=refresh))
                continue
            counted[named] = self._rows("SELECT count(*) FROM field WHERE version = ?", (spelled,))[
                0
            ][0]
        return counted

    # -- the questions ---------------------------------------------------------

    def lookup(self, key: int | str, version: str | None = None) -> list[Field]:
        """Every version's definition of one field, newest version first.

        One indexed query, and a `Field` built only for what comes back.
        """
        walked = self._walked(version)
        if not walked:
            return []
        placeholders = ",".join("?" * len(walked))
        if _is_tag(key):
            where, wanted = "f.tag = ?", int(key)
        else:
            where, wanted = "f.name_lower = ?", str(key).strip().lower()
        rows = self._rows(
            f"SELECT {_QUALIFIED} FROM field f JOIN version v ON v.name = f.version "
            f"WHERE {where} AND f.version IN ({placeholders}) ORDER BY v.rank, f.tag",
            (wanted, *walked),
        )
        return [self._field_of(row) for row in _first_per_version(rows)]

    def tags(self, version: str | None = None) -> dict[str, int]:
        """Every field name to its tag number, lowercased, newest version winning.

        One `GROUP BY` over the by-name index rather than a `Field` per field
        of every version: 2 ms against 80 ms over the published dump, and the
        answer is the same mapping.
        """
        if version is not None:
            (candidate,) = self._versions(version)
            # A named version that is not indexed yet is loaded -- and so
            # raises when it cannot be, exactly as on the JSON registry: an
            # empty mapping would quietly un-resolve every rendered key
            # downstream. One that *is* indexed is already the answer, and
            # building a field per field of it to say so cost 10 ms against
            # the query's half a millisecond.
            if candidate not in self.indexed():
                self.fields(candidate)
            walked = (candidate,)
        else:
            walked = self._walked(None)
        if not walked:
            return {}
        placeholders = ",".join("?" * len(walked))
        return {
            name: tag
            for name, tag in self._rows(
                f"SELECT f.name_lower, min(v.rank * {_FOLD} + f.tag) % {_FOLD} "
                "FROM field f JOIN version v ON v.name = f.version "
                f"WHERE f.version IN ({placeholders}) GROUP BY f.name_lower",
                walked,
            )
        }

    def search(
        self,
        text: int | str,
        version: str | None = None,
        *,
        limit: int = 10,
        fuzzy: bool = True,
    ) -> list[Field]:
        """Fields matching `text` by tag, name or description, best first.

        The same four ranks as the JSON registry -- exact, prefix, substring,
        description -- ordered newest version first, done as one `CASE` and
        one `ORDER BY` so only `limit` rows are ever built into fields. The
        Levenshtein fallback still runs here, over names read from the index
        rather than over objects built to be thrown away.
        """
        wanted = str(text).strip().lower()
        walked = self._walked(version)
        if not wanted or not walked:
            return []
        # Named parameters throughout, the version list included: sqlite3
        # refuses a statement that mixes `?` with `:name`.
        named = {f"v{index}": name for index, name in enumerate(walked)}
        placeholders = ",".join(f":{key}" for key in named)
        rows = self._rows(
            f"SELECT {_QUALIFIED}, CASE"
            "   WHEN f.name_lower = :text OR CAST(f.tag AS TEXT) = :text THEN 0"
            "   WHEN f.name_lower LIKE :prefix ESCAPE '\\' THEN 1"
            "   WHEN f.name_lower LIKE :contains ESCAPE '\\' THEN 2"
            "   ELSE 3 END AS matched "
            "FROM field f JOIN version v ON v.name = f.version "
            f"WHERE f.version IN ({placeholders}) AND ("
            "   f.name_lower LIKE :contains ESCAPE '\\'"
            "   OR f.description LIKE :contains ESCAPE '\\'"
            "   OR CAST(f.tag AS TEXT) = :text) "
            # A negative limit is `LIMIT -1` in SQL -- which means *no* limit
            # -- and `ranked[:-1]` in Python, which means all but the last.
            # Nobody asks for one, and the two answers still have to agree.
            "ORDER BY matched, v.rank, f.tag" + (" LIMIT :limit" if limit >= 0 else ""),
            {
                "text": wanted,
                "prefix": f"{_escaped(wanted)}%",
                "contains": f"%{_escaped(wanted)}%",
                "limit": limit,
                **named,
            },
        )
        if limit < 0:
            rows = rows[:limit]
        if rows:
            return [self._field_of(row[:-1]) for row in rows]
        if not fuzzy or _is_tag(wanted):
            return []
        return self._nearest(wanted, walked, limit)

    def _nearest(self, wanted: str, walked: tuple[str, ...], limit: int) -> list[Field]:
        """The names closest to `wanted` by edit distance, best first.

        Read as three columns and measured in Python, then the winners are
        fetched whole -- the distance is computed for every name either way,
        and there is no reason to build six thousand fields to rank them.
        """
        ceiling = max(2, len(wanted) // 3)
        placeholders = ",".join("?" * len(walked))
        # One distance per distinct name, not per row: the same name is
        # declared in every version that kept it, and the edit distance to it
        # is the same every time -- 1,566 names against 6,479 rows over the
        # published dump.
        distances: dict[str, int | None] = {}
        ranked: list[tuple[int, int, int, str]] = []
        for name_lower, tag, rank, version in self._rows(
            "SELECT f.name_lower, f.tag, v.rank, f.version "
            "FROM field f JOIN version v ON v.name = f.version "
            f"WHERE f.version IN ({placeholders})",
            walked,
        ):
            if name_lower not in distances:
                distances[name_lower] = _levenshtein(wanted, name_lower, ceiling)
            distance = distances[name_lower]
            if distance is not None:
                ranked.append((100 + distance, rank, tag, version))
        ranked.sort(key=lambda entry: entry[:3])
        nearest = ranked[:limit]  # negative slices the way the JSON registry does
        if not nearest:
            return []
        # In chunks, because the list is as long as the query matched and a
        # bound-parameter ceiling is a compile-time choice of whichever
        # SQLite this runs on -- 250,000 here, 999 on an older build.
        rows: list[tuple[Any, ...]] = []
        for start in range(0, len(nearest), _CHUNK):
            chunk = nearest[start : start + _CHUNK]
            pairs = ",".join("(?,?)" for _ in chunk)
            rows.extend(
                self._rows(
                    f"SELECT {COLUMNS} FROM field WHERE (version, tag) IN (VALUES {pairs})",
                    tuple(value for _, _, tag, version in chunk for value in (version, tag)),
                )
            )
        # The query answers in storage order; the ranking is what was asked for.
        by_key = {(row[0], row[1]): row for row in rows}
        return [self._field_of(by_key[(version, tag)]) for _, _, tag, version in nearest]

    # -- rows and fields -------------------------------------------------------

    def _walked(self, version: str | None) -> tuple[str, ...]:
        """The versions a question covers, each one made sure of first.

        `lookup` and `search` on the JSON registry load every version they
        walk -- scraping the ones that are missing -- and answer from what
        they could get. So does this: a version already in the index costs one
        query on a nine-row table, and one that is not is imported from the
        dump beside it or scraped, exactly as there. Versions that cannot be
        had right now are skipped, not raised over.
        """
        indexed = self.indexed()
        walked = []
        for candidate in self._versions(version):
            if candidate not in indexed:
                try:
                    self.fields(candidate)
                except OSError:
                    continue
            walked.append(candidate)
        return tuple(walked)

    def _rows(self, query: str, parameters: Any = ()) -> list[tuple[Any, ...]]:
        """Run one query. The single place the database is read."""
        cursor = self.connection.cursor()
        try:
            return cursor.execute(query, parameters).fetchall()
        finally:
            cursor.close()

    @cached_property
    def _types(self) -> dict[str, pyarrow.DataType]:
        """One parse per distinct Arrow type spelling, not per field.

        A FIX dictionary names about forty types across thousands of fields,
        and parsing `timestamp[ns]` afresh for each of them was measured at a
        third of the cost of building a whole version.
        """
        return {}

    def _field_of(self, row: tuple[Any, ...]) -> Field:
        """One row as the `Field` the JSON registry would have built."""
        (
            _version,
            tag,
            name,
            _name_lower,
            arrow_type,
            nullable,
            description,
            fix_type,
            fix_version,
            values_json,
            used_in,
            note,
            extra,
        ) = row
        metadata: dict[str, str] = {"fix:tag": str(tag)}
        if fix_type:
            metadata["fix:type"] = fix_type
        if fix_version:
            # What the field said, not the row's key: they are the same for
            # everything the scraper writes, and a field that carried no
            # version must not come back out of the index carrying one.
            metadata["fix:version"] = fix_version
        if values_json:
            metadata["fix:values"] = values_json
        if note:
            metadata["fix:note"] = note
        if used_in:
            metadata["fix:used_in"] = used_in
        if extra:
            metadata.update(json.loads(extra))
        if description:
            metadata["description"] = description
        kind = self._types.get(arrow_type)
        if kind is None:
            kind = self._types[arrow_type] = Field.from_dict(
                {"name": name, "type": arrow_type}
            ).arrow_type
        return Field(name=name, arrow_type=kind, nullable=bool(nullable), metadata=metadata)


# -- what a row is -----------------------------------------------------------

#: The same columns as `COLUMNS`, qualified for the join the questions make.
_QUALIFIED = ", ".join(f"f.{column.strip()}" for column in COLUMNS.split(","))


def _row_of(version: str, member: Field) -> tuple[Any, ...]:
    """One `Field` as the row `COLUMNS` names, metadata and all.

    Every key the columns do not name is kept in `extra`: a dump that was
    hand-edited, or a future `fix:` key, has to survive a round trip through
    the index or the index is lossy where the JSON was not.
    """
    fix = member.fix
    extra = {key: value for key, value in member.metadata.items() if key not in KNOWN}
    return (
        version,
        int(fix["tag"]),
        member.name,
        member.name.lower(),
        member.kind(),
        int(member.nullable),
        member.description,
        fix.get("type"),
        fix.get("version"),
        fix.get("values"),
        fix.get("used_in"),
        fix.get("note"),
        json.dumps(extra, separators=(",", ":")) if extra else None,
    )


def _repeats(rows: list[tuple[Any, ...]], tag: int) -> bool:
    """Whether `tag` is declared more than once in `rows`."""
    return sum(1 for row in rows if row[1] == tag) > 1


def _first_per_version(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """One row per version, the first one -- what a by-name index answers.

    A name declared twice in one version resolves to its first declaration on
    the JSON registry, because that is the one its index kept. The rows arrive
    in `(rank, tag)` order, so the first of each version is that field.
    """
    seen: set[str] = set()
    first = []
    for row in rows:
        if row[0] not in seen:
            seen.add(row[0])
            first.append(row)
    return first


def _renumber(db: sqlite3.Connection) -> None:
    """Rank every stored version, newest first, from the whole set.

    Ranks are positions in a list, so they are recomputed whenever the list
    changes rather than assigned once: a FIX version added tomorrow moves
    every rank below it, and rows that kept a copy of yesterday's would order
    a lookup by a list that no longer exists.
    """
    names = sorted((name for (name,) in db.execute("SELECT name FROM version")), key=_version_key)
    db.executemany(
        "UPDATE version SET rank = ? WHERE name = ?",
        [(rank, name) for rank, name in enumerate(reversed(names))],
    )


def _escaped(text: str) -> str:
    """A search term as a LIKE pattern's literal part.

    `%` and `_` are wildcards there and plain characters in a search, so a
    query for `100%` must not become a query for "starts with 100".
    """
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
