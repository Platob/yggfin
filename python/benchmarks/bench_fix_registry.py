"""Benchmark the FIX registry's two stores: a directory of JSON, and a zip of it."""

from __future__ import annotations

import pathlib
import sys
import tempfile
import zipfile
from collections.abc import Callable

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser  # noqa: E402

from rekep.fix import FixRegistry  # noqa: E402

#: The published dictionary this sweeps over: the repository's own archive.
ARCHIVE = pathlib.Path(__file__).resolve().parents[2] / "data" / "fix.zip"

#: The questions, as the calls a caller makes. `fields` is what a bulk load
#: asks; the rest are what a job asks.
QUESTIONS: dict[str, Callable[[FixRegistry], object]] = {
    "field('Side')  every version": lambda registry: registry.field("Side"),
    "field(54, '4.4')  one version": lambda registry: registry.field(54, "4.4"),
    "tags()  every version": lambda registry: registry.tags(),
    "search('reject')": lambda registry: registry.search("reject"),
    "fields('4.4')  one version": lambda registry: registry.fields("4.4"),
    "load()  every version": lambda registry: registry.load(),
}


def unpacked(into: pathlib.Path) -> pathlib.Path:
    """The archive's members as a directory of files: the other store, same bytes.

    Layout and all: a shard is `fields/000000.json` in both stores, and a
    directory holding it under any other name is not the same store -- it is
    an empty one, which answers every question below with a scrape.
    """
    directory = into / "fix"
    directory.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ARCHIVE) as archive:
        for name in archive.namelist():
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))
    return directory


def check(folder: pathlib.Path) -> None:
    """The archived answer *is* the directory's answer, asserted before timing."""
    directory = FixRegistry(cache_dir=folder, retries=0)
    archived = FixRegistry(cache_dir=ARCHIVE, retries=0)
    assert archived.versions == directory.versions
    for label, question in QUESTIONS.items():
        assert question(archived) == question(directory), label
    for version in directory.versions:
        assert archived.fields(version) == directory.fields(version), version
    for text in ("reject", "side", 54, "Sied", "100%", "px"):
        assert archived.search(text) == directory.search(text), text


def sweep_questions(folder: pathlib.Path, repeat: int) -> None:
    """Cold, question by question. Cold is a registry with nothing in it yet."""
    print(f"\nquestions, best of {repeat} (ms)")
    print(f"{'':>32} {'directory':>12} {'zip':>12} {'zip costs':>12}")
    for label, question in QUESTIONS.items():
        loose = best_of(lambda q=question: q(FixRegistry(cache_dir=folder, retries=0)), repeat)
        archived = best_of(lambda q=question: q(FixRegistry(cache_dir=ARCHIVE, retries=0)), repeat)
        print(
            f"{label:>32} {loose * 1000:>12.3f} {archived * 1000:>12.3f} "
            f"{(archived / loose - 1) * 100:>11.0f}%"
        )


def sweep_size(folder: pathlib.Path, repeat: int) -> None:
    """What the archive saves, and what each deflate level is worth.

    Zipped here rather than through `into_zip`, because the level is what is
    being swept and the library does not take one: it writes at zlib's
    default, and these rows are why.
    """
    print(f"\nsize and publishing, best of {repeat}")
    # `rglob`, because the store is a tree: the shards and the components sit
    # in their own folders, and a flat glob measures the version index alone.
    loose = sum(path.stat().st_size for path in folder.rglob("*.json"))
    print(f"{'directory of JSON':>32} {loose / 1e6:>8.2f} MB")
    documents = {
        path.relative_to(folder).as_posix(): path.read_bytes()
        for path in sorted(folder.rglob("*.json"))
    }
    with tempfile.TemporaryDirectory() as scratch:
        for level in (None, 0, 1, 6, 9):
            target = pathlib.Path(scratch) / f"level{level}.zip"
            seconds = best_of(lambda t=target, level=level: _zip(documents, t, level), repeat)
            size = target.stat().st_size
            named = "zip, zlib's own level" if level is None else f"zip, deflate level {level}"
            print(
                f"{named:>32} {size / 1e6:>8.2f} MB   {seconds * 1000:>7.0f} ms to write"
                f"   {loose / size:>5.1f}x smaller"
            )


def _zip(documents: dict[str, bytes], target: pathlib.Path, level: int | None) -> None:
    """The same members at one deflate level; level 0 stores rather than deflates."""
    kind = zipfile.ZIP_STORED if level == 0 else zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(target, "w", kind, compresslevel=level) as archive:
        for name, document in documents.items():
            archive.writestr(name, document)


def main() -> None:
    arguments = parser(__doc__, repeat=7).parse_args()
    repeat = 3 if arguments.quick else arguments.repeat

    if not ARCHIVE.exists():
        raise SystemExit(f"no dictionary to measure: {ARCHIVE} is not there")
    with tempfile.TemporaryDirectory() as scratch:
        folder = unpacked(pathlib.Path(scratch))
        versions = FixRegistry(cache_dir=folder, retries=0).versions
        fields = sum(len(FixRegistry(cache_dir=folder, retries=0).fields(v)) for v in versions)
        print(f"{ARCHIVE.name}: {len(versions)} versions, {fields:,} fields")
        check(folder)
        print("every answer matches between the two stores")
        sweep_questions(folder, repeat)
        sweep_size(folder, repeat)


if __name__ == "__main__":
    main()
