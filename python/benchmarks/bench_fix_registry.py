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
from rekep.fix.adapters import ADAPTERS, SourceAdapter  # noqa: E402
from rekep.fix.publish import publish_builtin, publish_full  # noqa: E402

#: The published dictionary this sweeps over: the repository's own archive.
ARCHIVE = pathlib.Path(__file__).resolve().parents[2] / "data" / "fix.zip"

#: Complete upstream artifacts kept by `registry scrape`, beside the reviewable store.
SOURCE_CACHE = ARCHIVE.parent / ".fix-sources"

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


def check(folder: pathlib.Path, *, exhaustive: bool = True) -> None:
    """The archived answer *is* the directory's answer, asserted before timing."""
    directory = FixRegistry(cache_dir=folder, retries=0)
    archived = FixRegistry(cache_dir=ARCHIVE, retries=0)
    assert archived.versions == directory.versions
    assert archived.field(54, "4.4") == directory.field(54, "4.4")
    if not exhaustive:
        return
    assert archived.definitions(9001) == directory.definitions(9001)
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


def sweep_construction(folder: pathlib.Path, repeat: int) -> None:
    """Price the lazy handle separately from any JSON materialization."""
    loose = best_of(lambda: FixRegistry(cache_dir=folder, retries=0), repeat)
    archived = best_of(lambda: FixRegistry(cache_dir=ARCHIVE, retries=0), repeat)
    print(f"\nconstruction, best of {repeat} (ms)")
    print(f"{'directory':>32} {loose * 1000:>12.3f}")
    print(f"{'zip':>32} {archived * 1000:>12.3f}")


def sweep_size(folder: pathlib.Path, repeat: int, levels: tuple[int | None, ...]) -> None:
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
        for level in levels:
            target = pathlib.Path(scratch) / f"level{level}.zip"
            seconds = best_of(lambda t=target, level=level: _zip(documents, t, level), repeat)
            size = target.stat().st_size
            named = "zip, zlib's own level" if level is None else f"zip, deflate level {level}"
            print(
                f"{named:>32} {size / 1e6:>8.2f} MB   {seconds * 1000:>7.0f} ms to write"
                f"   {loose / size:>5.1f}x smaller"
            )


def sweep_sources(adapters: tuple[SourceAdapter, ...], repeat: int) -> None:
    """Parse, project, and construct every available complete source artifact."""
    print(f"\ncomplete sources, best of {repeat} (ms)")
    print(f"{'':>32} {'fields':>10} {'parse':>12} {'project':>12} {'structure':>12}")
    for adapter in adapters:
        try:
            document = adapter.fetch(SOURCE_CACHE, offline=True)
        except FileNotFoundError:
            continue
        parsed = adapter.parse(document)
        parse = best_of(lambda source=adapter, item=document: source.parse(item), repeat)
        project = best_of(
            lambda registry=parsed: tuple(field.into_field() for field in registry.fields),
            repeat,
        )
        structure = (
            best_of(lambda registry=parsed: registry.declarations(), repeat)
            if parsed.messages or parsed.components
            else 0
        )
        print(
            f"{adapter.source_id:>32} {len(parsed.fields):>10,} {parse * 1000:>12.1f} "
            f"{project * 1000:>12.1f} {structure * 1000:>12.1f}"
        )


def sweep_publication(folder: pathlib.Path, repeat: int, *, warm: bool = True) -> None:
    """Build the two artifacts callers actually publish."""
    with tempfile.TemporaryDirectory() as scratch:
        root = pathlib.Path(scratch)
        full = root / "fix.zip"
        builtin = root / "registry.zip"
        full_seconds = best_of(lambda: publish_full(folder, full), repeat, warm=warm)
        builtin_seconds = best_of(lambda: publish_builtin(full, builtin), repeat, warm=warm)
        print(f"\npublication, best of {repeat} (ms)")
        print(f"{'full registry':>32} {full_seconds * 1000:>12.1f} {full.stat().st_size:>12,} B")
        print(
            f"{'wheel registry':>32} {builtin_seconds * 1000:>12.1f} "
            f"{builtin.stat().st_size:>12,} B"
        )


def sweep_quick(folder: pathlib.Path) -> None:
    """One complete archive load, warm lookup, and deterministic publication."""
    import time

    registry = FixRegistry(cache_dir=ARCHIVE, retries=0)
    started = time.perf_counter()
    loaded = registry.load()
    load_seconds = time.perf_counter() - started
    started = time.perf_counter()
    for _ in range(1_000):
        assert registry.field(54, "4.4") is not None
    lookup_seconds = time.perf_counter() - started
    print("\nquick registry smoke")
    print(f"{'archive load':>32} {load_seconds * 1000:>12.1f} {len(loaded):>12,} fields")
    print(f"{'warm field lookup':>32} {lookup_seconds * 1_000:>12.1f} ms / 1,000")
    sweep_publication(folder, 1, warm=False)


def _zip(documents: dict[str, bytes], target: pathlib.Path, level: int | None) -> None:
    """The same members at one deflate level; level 0 stores rather than deflates."""
    kind = zipfile.ZIP_STORED if level == 0 else zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(target, "w", kind, compresslevel=level) as archive:
        for name, document in documents.items():
            archive.writestr(name, document)


def main() -> None:
    arguments = parser(__doc__, repeat=7).parse_args()
    repeat = arguments.repeat

    if not ARCHIVE.exists():
        raise SystemExit(f"no dictionary to measure: {ARCHIVE} is not there")
    with tempfile.TemporaryDirectory() as scratch:
        folder = unpacked(pathlib.Path(scratch))
        versions = FixRegistry(cache_dir=folder, retries=0).versions
        fields = sum(len(FixRegistry(cache_dir=folder, retries=0).fields(v)) for v in versions)
        print(f"{ARCHIVE.name}: {len(versions)} versions, {fields:,} fields")
        check(folder, exhaustive=not arguments.quick)
        print("every answer matches between the two stores")
        if arguments.quick:
            sweep_construction(folder, 1)
            sweep_quick(folder)
            return
        sweep_construction(folder, repeat)
        sweep_questions(folder, repeat)
        sweep_sources(ADAPTERS, repeat)
        sweep_publication(folder, repeat)
        # Quick prices the shipped level against no compression at all, which
        # is the comparison that settles whether to publish an archive; the
        # level sweep beside it rewrites the whole store once per level.
        sweep_size(folder, repeat, (None, 0) if arguments.quick else (None, 0, 1, 6, 9))


if __name__ == "__main__":
    main()
