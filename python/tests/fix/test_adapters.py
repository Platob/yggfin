"""Complete source artifacts are cached once and remain usable offline."""

from __future__ import annotations

import io
import stat
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pyarrow
import pytest

import rekep.fix
from rekep.fix import adapters
from rekep.fix.adapters import (
    ADAPTERS_BY_ID,
    CLEAR_STREET,
    EXCLUDED_SOURCES,
    FIX_LATEST,
    FIXTRADING_UDF,
    QUICKFIX,
    SOURCE_CATALOG,
    ClearStreetAdapter,
    OrchestraAdapter,
    _open_source,
)
from rekep.fix.orchestra import SourceProvenance

FIXTURES = Path(__file__).parent / "fixtures"
ORCHESTRA = (FIXTURES / "orchestra.xml").read_bytes()
CLEAR_STREET_MARKDOWN = (FIXTURES / "clear_street.md").read_bytes()


def fixture_adapter(**changes: object) -> OrchestraAdapter:
    """A complete source pointed at an injected offline fixture URL."""
    values = {
        "source_id": "fixture",
        "namespace": "vendor",
        "version": "7",
        "url": "https://example.test/repository.xml",
        "format": "orchestra",
        "license_url": "https://example.test/terms",
    }
    values.update(changes)
    return OrchestraAdapter(**values)  # type: ignore[arg-type]


def test_a_complete_source_is_downloaded_once_then_replayed_offline(tmp_path: Path) -> None:
    opened: list[str] = []

    def opener(url: str) -> bytes:
        opened.append(url)
        return ORCHESTRA

    source = fixture_adapter()
    first = source.load(tmp_path, opener=opener)
    second = source.load(tmp_path, offline=True, opener=lambda _: b"should not open")

    assert first == second
    assert opened == [source.url]
    assert (tmp_path / source.cache_name).read_bytes() == ORCHESTRA
    assert first.source.checksum.startswith("sha256:")


def test_a_complete_source_retries_only_transient_download_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        def __init__(self) -> None:
            self.chunks = iter((b"complete", b""))

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self, _: int) -> bytes:
            return next(self.chunks)

    failure = urllib.error.HTTPError(
        "https://example.test/source.xml",
        503,
        "later",
        {"Retry-After": "0"},
        None,
    )
    answers: list[OSError | Response] = [failure, Response()]
    waits: list[float] = []

    def open_once(*_: object, **__: object) -> Response:
        answer = answers.pop(0)
        if isinstance(answer, OSError):
            raise answer
        return answer

    monkeypatch.setattr(urllib.request, "urlopen", open_once)
    monkeypatch.setattr("rekep.fix.adapters.time.sleep", waits.append)

    assert (
        _open_source("https://example.test/source.xml", timeout=3.0, retries=1, backoff=0.25)
        == b"complete"
    )
    assert waits == [0.0]


def test_a_poisoned_online_cache_is_replaced_once(tmp_path: Path) -> None:
    source = fixture_adapter()
    cached = tmp_path / source.cache_name
    cached.parent.mkdir(exist_ok=True)
    cached.write_bytes(b"<broken")
    opened = 0

    def opener(_: str) -> bytes:
        nonlocal opened
        opened += 1
        return ORCHESTRA

    parsed = source.load(tmp_path, opener=opener)

    assert parsed.repository_name == "FIX.Test"
    assert opened == 1 and cached.read_bytes() == ORCHESTRA


def test_offline_never_turns_a_missing_or_malformed_cache_into_network_io(
    tmp_path: Path,
) -> None:
    source = fixture_adapter()
    with pytest.raises(FileNotFoundError, match="offline FIX source cache"):
        source.load(tmp_path, offline=True, opener=lambda _: ORCHESTRA)
    (tmp_path / source.cache_name).write_bytes(b"<broken")
    with pytest.raises(ValueError, match="malformed Orchestra XML"):
        source.load(tmp_path, offline=True, opener=lambda _: ORCHESTRA)


def test_an_oversized_cached_source_is_refused_before_it_is_materialized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = fixture_adapter()
    cached = tmp_path / source.cache_name
    cached.write_bytes(ORCHESTRA)
    monkeypatch.setattr(adapters, "_MAX_SOURCE_BYTES", len(ORCHESTRA) - 1)

    with pytest.raises(ValueError, match="cached FIX source .* complete-file limit"):
        source.load(tmp_path, offline=True)


def test_source_manifest_has_every_required_key_in_fixed_order(tmp_path: Path) -> None:
    manifest = fixture_adapter().load(tmp_path, opener=lambda _: ORCHESTRA).source.into_dict()

    assert list(manifest) == [
        "source_id",
        "namespace",
        "version",
        "url",
        "format",
        "checksum",
        "license_url",
    ]
    assert manifest["license_url"] == "https://example.test/terms"


def test_a_pinned_checksum_is_verified_before_the_cache_is_written(tmp_path: Path) -> None:
    source = fixture_adapter(checksum="sha256:" + "0" * 64)

    with pytest.raises(ValueError, match="checksum is .* expected"):
        source.load(tmp_path, opener=lambda _: ORCHESTRA)
    assert not (tmp_path / source.cache_name).exists()

    expected = SourceProvenance.for_bytes(ORCHESTRA).checksum
    pinned = fixture_adapter(checksum=expected)
    parsed = pinned.load(tmp_path, opener=lambda _: ORCHESTRA)
    assert parsed.source.checksum == expected
    assert (tmp_path / pinned.cache_name).read_bytes() == ORCHESTRA


def test_an_archive_member_is_read_without_extracting_paths(tmp_path: Path) -> None:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("spec/repository.xml", ORCHESTRA)
    source = fixture_adapter(
        url="https://example.test/repository.zip", archive_member="spec/repository.xml"
    )

    parsed = source.load(tmp_path, opener=lambda _: payload.getvalue())

    assert parsed.repository_version == "FIX.Test_EP7"
    assert (tmp_path / source.cache_name).read_bytes() == payload.getvalue()


def test_unsafe_archive_paths_and_links_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsafe FIX source archive member"):
        fixture_adapter(url="https://example.test/repository.zip", archive_member="../bad.xml")
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        member = zipfile.ZipInfo("repository.xml")
        member.create_system = 3
        member.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(member, "elsewhere.xml")
    source = fixture_adapter(
        url="https://example.test/repository.zip", archive_member="repository.xml"
    )

    with pytest.raises(ValueError, match="unsafe FIX source archive member"):
        source.load(tmp_path, opener=lambda _: payload.getvalue())


def test_clear_street_is_a_namespaced_alternative_for_udf_tags(tmp_path: Path) -> None:
    source = ClearStreetAdapter(
        source_id="clear-street-test",
        namespace="clear-street",
        version="FIX.4.2",
        url="https://example.test/clear-street.md",
        format="markdown",
    )
    parsed = source.load(tmp_path, opener=lambda _: CLEAR_STREET_MARKDOWN)

    assert len(parsed.fields) == 11
    assert [field.tag for field in parsed.fields] == list(range(9001, 9012))
    assert parsed.field(9001).name == "TradeType"  # type: ignore[union-attr]
    assert [value.value for value in parsed.field(9001).values] == [  # type: ignore[union-attr]
        "A",
        "B",
        "E",
        "T",
        "W",
    ]
    qualifier = parsed.field(9004)
    assert qualifier is not None and qualifier.arrow_type == pyarrow.int32()
    assert parsed.field(9003).name == "BranchOffice"  # type: ignore[union-attr]


def test_adapter_catalog_separates_defaults_vendors_and_restricted_sources() -> None:
    assert tuple(ADAPTERS_BY_ID) == (
        "fix-latest",
        "fixtrading-udf",
        "quickfix",
        "clear-street",
    )
    assert FIX_LATEST.default and QUICKFIX.default
    assert not FIXTRADING_UDF.default and FIXTRADING_UDF.fetch_allowed
    assert not CLEAR_STREET.default and CLEAR_STREET.fetch_allowed
    assert {source.source_id for source in EXCLUDED_SOURCES} == {
        "cboe-cfe",
        "kraken",
        "miax-emerald",
        "lime",
        "falconx",
        "eurex-t7",
        "b2bits",
    }
    assert all(not source.default and not source.fetch_allowed for source in EXCLUDED_SOURCES)
    assert set(SOURCE_CATALOG) == {
        *ADAPTERS_BY_ID,
        *(source.source_id for source in EXCLUDED_SOURCES),
    }


def test_source_revision_and_protocol_version_are_distinct_catalog_facts() -> None:
    assert (FIX_LATEST.version, FIX_LATEST.protocol_version) == (
        "FIX.5.0SP2_EP309",
        "5.0.SP2",
    )
    assert (FIXTRADING_UDF.version, FIXTRADING_UDF.protocol_version) == (
        "1.0",
        "5.0.SP2",
    )
    assert (QUICKFIX.version, QUICKFIX.protocol_version) == (
        "FIX.5.0SP2_EP280",
        "5.0.SP2",
    )
    assert (CLEAR_STREET.version, CLEAR_STREET.protocol_version) == ("FIX.4.2", "4.2")


def test_fix_package_exports_the_reusable_adapter_api() -> None:
    exported = {
        "ADAPTERS",
        "ADAPTERS_BY_ID",
        "SOURCE_CATALOG",
        "SourceAdapter",
        "SourceRegistry",
        "parse_orchestra",
        "parse_quickfix",
    }

    assert exported <= set(rekep.fix.__all__)
    assert rekep.fix.ADAPTERS_BY_ID is ADAPTERS_BY_ID


def test_invalid_adapter_identifiers_and_schemes_are_refused() -> None:
    with pytest.raises(ValueError, match="not portable"):
        fixture_adapter(source_id="../escape")
    with pytest.raises(ValueError, match="unsupported URL"):
        fixture_adapter(url="ftp://example.test/repository.xml")
    with pytest.raises(ValueError, match="invalid checksum"):
        fixture_adapter(checksum="sha256:short")
