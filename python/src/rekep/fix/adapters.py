"""Complete-file source adapters for offline FIX registry refreshes."""

from __future__ import annotations

import abc
import dataclasses
import io
import os
import pathlib
import re
import stat
import tempfile
import time
import types
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from collections.abc import Callable, Mapping
from urllib.parse import urlsplit

from rekep.fix.orchestra import (
    SourceConflict,
    SourceField,
    SourceProvenance,
    SourceRegistry,
    SourceValue,
    infer_arrow_type,
    parse_orchestra,
    parse_quickfix,
)

OpenSource = Callable[[str], bytes]

_IDENTIFIER = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
_MAX_SOURCE_BYTES = 256 * 1024 * 1024
_MAX_ARCHIVE_MEMBER = 512 * 1024 * 1024
_MAX_COMPRESSION_RATIO = 1_000
_MARKDOWN_CODE = re.compile(r"`([^`]+)`")
_RETRIED = frozenset({408, 425, 429, 500, 502, 503, 504})
_MAX_WAIT_SECONDS = 60.0


@dataclasses.dataclass(frozen=True)
class SourceDocument:
    """One verified complete source file and the parser payload it contains."""

    provenance: SourceProvenance
    content: bytes
    cached_path: pathlib.Path
    cached: bool


@dataclasses.dataclass(frozen=True)
class SourceAdapter(abc.ABC):
    """A pinned registry source which can be cached and replayed offline."""

    source_id: str
    namespace: str
    version: str
    url: str
    format: str
    protocol_version: str = ""
    license_url: str = ""
    archive_member: str = ""
    checksum: str = ""
    priority: int = 0
    default: bool = True
    fetch_allowed: bool = True

    def __post_init__(self) -> None:
        """Refuse identifiers and locations that cannot name a deterministic cache file."""
        for owner, value in (("source", self.source_id), ("namespace", self.namespace)):
            if not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"FIX {owner} identifier {value!r} is not portable")
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https", "file"}:
            raise ValueError(f"FIX source {self.source_id!r} has unsupported URL {self.url!r}")
        if self.checksum and not re.fullmatch(r"sha256:[0-9a-f]{64}", self.checksum.casefold()):
            raise ValueError(f"FIX source {self.source_id!r} has an invalid checksum")
        if self.archive_member:
            _safe_member_name(self.archive_member)

    @property
    def cache_name(self) -> str:
        """The stable file name which keeps the complete source artifact."""
        suffix = pathlib.PurePosixPath(urlsplit(self.url).path).suffix.casefold()
        if suffix not in {".xml", ".zip"}:
            suffix = ".source"
        version = re.sub(r"[^a-z0-9._-]+", "-", self.version.casefold()).strip("-.")
        return f"{self.source_id}-{version or 'current'}{suffix}"

    def fetch(
        self,
        cache_dir: str | os.PathLike[str],
        *,
        offline: bool = False,
        refresh: bool = False,
        opener: OpenSource | None = None,
        timeout: float = 60.0,
        retries: int = 0,
        backoff: float = 2.0,
    ) -> SourceDocument:
        """Read one complete cached artifact, downloading atomically when allowed."""
        target = pathlib.Path(cache_dir) / self.cache_name
        if target.is_file() and not refresh:
            try:
                return self._document(_read_cached_source(target), target, cached=True)
            except ValueError:
                if offline:
                    raise
        if offline:
            raise FileNotFoundError(f"offline FIX source cache is missing {target}")
        raw = (
            opener(self.url)
            if opener is not None
            else _open_source(
                self.url,
                timeout=timeout,
                retries=retries,
                backoff=backoff,
            )
        )
        if not raw:
            raise ValueError(f"FIX source {self.source_id!r} returned an empty artifact")
        if len(raw) > _MAX_SOURCE_BYTES:
            raise ValueError(f"FIX source {self.source_id!r} exceeds the complete-file limit")
        document = self._document(raw, target, cached=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = pathlib.Path(
            tempfile.NamedTemporaryFile(
                prefix=f".{target.name}-", suffix=".tmp", dir=target.parent, delete=False
            ).name
        )
        try:
            temporary.write_bytes(raw)
            os.replace(temporary, target)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return document

    def load(
        self,
        cache_dir: str | os.PathLike[str],
        *,
        offline: bool = False,
        refresh: bool = False,
        opener: OpenSource | None = None,
        timeout: float = 60.0,
        retries: int = 0,
        backoff: float = 2.0,
    ) -> SourceRegistry:
        """Parse a cached source, replacing a poisoned online cache once."""
        fetch_options = {
            "offline": offline,
            "opener": opener,
            "timeout": timeout,
            "retries": retries,
            "backoff": backoff,
        }
        document = self.fetch(cache_dir, refresh=refresh, **fetch_options)
        try:
            return self.parse(document)
        except ValueError:
            if offline or refresh or not document.cached:
                raise
        document = self.fetch(cache_dir, refresh=True, **fetch_options)
        return self.parse(document)

    @abc.abstractmethod
    def parse(self, document: SourceDocument) -> SourceRegistry:
        """Read definitions from the adapter's complete parser payload."""

    def _document(self, raw: bytes, cached_path: pathlib.Path, *, cached: bool) -> SourceDocument:
        provenance = SourceProvenance.for_bytes(
            raw,
            source_id=self.source_id,
            namespace=self.namespace,
            version=self.version,
            url=self.url,
            format=self.format,
            license_url=self.license_url,
            protocol_version=self.protocol_version,
        )
        if self.checksum and provenance.checksum != self.checksum.casefold():
            raise ValueError(
                f"FIX source {self.source_id!r} checksum is {provenance.checksum}, "
                f"expected {self.checksum.casefold()}"
            )
        content = _archive_payload(raw, self.archive_member) if self.archive_member else raw
        return SourceDocument(provenance, content, cached_path, cached)


@dataclasses.dataclass(frozen=True)
class OrchestraAdapter(SourceAdapter):
    """A complete FIX Orchestra repository source."""

    def parse(self, document: SourceDocument) -> SourceRegistry:
        """Parse this source as namespace-agnostic Orchestra XML."""
        return parse_orchestra(document.content, document.provenance)


@dataclasses.dataclass(frozen=True)
class QuickFixAdapter(SourceAdapter):
    """A complete QuickFIX dictionary source."""

    def parse(self, document: SourceDocument) -> SourceRegistry:
        """Parse this source through the shared registry model."""
        return parse_quickfix(document.content, document.provenance)


@dataclasses.dataclass(frozen=True)
class ClearStreetAdapter(SourceAdapter):
    """Clear Street's complete Markdown FIX dictionary."""

    def parse(self, document: SourceDocument) -> SourceRegistry:
        """Read the custom-field rows repeated across its message tables."""
        return _parse_clear_street(document.content, document.provenance)


@dataclasses.dataclass(frozen=True)
class SourceExclusion:
    """An official source withheld from automation by format or redistribution terms."""

    source_id: str
    namespace: str
    version: str
    url: str
    format: str
    reason: str
    license_url: str = ""
    default: bool = False
    fetch_allowed: bool = False


FIX_LATEST = OrchestraAdapter(
    source_id="fix-latest",
    namespace="standard",
    version="FIX.5.0SP2_EP309",
    protocol_version="5.0.SP2",
    url=(
        "https://raw.githubusercontent.com/FIXTradingCommunity/orchestrations/"
        "099914dd0edd49a699326f0441776d6e21cfaf93/"
        "FIX%20Standard/OrchestraFIXLatest.xml"
    ),
    format="orchestra",
    license_url=(
        "https://raw.githubusercontent.com/FIXTradingCommunity/orchestrations/master/LICENSE"
    ),
    checksum="sha256:9ea5ee01a90019eb2d307cdd91e3fbec0b4a9249bc196da62d08417c9df3da07",
    priority=-100,
)

FIXTRADING_UDF = OrchestraAdapter(
    source_id="fixtrading-udf",
    namespace="fixtrading-udf",
    version="1.0",
    protocol_version="5.0.SP2",
    url=(
        "https://orchestrahub.org/api/v3/repos/community/fix-udf/"
        "revisions/2EewWYrfFXqxPgLF/download"
    ),
    format="orchestra",
    license_url="https://orchestrahub.org/community/fix-udf",
    checksum="sha256:dbc1dfbd9deea8180303edd28145e7494ffeae273e0ccee3cc72ecdeefe9afe5",
    priority=100,
    default=False,
)

QUICKFIX = QuickFixAdapter(
    source_id="quickfix",
    namespace="standard",
    version="FIX.5.0SP2_EP280",
    protocol_version="5.0.SP2",
    url=(
        "https://raw.githubusercontent.com/quickfix/quickfix/"
        "3536699e830e65f875df4a50b647a6d3bad3b884/spec/FIX50SP2.xml"
    ),
    format="quickfix",
    license_url="https://github.com/quickfix/quickfix/blob/master/LICENSE",
    checksum="sha256:7d34e565586dd4096a08691d10e415b5a2fd531a8dadfcfc831daea419d3c3f3",
    priority=200,
)

CLEAR_STREET = ClearStreetAdapter(
    source_id="clear-street",
    namespace="clear-street",
    version="FIX.4.2",
    protocol_version="4.2",
    url=(
        "https://raw.githubusercontent.com/clear-street/FIX-docs/"
        "878fe44f35290a11539a015445cd71c80dd5a7ed/source/index.html.md"
    ),
    format="markdown",
    license_url="https://github.com/clear-street/FIX-docs/blob/main/LICENSE",
    checksum="sha256:cbcff58055c619f4f93cea9ee43124bde59b0a90b5bd4059180079f53e0846d3",
    priority=1_000,
    default=False,
)

EXCLUDED_SOURCES: tuple[SourceExclusion, ...] = (
    SourceExclusion(
        "cboe-cfe",
        "cboe-cfe",
        "current",
        (
            "https://www.cboe.com/document/tech-spec/document/technical-specifications/"
            "cboe-titanium-cboe-futures-exchange-fix-specification"
        ),
        "html",
        "Venue terms do not permit deterministic artifact redistribution.",
        "https://www.cboe.com/use-of-content",
    ),
    SourceExclusion(
        "kraken",
        "kraken",
        "current",
        "https://docs.kraken.com/exchange/api-reference/unified-fix/mdsfr",
        "html",
        "The live documentation is not a complete machine-readable dictionary.",
        "https://www.kraken.com/legal/global-terms",
    ),
    SourceExclusion(
        "miax-emerald",
        "miax-emerald",
        "1.3a",
        "https://www.miaxglobal.com/markets/us-options/emerald-options/interface-specifications",
        "pdf",
        "The venue PDF requires a reviewed local extraction before publication.",
        "https://www.miaxglobal.com/terms-use",
    ),
    SourceExclusion(
        "lime",
        "lime",
        "6.2.5",
        "https://docs.lime.co/fix/LimeFIXManual.pdf",
        "pdf",
        "The venue PDF requires a reviewed local extraction before publication.",
        "https://lime.co/terms-of-use/",
    ),
    SourceExclusion(
        "falconx",
        "falconx",
        "2.1.3",
        "https://app.falconx.io/static/pdf/FIX-Document.pdf",
        "pdf",
        "The venue PDF requires a reviewed local extraction before publication.",
        "https://www.falconx.io/terms-of-use",
    ),
    SourceExclusion(
        "eurex-t7",
        "eurex-t7",
        "14.1",
        "https://www.eurex.com/ex-en/support/initiatives/t7release14-1",
        "zip-quickfix",
        "The release bundles require a reviewed local extraction before publication.",
        "https://www.eurex.com/ex-en/legal-information/terms-of-use",
    ),
    SourceExclusion(
        "b2bits",
        "standard",
        "current",
        "https://www.b2bits.com/fixopaedia/index.html",
        "html",
        "FIXopaedia is enrichment-only and has no permissive artifact redistribution license.",
    ),
)

ADAPTERS: tuple[SourceAdapter, ...] = (FIX_LATEST, FIXTRADING_UDF, QUICKFIX, CLEAR_STREET)
ADAPTERS_BY_ID: Mapping[str, SourceAdapter] = types.MappingProxyType(
    {adapter.source_id: adapter for adapter in ADAPTERS}
)
SOURCE_CATALOG: Mapping[str, SourceAdapter | SourceExclusion] = types.MappingProxyType(
    {
        **ADAPTERS_BY_ID,
        **{source.source_id: source for source in EXCLUDED_SOURCES},
    }
)


def adapter(source_id: str) -> SourceAdapter:
    """One configured source by its CLI identifier."""
    try:
        return ADAPTERS_BY_ID[source_id]
    except KeyError as error:
        raise KeyError(f"unknown FIX registry source {source_id!r}") from error


def _read_cached_source(path: pathlib.Path) -> bytes:
    """One cached complete file, read no further than the source size limit."""
    with path.open("rb") as stream:
        content = stream.read(_MAX_SOURCE_BYTES + 1)
    if len(content) > _MAX_SOURCE_BYTES:
        raise ValueError(f"cached FIX source {path} exceeds the complete-file limit")
    return content


def _open_source(
    url: str,
    *,
    timeout: float = 60.0,
    retries: int = 0,
    backoff: float = 2.0,
) -> bytes:
    """One bounded complete-file download, retried only for transient failures."""
    request = urllib.request.Request(url, headers={"User-Agent": "rekep-fix-registry"})
    pause = backoff
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                content = bytearray()
                while chunk := response.read(1024 * 1024):
                    content.extend(chunk)
                    if len(content) > _MAX_SOURCE_BYTES:
                        raise ValueError("FIX source exceeds the complete-file limit")
                return bytes(content)
        except OSError as error:
            if attempt == retries or not _transient(error):
                raise
            headers = getattr(error, "headers", None)
            asked = headers.get("Retry-After", "") if headers is not None else ""
            wait = float(asked) if str(asked).strip().isdigit() else pause
            time.sleep(min(wait, _MAX_WAIT_SECONDS))
            pause *= 2
    raise AssertionError("the complete-file retry loop always returns or raises")


def _transient(error: OSError) -> bool:
    """Whether a complete-file failure asks the caller to try again later."""
    if isinstance(error, urllib.error.HTTPError):
        return error.code in _RETRIED
    return isinstance(error, urllib.error.URLError | TimeoutError | ConnectionError)


def _safe_member_name(member: str) -> pathlib.PurePosixPath:
    path = pathlib.PurePosixPath(member.replace("\\", "/"))
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"unsafe FIX source archive member {member!r}")
    return path


def _archive_payload(raw: bytes, member: str) -> bytes:
    """Read one regular member without extracting paths from an untrusted archive."""
    wanted = _safe_member_name(member).as_posix()
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            try:
                info = archive.getinfo(wanted)
            except KeyError as error:
                raise ValueError(f"FIX source archive has no member {wanted!r}") from error
            mode = info.external_attr >> 16
            if info.is_dir() or stat.S_ISLNK(mode) or info.flag_bits & 1:
                raise ValueError(f"unsafe FIX source archive member {wanted!r}")
            if info.file_size > _MAX_ARCHIVE_MEMBER:
                raise ValueError(f"FIX source archive member {wanted!r} is too large")
            if (
                info.file_size > 1_048_576
                and info.file_size > max(info.compress_size, 1) * _MAX_COMPRESSION_RATIO
            ):
                raise ValueError(f"FIX source archive member {wanted!r} is over-compressed")
            return archive.read(info)
    except zipfile.BadZipFile as error:
        raise ValueError("FIX source archive is not a readable ZIP") from error


def _parse_clear_street(content: bytes, provenance: SourceProvenance) -> SourceRegistry:
    """Collapse repeated custom-field table rows from the venue's complete guide."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("Clear Street source is not UTF-8 Markdown") from error
    rows: dict[int, list[tuple[str, str, str, tuple[SourceValue, ...]]]] = defaultdict(list)
    for line in text.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 4:
            continue
        name = _markdown_text(cells[0])
        tag_text = _markdown_text(cells[1])
        if not tag_text.isdigit() or not 5_000 <= int(tag_text) <= 9_999:
            continue
        datatype = _clear_street_datatype(_markdown_text(cells[3]))
        if not name or not datatype:
            continue
        description_index = 6 if len(cells) >= 7 else 4
        description = _markdown_text(cells[description_index])
        values = tuple(
            SourceValue(value=value, description=_value_meaning(value, description))
            for value in _MARKDOWN_CODE.findall(cells[2])
        )
        rows[int(tag_text)].append((name, datatype, description, values))
    if not rows:
        raise ValueError("Clear Street source declares no custom FIX fields")
    fields: list[SourceField] = []
    conflicts: list[SourceConflict] = []
    for tag, definitions in sorted(rows.items()):
        name, datatype, description, _ = definitions[0]
        readings = tuple(dict.fromkeys(definition[1] for definition in definitions))
        disputed = len({reading.casefold() for reading in readings}) > 1
        inferred = infer_arrow_type(datatype, description=description, disputed=disputed)
        if disputed:
            conflicts.append(SourceConflict(str(tag), "datatype", readings, "string"))
        values: dict[str, SourceValue] = {}
        for _, _, _, enumerated in definitions:
            for value in enumerated:
                values.setdefault(value.value, value)
        canonical = re.sub(r"[^A-Za-z0-9]+", "", name)
        aliases = tuple(
            dict.fromkeys(
                alternative
                for alternative, _, _, _ in definitions
                if re.sub(r"[^A-Za-z0-9]+", "", alternative) != canonical
            )
        )
        fields.append(
            SourceField(
                tag=tag,
                name=canonical,
                original_datatype=datatype,
                datatype=inferred.datatype,
                arrow_type=inferred.arrow,
                description=description,
                values=tuple(values.values()),
                aliases=aliases,
                scenarios=("base",),
                type_readings=readings,
                fallback=inferred.fallback,
                provenance=provenance,
            )
        )
    return SourceRegistry(
        source=provenance,
        repository_name="Clear Street FIX Trade Specification",
        repository_version=provenance.version,
        fields=tuple(fields),
        conflicts=tuple(conflicts),
    )


def _markdown_text(cell: str) -> str:
    return re.sub(r"\s+", " ", cell.replace("`", "")).strip()


def _clear_street_datatype(datatype: str) -> str:
    return {
        "decimal": "float",
        "integer": "int",
        "string": "string",
    }.get(datatype.casefold(), datatype)


def _value_meaning(value: str, description: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(value)}\s*[-=]\s*([^`]+)", description)
    return match[1].strip() if match else ""


def with_url(source: SourceAdapter, url: str) -> SourceAdapter:
    """The same adapter pointed at a mirror or an offline file URL."""
    return dataclasses.replace(source, url=url)
