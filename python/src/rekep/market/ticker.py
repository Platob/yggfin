"""Canonical FIX instrument tickers.

FIX 4.4 defines the Instrument component, `SecurityIDSource <22>` and
`SecurityID <48>` at https://www.onixs.biz/fix-dictionary/4.4/compblock_instrument.html,
https://www.onixs.biz/fix-dictionary/4.4/tagnum_22.html and
https://www.onixs.biz/fix-dictionary/4.4/tagnum_48.html.

ISO 4217's 2026-01-01 alphabetic codes are published at
https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml.
"""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Iterable, Mapping
from typing import Any, Self

import pyarrow
import pyarrow.compute as compute

from rekep.enums import MIC, AssetKind, Currency
from rekep.fields import column_name
from rekep.fix.access import FieldAccess
from rekep.fix.registry import FixRegistry

_CACHE_SIZE = 65_536
_FX_SYMBOL = re.compile(r"^(?P<base>[A-Za-z]{3})(?:[/\.]?)(?P<quote>[A-Za-z]{3})$")
_ISO_4217 = frozenset(
    """
    AED AFN ALL AMD AOA ARS AUD AWG AZN BAM BBD BDT BHD BIF BMD BND BOB BOV
    BRL BSD BTN BWP BYN BZD CAD CDF CHE CHF CHW CLF CLP CNY COP COU CRC CUP
    CVE CZK DJF DKK DOP DZD EGP ERN ETB EUR FJD FKP GBP GEL GHS GIP GMD GNF
    GTQ GYD HKD HNL HTG HUF IDR ILS INR IQD IRR ISK JMD JOD JPY KES KGS KHR
    KMF KPW KRW KWD KYD KZT LAK LBP LKR LRD LSL LYD MAD MDL MGA MKD MMK MNT
    MOP MRU MUR MVR MWK MXN MXV MYR MZN NAD NGN NIO NOK NPR NZD OMR PAB PEN
    PGK PHP PKR PLN PYG QAR RON RSD RUB RWF SAR SBD SCR SDG SEK SGD SHP SLE
    SOS SRD SSP STN SVC SYP SZL THB TJS TMT TND TOP TRY TTD TWD TZS UAH UGX
    USD USN UYI UYU UYW UZS VED VES VND VUV WST XAD XAF XAG XAU XBA XBB XBC
    XBD XCD XCG XDR XOF XPD XPF XPT XSU XTS XUA XXX YER ZAR ZMW ZWG
    """.split()
)


@dataclasses.dataclass(frozen=True, slots=True)
class SymbolTicker:
    """One canonical stored instrument spelling and its FX facts."""

    symbolticker: str = ""
    kind: AssetKind = AssetKind.UNKNOWN
    currency: Currency | None = None

    @classmethod
    def from_fixmsg(cls, message: Any) -> Self:
        """Build from one parsed FIX row through its registry-backed accessor."""
        registry = message.registry
        version = message.resolved_version(registry)
        access = FieldAccess.of(registry, version)
        entries = (*(message.entries or ()), *(message.unmap or ()))

        def read(name: str) -> Any:
            value = getattr(message, column_name(name), None)
            if value not in (None, ""):
                return value
            reading = access.reading(entries, name)
            return reading.raw if reading else None

        found = cls._from_parts(
            securityid=read("SecurityID"),
            securityidsource=read("SecurityIDSource"),
            symbol=read("Symbol"),
            securityexchange=read("SecurityExchange"),
            registry=registry,
            version=version,
        )
        return found if found.symbolticker else cls.from_str(message.symbolticker)

    @classmethod
    def from_entries(
        cls,
        entries: Iterable[Any],
        registry: FixRegistry | None = None,
        version: str | None = None,
    ) -> Self:
        """Build from raw entries resolved against one FIX registry."""
        selected = registry if registry is not None else FixRegistry.from_builtin()
        access = FieldAccess.of(selected, version)
        fields = tuple(entries) if entries is not None else ()

        def read(*names: str) -> Any:
            for name in names:
                reading = access.reading(fields, name)
                if reading and reading.raw not in (None, ""):
                    return reading.raw
            return None

        found = cls._from_parts(
            securityid=read("SecurityID", "LegSecurityID"),
            securityidsource=read("SecurityIDSource", "LegSecurityIDSource"),
            symbol=read("Symbol", "LegSymbol"),
            securityexchange=read("SecurityExchange", "LegSecurityExchange"),
            registry=selected,
            version=version,
        )
        return found if found.symbolticker else cls.from_str(read("SymbolTicker") or "")

    @classmethod
    def into_arrow_array(
        cls,
        columns: Mapping[str, Any],
        rows: int,
        registry: FixRegistry | None = None,
    ) -> pyarrow.Array:
        """Build canonical tickers from promoted FIX instrument columns."""
        selected = registry if registry is not None else FixRegistry.from_builtin()
        securityid = _text_array(columns.get("securityid"), rows)
        source = _text_array(columns.get("securityidsource"), rows)
        symbol = _text_array(columns.get("symbol"), rows)
        exchange = _text_array(columns.get("securityexchange"), rows)

        scheme = _scheme_arrow(source, selected)
        venue = _mic_arrow(exchange)
        identifier = compute.and_(_present(securityid), _present(scheme))
        qualified = compute.binary_join_element_wise(scheme, securityid, ":")
        qualified = compute.if_else(
            _present(venue),
            compute.binary_join_element_wise(venue, qualified, ":"),
            qualified,
        )

        readable = compute.if_else(
            compute.equal(compute.utf8_lower(symbol), "[n/a]"),
            pyarrow.scalar(""),
            symbol,
        )
        readable = _forex_symbol_arrow(readable)
        readable = compute.if_else(
            compute.and_(_present(venue), _present(readable)),
            compute.binary_join_element_wise(venue, readable, ":"),
            readable,
        )
        found = compute.if_else(identifier, qualified, readable)
        stored = _canonical_arrow(_text_array(columns.get("symbolticker"), rows))
        return compute.if_else(_present(found), found, stored)

    @classmethod
    def currency_arrow(cls, values: Any) -> pyarrow.Array:
        """FX quote currencies packed as int32, null for every other ticker."""
        tickers = _text_array(values, len(values))
        unique = compute.unique(tickers)
        currencies = [
            int(parsed.currency) if (parsed := cls.from_str(value)).currency is not None else None
            for value in unique.to_pylist()
        ]
        return compute.take(
            pyarrow.array(currencies, pyarrow.int32()),
            compute.index_in(tickers, value_set=unique),
        )

    @classmethod
    @functools.lru_cache(maxsize=_CACHE_SIZE)
    def from_str(cls, value: str) -> Self:
        """Parse one stored spelling and settle it to its canonical form."""
        text = str(value or "").strip()
        if not text:
            return cls()
        parts = text.split(":", 2)
        registry = FixRegistry.from_builtin()
        if len(parts) == 3:
            mic, source, securityid = parts
            scheme = _scheme_name(registry, None, source) or source.strip()
            return cls(symbolticker=_format_symbolticker(mic, scheme, securityid, ""))
        if len(parts) == 2:
            lead, tail = parts
            scheme = _scheme_name(registry, None, lead)
            if scheme:
                return cls(symbolticker=_format_symbolticker("", scheme, tail, ""))
            if _mic_name(lead):
                value = _format_symbolticker(lead, "", "", tail)
                return cls._from_formatted(value, value.rsplit(":", 1)[-1])
            return cls(symbolticker=_format_symbolticker("", lead.strip(), tail, ""))
        return cls._from_formatted(_format_symbolticker("", "", "", text))

    @classmethod
    def _from_parts(
        cls,
        *,
        securityid: Any,
        securityidsource: Any,
        symbol: Any,
        securityexchange: Any,
        registry: FixRegistry,
        version: str | None,
    ) -> Self:
        source = str(securityidsource or "").strip()
        scheme = _scheme_name(registry, version, source) or source
        value = _format_symbolticker(
            str(securityexchange or ""),
            scheme,
            str(securityid or ""),
            str(symbol or ""),
        )
        return cls.from_str(value)

    @classmethod
    def _from_formatted(cls, value: str, symbol: str | None = None) -> Self:
        pair = _forex_pair(symbol if symbol is not None else value)
        if pair is None:
            return cls(symbolticker=value)
        _, quote = pair
        return cls(
            symbolticker=value,
            kind=AssetKind.CURRENCY,
            currency=quote,
        )

    def into_str(self) -> str:
        """Return the stored spelling."""
        return self.symbolticker

    def __str__(self) -> str:
        return self.into_str()


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _format_symbolticker(mic: str, scheme: str, securityid: str, symbol: str) -> str:
    """Format one normalized FIX identity tuple."""
    venue = _mic_name(mic)
    identifier = securityid.strip()
    if identifier and scheme:
        return ":".join(part for part in (venue, scheme, identifier) if part)
    readable = symbol.strip()
    if readable.casefold() == "[n/a]":
        readable = ""
    if not readable:
        return ""
    pair = _forex_pair(readable)
    if pair is not None:
        readable = f"{pair[0].code}/{pair[1].code}"
    return ":".join(part for part in (venue, readable) if part)


def _mic_name(value: Any) -> str:
    """Canonical MIC, empty where FIX says no venue."""
    mic = MIC.from_str(value)
    return "" if mic in (MIC.UNKNOWN, MIC.XXXX) else mic.code


def _text_array(value: Any, rows: int) -> pyarrow.Array:
    """One trimmed, non-null string array of the requested length."""
    if value is None:
        return pyarrow.repeat(pyarrow.scalar(""), rows)
    if isinstance(value, pyarrow.ChunkedArray):
        value = value.combine_chunks()
    return compute.fill_null(
        compute.utf8_trim_whitespace(value.cast(pyarrow.string(), safe=False)), ""
    )


def _present(value: pyarrow.Array) -> pyarrow.Array:
    """Whether each normalized string carries content."""
    return compute.not_equal(value, "")


def _scheme_arrow(values: pyarrow.Array, registry: FixRegistry) -> pyarrow.Array:
    """Identifier source spellings rendered as registry scheme names."""
    unique = compute.unique(values)
    names = [
        (_scheme_name(registry, None, value) or value) if value else ""
        for value in unique.to_pylist()
    ]
    return compute.take(
        pyarrow.array(names, pyarrow.string()),
        compute.index_in(values, value_set=unique),
    )


def _canonical_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Stored ticker spellings settled through the cached scalar parser."""
    unique = compute.unique(values)
    canonical = [SymbolTicker.from_str(value).symbolticker for value in unique.to_pylist()]
    return compute.take(
        pyarrow.array(canonical, pyarrow.string()),
        compute.index_in(values, value_set=unique),
    )


def _mic_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """FIX venue spellings rendered as canonical MICs or empty strings."""
    rendered = compute.utf8_upper(values)
    valid = compute.and_(
        compute.match_substring_regex(rendered, r"^[A-Z0-9]{4}$"),
        compute.not_equal(rendered, MIC.XXXX.code),
    )
    return compute.if_else(valid, rendered, pyarrow.scalar(""))


def _forex_symbol_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Common FX symbol spellings settled to `BASE/QUOTE` with kernels."""
    upper = compute.utf8_upper(values)
    parts = compute.extract_regex(upper, _FX_SYMBOL.pattern)
    base = compute.struct_field(parts, "base")
    quote = compute.struct_field(parts, "quote")
    currencies = pyarrow.array(sorted(_ISO_4217), pyarrow.string())
    valid = compute.fill_null(
        compute.and_(
            compute.is_in(base, value_set=currencies),
            compute.is_in(quote, value_set=currencies),
        ),
        False,
    )
    canonical = compute.binary_join_element_wise(base, quote, "/")
    return compute.if_else(valid, canonical, values)


def _scheme_name(registry: FixRegistry, version: str | None, value: Any) -> str:
    """Registry name for one `SecurityIDSource` spelling."""
    source = str(value or "").strip()
    if not source:
        return ""
    try:
        field = registry.field("SecurityIDSource", version)
    except (KeyError, OSError, ValueError):
        return ""
    if field is None:
        return ""
    encoded = field.fix.encode(source)
    return field.fix.symbols.get(encoded, "")


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _forex_pair(value: str) -> tuple[Currency, Currency] | None:
    """Two ISO currencies named by one common FX symbol spelling."""
    matched = _FX_SYMBOL.fullmatch(value.strip())
    if matched is None:
        return None
    base_code = matched.group("base").upper()
    quote_code = matched.group("quote").upper()
    if base_code not in _ISO_4217 or quote_code not in _ISO_4217:
        return None
    return Currency.from_str(base_code), Currency.from_str(quote_code)
