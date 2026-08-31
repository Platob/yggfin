"""Ordered key/value arguments parsed from otherwise opaque text."""

from __future__ import annotations

import re
from typing import Any
from xml.etree import ElementTree

import pyarrow
import pyarrow.compute

from rekep.entries import ENTRIES, TAG, Entry
from rekep.fields import column_name, column_names
from rekep.fields.arrays import (
    build_list,
    dense_counts,
    groups_of,
    null_mask,
    scattered,
    sequence,
)
from rekep.fix.message import (
    BEGIN_STRING,
    BEGIN_VECTOR,
    FIX_MSG_TYPE_PATTERN,
    MARKED_VECTOR,
    NAMED_MSG_TYPE_PATTERN,
    split_payload_arrow,
)
from rekep.fix.rules import joined_pattern

# A generic argument name. Capture its marker so `Entry` can remove it while
# preserving that normalization for protocol-specific conversion.
_NAME = r"[A-Za-z_][A-Za-z0-9_.\-]*"
_BARE_KEY = rf"(?:[0-9]+|{_NAME})(?:\[[^\]\r\n]+\])?(?:\.[A-Za-z0-9_.\-]+)?"
_KEY = rf"#?{_BARE_KEY}"
_WS = r"[ \t\r\n\f\x0b]"
_ASSIGNMENT = rf"{_KEY}{_WS}*="

# Prefer a separator between two marked arguments. An indexed value may itself
# contain `A=1^AB=2`; requiring the next marker keeps that inner `^A` from
# becoming the row's outer separator.
# EOT/ETX leads, because a multi-character candidate has to be tried before
# anything it contains -- the same order `fix.message.SEPARATORS` is written in.
_PUNCTUATION = r"\x04\x03|\^A|[\x01\x21-\x2c\x2f\x3a-\x3c\x3e-\x40\x5c\x5e\x60\x7b-\x7e]"
_MARKED_SEPARATOR = (
    rf"(?s)#{_BARE_KEY}{_WS}*=.*?"
    rf"(?:(?P<sep>{_PUNCTUATION}){_WS}*#|(?P<marker>#)){_BARE_KEY}{_WS}*="
)
_PAIR_PUNCTUATION_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>{_PUNCTUATION}){_WS}*{_ASSIGNMENT}"
_PAIR_WHITESPACE_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>[ \t]+){_ASSIGNMENT}"
_SEPARATORS = r"\x04\x03|\^A|[\x01|^;#]"
_TRAILING_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>{_SEPARATORS}){_WS}*$"
_STRUCTURED_PAIRS = rf"(?s){_ASSIGNMENT}.*?(?:{_SEPARATORS}){_WS}*{_ASSIGNMENT}"
_SEPARATOR_BEFORE_ASSIGNMENT = rf"(?:{_PUNCTUATION}){_WS}*{_ASSIGNMENT}"
_MARKED_BODY = rf"(?s)(?P<body>#{_BARE_KEY}{_WS}*=.*)$"
_UNMARKED_BODY = rf"(?s)(?:^|[^A-Za-z0-9_.\-\]#])(?P<body>{_BARE_KEY}{_WS}*=.*)$"
_TOKEN = rf"(?s)^{_WS}*(?P<key>{_KEY}){_WS}*=(?P<value>.*?){_WS}*$"
_DEFAULT_SEPARATOR = "\x01"
_XML_START = re.compile(rb"<(?:\?xml\b|[A-Za-z_:])", re.IGNORECASE)
_EVENT_XML_START = re.compile(rb"<event(?:\s|>)", re.IGNORECASE)
_XML_ERROR_LENGTH = 2_048
_REFERENTIAL_START = re.compile(r"(?:^|[^A-Za-z0-9_])Referential[ \t]*\(", re.IGNORECASE)
_REFERENTIAL_ERROR_LENGTH = 2_048
_NUMBER = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")
_TICK_SIZE_SCALE_ID = column_name("tick-size-scale-id")
_QUANTITY_TYPE = column_name("quantity-type")
_REFERENTIAL_COMP = "Referential"


#: What makes a row worth tokenising: two delimiter-separated assignments, or
#: an envelope or a discriminator that says a frame opens here even where it
#: carries one field. Prose and stack traces have neither, and keeping them out
#: of the splitter is what makes a capture full of them cheap -- finding one
#: token is deliberately cheaper than splitting a whole payload.
_STRUCTURED = joined_pattern(
    _STRUCTURED_PAIRS, BEGIN_STRING, FIX_MSG_TYPE_PATTERN, NAMED_MSG_TYPE_PATTERN
)


def looks_structured_arrow(messages):
    """Which rows carry a payload at all."""
    if isinstance(messages, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [looks_structured_arrow(chunk) for chunk in messages.chunks],
            pyarrow.bool_(),
        )
    text = pyarrow.compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
    return pyarrow.compute.fill_null(
        pyarrow.compute.match_substring_regex(text, _STRUCTURED), False
    )


def payload_arrow(messages):
    """`parse_arrow` over the rows that carry a payload; the rest come back empty.

    The gate is not only an optimisation. A capture is mostly prose, splitting a
    megabyte of it is the expensive half of a parse, and one `seq=1092` inside a
    log line is a sentence rather than a message -- so a row with no payload
    carries no arguments and, having no keys, no protocol either.
    """
    compute = pyarrow.compute
    if isinstance(messages, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [payload_arrow(chunk) for chunk in messages.chunks], type=ENTRIES
        )
    rows = len(messages)
    if not rows:
        return pyarrow.array([], type=ENTRIES)
    carried = looks_structured_arrow(messages)
    if compute.all(carried, min_count=0).as_py():
        return parse_arrow(messages)
    empty = pyarrow.scalar([], ENTRIES)
    if not compute.any(carried, min_count=0).as_py():
        return pyarrow.repeat(empty, rows)
    positions = sequence(rows)
    carried_at = compute.filter(positions, carried)
    return scattered(
        [
            parse_arrow(compute.take(messages, carried_at)),
            pyarrow.repeat(empty, rows - len(carried_at)),
        ],
        [carried_at, compute.filter(positions, compute.invert(carried))],
    )


def normalized_arrow(
    stored: Any,
    plugins: Any = None,
    plugin_keys: Any = None,
    null_values: Any = (),
) -> Any:
    """Normalize plugin-owned keys and absent values without leaving Arrow."""
    if isinstance(stored, pyarrow.ChunkedArray):
        offsets = 0
        chunks = []
        for chunk in stored.chunks:
            plugin_chunk = None if plugins is None else plugins.slice(offsets, len(chunk))
            chunks.append(normalized_arrow(chunk, plugin_chunk, plugin_keys, null_values))
            offsets += len(chunk)
        return pyarrow.chunked_array(chunks, type=ENTRIES)

    if plugins is not None and len(plugins) != len(stored):
        raise ValueError("plugins and entries must have the same number of rows")

    replacements = _plugin_keys(plugin_keys)
    if replacements and plugins is not None and len(stored):
        compute = pyarrow.compute
        plugin_codes = compute.utf8_lower(
            compute.utf8_trim_whitespace(plugins.cast(pyarrow.string(), safe=False))
        )
        positions = sequence(len(stored))
        for plugin, mapping in replacements.items():
            selected = compute.fill_null(compute.equal(plugin_codes, plugin), False)
            if not compute.any(selected, min_count=0).as_py():
                continue
            selected_at = compute.filter(positions, selected)
            other_at = compute.filter(positions, compute.invert(selected))
            stored = scattered(
                [
                    _renamed_keys(compute.take(stored, selected_at), mapping),
                    compute.take(stored, other_at),
                ],
                [selected_at, other_at],
            )

    if isinstance(null_values, str):
        raise TypeError("null_values must be a sequence of strings, not one string")
    absent = frozenset(str(value).strip().lower() for value in null_values or ())
    if not absent or not len(stored):
        return stored
    compute = pyarrow.compute
    entries = compute.list_flatten(stored)
    if not len(entries):
        return stored
    parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    values = compute.struct_field(entries, "value")
    missing = compute.is_in(
        compute.utf8_lower(compute.utf8_trim_whitespace(values)),
        value_set=pyarrow.array(sorted(absent), pyarrow.string()),
    )
    keep = compute.and_(compute.is_valid(values), compute.invert(compute.fill_null(missing, False)))
    if compute.all(keep, min_count=0).as_py():
        return stored
    kept_parents = compute.filter(parents, keep)
    return build_list(
        ENTRIES,
        dense_counts(kept_parents, len(stored)),
        compute.filter(entries, keep),
        null_mask(stored),
    )


def _plugin_keys(declared: Any) -> dict[str, dict[str, str]]:
    """Validate plugin key maps and fold only their matching identities."""
    found: dict[str, dict[str, str]] = {}
    for plugin, replacements in (declared or {}).items():
        code = str(plugin).strip().lower()
        if not code:
            raise ValueError("plugin_keys needs a non-empty plugin name")
        if not isinstance(replacements, dict):
            try:
                replacements = dict(replacements)
            except (TypeError, ValueError):
                raise TypeError(
                    f"plugin_keys[{plugin!r}] must map source keys to target keys"
                ) from None
        normalized: dict[str, str] = {}
        for source, target in replacements.items():
            folded = column_name(str(source))
            target = str(target).strip()
            if not folded or not target:
                raise ValueError(f"plugin_keys[{plugin!r}] needs non-empty source and target keys")
            previous = normalized.get(folded)
            if previous is not None and previous != target:
                raise ValueError(f"plugin_keys[{plugin!r}] gives folded key {folded!r} two targets")
            normalized[folded] = target
        previous = found.get(code)
        if previous is not None and previous != normalized:
            raise ValueError(f"plugin_keys gives plugin {plugin!r} two key maps")
        found[code] = normalized
    return found


def _renamed_keys(stored: pyarrow.Array, replacements: dict[str, str]) -> pyarrow.Array:
    """Replace matching terminal keys while preserving rows and wire order."""
    if not replacements or not len(stored):
        return stored
    compute = pyarrow.compute
    lengths = compute.fill_null(compute.list_value_length(stored), 0).cast(pyarrow.int64())
    entries = compute.list_flatten(stored)
    if not len(entries):
        return stored
    keys = compute.struct_field(entries, "key")
    sources = pyarrow.array(list(replacements), pyarrow.string())
    targets = pyarrow.array(list(replacements.values()), pyarrow.string())
    indices = compute.index_in(column_names(keys), value_set=sources)
    matched = compute.is_valid(indices)
    if not compute.any(matched, min_count=0).as_py():
        return stored
    renamed = compute.if_else(matched, compute.take(targets, indices), keys)
    target_tags = pyarrow.array(
        [Entry(key=target).tag for target in replacements.values()],
        TAG,
    )
    tags = compute.if_else(
        matched,
        compute.take(target_tags, indices),
        compute.struct_field(entries, "tag"),
    )
    rebuilt = pyarrow.StructArray.from_arrays(
        [
            tags,
            renamed,
            compute.struct_field(entries, "value"),
            compute.struct_field(entries, "comp"),
        ],
        fields=list(ENTRIES.value_type),
    )
    return build_list(ENTRIES, lengths, rebuilt, null_mask(stored))


def xml_payload_arrow(bodies: Any, selected: Any = None) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Parse selected XML bodies into ordered entries and row-local errors.

    XML has no Arrow kernel. The selected row slice is therefore the bounded
    unit: every document is released after its entries are built, and one bad
    document returns a diagnostic without changing its neighbours.
    """
    if isinstance(bodies, pyarrow.ChunkedArray):
        bodies = bodies.combine_chunks()
    rows = len(bodies)
    if not rows:
        return pyarrow.array([], type=ENTRIES), pyarrow.array([], pyarrow.string())
    selected_bodies, selected_at, other_at = _selected_payloads(bodies, selected)
    parsed: list[list[dict[str, Any]]] = []
    errors: list[str | None] = []
    for body in selected_bodies.to_pylist():
        try:
            raw = body.encode("utf-8") if isinstance(body, str) else bytes(body or b"")
            root = _xml_root(raw)
            parsed.append([entry.into_dict() for entry in _xml_entries(root)])
            errors.append(None)
        except (ElementTree.ParseError, UnicodeError, TypeError, ValueError) as error:
            parsed.append([])
            detail = " ".join(str(error).split()) or "no detail"
            errors.append(f"XML parse failed: {type(error).__name__}: {detail}"[:_XML_ERROR_LENGTH])
    return _restored_payloads(
        pyarrow.array(parsed, type=ENTRIES),
        pyarrow.array(errors, pyarrow.string()),
        selected_at,
        other_at,
    )


def referential_payload_arrow(
    bodies: Any, selected: Any = None
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Parse selected `Referential(...)` envelopes and isolate bad rows."""
    if isinstance(bodies, pyarrow.ChunkedArray):
        bodies = bodies.combine_chunks()
    rows = len(bodies)
    if not rows:
        return pyarrow.array([], type=ENTRIES), pyarrow.array([], pyarrow.string())
    selected_bodies, selected_at, other_at = _selected_payloads(bodies, selected)
    parsed: list[list[dict[str, Any]]] = []
    errors: list[str | None] = []
    for body in selected_bodies.to_pylist():
        try:
            text = body.decode("utf-8") if isinstance(body, bytes) else str(body or "")
            entries, detail = _referential_entries(text)
            parsed.append([entry.into_dict() for entry in entries])
            errors.append(detail)
        except (UnicodeError, TypeError, ValueError) as error:
            parsed.append([])
            detail = " ".join(str(error).split()) or "no detail"
            errors.append(
                f"Referential parse failed: {type(error).__name__}: {detail}"[
                    :_REFERENTIAL_ERROR_LENGTH
                ]
            )
    return _restored_payloads(
        pyarrow.array(parsed, type=ENTRIES),
        pyarrow.array(errors, pyarrow.string()),
        selected_at,
        other_at,
    )


def _selected_payloads(
    bodies: pyarrow.Array,
    selected: Any,
) -> tuple[pyarrow.Array, pyarrow.Array | None, pyarrow.Array | None]:
    """Only documents one protocol selected, with positions for restoration."""
    if selected is None:
        return bodies, None, None
    if isinstance(selected, pyarrow.ChunkedArray):
        selected = selected.combine_chunks()
    compute = pyarrow.compute
    selected = compute.fill_null(selected, False)
    if compute.all(selected, min_count=0).as_py():
        return bodies, None, None
    positions = sequence(len(bodies))
    selected_at = compute.filter(positions, selected)
    other_at = compute.filter(positions, compute.invert(selected))
    return compute.take(bodies, selected_at), selected_at, other_at


def _restored_payloads(
    entries: pyarrow.Array,
    errors: pyarrow.Array,
    selected_at: pyarrow.Array | None,
    other_at: pyarrow.Array | None,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Restore a protocol slice without building Python placeholders."""
    if selected_at is None or other_at is None:
        return entries, errors
    return (
        scattered([entries, pyarrow.nulls(len(other_at), ENTRIES)], [selected_at, other_at]),
        scattered(
            [errors, pyarrow.nulls(len(other_at), pyarrow.string())],
            [selected_at, other_at],
        ),
    )


def _referential_entries(text: str) -> tuple[list[Entry], str | None]:
    """One Referential envelope as ordered source and canonical tick entries."""
    found = _REFERENTIAL_START.search(text)
    if found is None:
        raise ValueError("no Referential envelope start")
    opened = text.find("(", found.start(), found.end())
    closed = _closing_parenthesis(text, opened)
    header = _depth_split(text[opened + 1 : closed], "|", maxsplit=3)
    if len(header) != 4:
        raise ValueError("expected venue|class|instrument-key|[bag]")
    venue, asset_class, instrument_key, raw_bag = (part.strip() for part in header)
    if not instrument_key:
        raise ValueError("instrument key is empty")
    bag = _unwrap_brackets(raw_bag)
    if bag == raw_bag.strip():
        raise ValueError("referential bag must be bracketed")

    entries = [
        *([Entry(key="Venue", value=venue, comp=_REFERENTIAL_COMP)] if venue else []),
        *(
            [Entry(key="AssetClass", value=asset_class, comp=_REFERENTIAL_COMP)]
            if asset_class
            else []
        ),
        Entry(key="InstrumentKey", value=instrument_key, comp=_REFERENTIAL_COMP),
    ]
    diagnostics: list[str] = []
    for token in _depth_split(bag, ","):
        token = token.strip()
        if not token:
            continue
        key, marker, value = token.partition("=")
        key, value = key.strip(), value.strip()
        if not marker or not key:
            diagnostics.append(f"invalid bag member {token!r}")
            continue
        # Entry values are required. Absence is represented by absence, which
        # lets nullable typed fields remain null without inventing a spelling.
        if not value:
            continue
        folded = column_name(key)
        if folded == _QUANTITY_TYPE:
            entries.append(Entry(key="QuantityType", value=value, comp=_REFERENTIAL_COMP))
            continue
        if folded == _TICK_SIZE_SCALE_ID:
            try:
                entries.extend(_tick_rule_entries(value))
            except ValueError as error:
                # Keep a source value an unknown grammar could not normalize;
                # one proprietary extension must not erase the instrument.
                entries.append(Entry(key=key, value=value, comp=_REFERENTIAL_COMP))
                diagnostics.append(str(error))
            continue
        entries.append(Entry(key=key, value=value, comp=_REFERENTIAL_COMP))
    detail = "; ".join(diagnostics)
    error = (
        f"Referential parse failed: ValueError: {detail}"[:_REFERENTIAL_ERROR_LENGTH]
        if detail
        else None
    )
    return entries, error


def _closing_parenthesis(text: str, opened: int) -> int:
    """Closing parenthesis outside nested square brackets."""
    if opened < 0:
        raise ValueError("no Referential opening parenthesis")
    parentheses = 1
    brackets = 0
    for index in range(opened + 1, len(text)):
        char = text[index]
        if char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
            if brackets < 0:
                raise ValueError("unmatched closing bracket")
        elif not brackets and char == "(":
            parentheses += 1
        elif not brackets and char == ")":
            parentheses -= 1
            if not parentheses:
                return index
    raise ValueError("unclosed Referential envelope")


def _depth_split(text: str, separator: str, *, maxsplit: int = -1) -> list[str]:
    """Split one-character separators only outside square brackets."""
    parts: list[str] = []
    start = 0
    depth = 0
    splits = 0
    for index, char in enumerate(text):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth < 0:
                raise ValueError("unmatched closing bracket")
        elif char == separator and not depth and (maxsplit < 0 or splits < maxsplit):
            parts.append(text[start:index])
            start = index + 1
            splits += 1
    if depth:
        raise ValueError("unclosed bracket")
    parts.append(text[start:])
    return parts


def _unwrap_brackets(text: str) -> str:
    """Remove one pair only where it encloses the complete value."""
    stripped = text.strip()
    if not stripped.startswith("["):
        return stripped
    depth = 0
    for index, char in enumerate(stripped):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth < 0:
                raise ValueError("unmatched closing bracket")
            if not depth:
                return stripped[1:index].strip() if index == len(stripped) - 1 else stripped
    raise ValueError("unclosed bracket")


def _tick_rule_entries(value: str) -> list[Entry]:
    """A source tick ladder normalized to FIX TickRules member names."""
    source = value.strip()
    first_bracket = source.find("[")
    scale_id = source[:first_bracket].rstrip(" |:=") if first_bracket > 0 else ""
    ladder = source[first_bracket:] if first_bracket >= 0 else source
    ladder = _unwrap_brackets(ladder)
    bands = [band.strip() for band in _depth_split(ladder, ",") if band.strip()]
    if not bands:
        raise ValueError("tick-size-scale-id has no bands")
    entries: list[Entry] = []
    if scale_id:
        entries.append(Entry(key="TickSizeScaleID", value=scale_id, comp=_REFERENTIAL_COMP))
    for index, band in enumerate(bands):
        parts = _depth_split(_unwrap_brackets(band), "|", maxsplit=1)
        if len(parts) != 2:
            raise ValueError(f"tick band {index} needs from|tick")
        start, tick = (_tick_number(part) for part in parts)
        if tick is None:
            raise ValueError(f"tick band {index} has no increment")
        component = f"TickRules[{index}]"
        if start is not None:
            entries.append(Entry(key="StartTickPriceRange", value=start, comp=component))
        entries.append(Entry(key="TickIncrement", value=tick, comp=component))
    return entries


def _tick_number(value: str) -> str | None:
    """One optional decimal spelling, allowing a named source member."""
    stripped = _unwrap_brackets(value).strip()
    if "=" in stripped:
        _name, stripped = stripped.split("=", 1)
        stripped = stripped.strip()
    if not stripped:
        return None
    if not _NUMBER.fullmatch(stripped):
        raise ValueError(f"invalid tick number {stripped!r}")
    return stripped


def _xml_root(raw: bytes) -> ElementTree.Element:
    """The first complete XML reading, then a transport-wrapped event fallback.

    A valid document keeps its outer root. If XML-looking transport prose made
    that reading fail, each later event envelope gets one bounded parse attempt.
    """
    leading = raw.lstrip()
    offset = len(raw) - len(leading)
    primary = _XML_START.search(raw, offset)
    events = tuple(_EVENT_XML_START.finditer(raw))
    candidates = (primary, *events) if leading.startswith(b"<") else (*events, primary)
    starts = tuple(dict.fromkeys(found.start() for found in candidates if found is not None))
    if not starts:
        raise ValueError("no XML document start")
    failure: ElementTree.ParseError | None = None
    for start in starts:
        try:
            return ElementTree.fromstring(raw[start:].strip())
        except ElementTree.ParseError as error:
            failure = error
    assert failure is not None
    raise failure


def _xml_entries(root: ElementTree.Element) -> list[Entry]:
    """One XML tree in source order, with indexed element paths as components."""
    entries: list[Entry] = []

    def visit(element: ElementTree.Element, parent: str | None, index: int) -> None:
        name = _xml_name(element.tag)
        component = f"{name}[{index}]" if parent is None else f"{parent}.{name}[{index}]"
        for key, value in element.attrib.items():
            entries.append(Entry(key=_xml_name(key), value=value, comp=component))

        children = list(element)
        text = (element.text or "").strip()
        if text:
            # A leaf element is the field its parent contains. Mixed content
            # belongs to the element itself under a neutral terminal.
            entries.append(
                Entry(
                    key=name if not children else "value",
                    value=text,
                    comp=parent if not children else component,
                )
            )

        counts: dict[str, int] = {}
        for child in children:
            child_name = _xml_name(child.tag)
            child_index = counts.get(child_name, 0)
            counts[child_name] = child_index + 1
            visit(child, component, child_index)
            tail = (child.tail or "").strip()
            if tail:
                entries.append(Entry(key="value", value=tail, comp=component))

    visit(root, None, 0)
    return entries


def _xml_name(name: Any) -> str:
    """An ElementTree QName as the local spelling a FIX registry can resolve."""
    text = str(name)
    return text.rsplit("}", 1)[-1] if "}" in text else text


def parse_arrow(messages):
    """Split text into ordered arguments without protocol interpretation."""
    if isinstance(messages, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [parse_arrow(chunk) for chunk in messages.chunks], type=ENTRIES
        )
    if not len(messages):
        return pyarrow.array([], type=ENTRIES)

    compute = pyarrow.compute
    text = _from_message_start(compute.fill_null(messages.cast(pyarrow.string(), safe=False), ""))
    common = _common_separators(text)
    common_rows = compute.is_valid(common)
    if compute.all(common_rows, min_count=0).as_py():
        return _parse_grouped(text, common)
    if compute.any(common_rows, min_count=0).as_py():
        positions = sequence(len(text))
        common_at = compute.filter(positions, common_rows)
        generic_at = compute.filter(positions, compute.invert(common_rows))
        return scattered(
            [
                _parse_grouped(
                    compute.filter(text, common_rows), compute.filter(common, common_rows)
                ),
                _parse_generic(compute.filter(text, compute.invert(common_rows))),
            ],
            [common_at, generic_at],
        )
    return _parse_generic(text)


def pop_arrow(
    stored,
    names: tuple[str, ...],
    *,
    case_sensitive: bool = True,
):
    """Return the first named value per row and every other argument."""
    if isinstance(stored, pyarrow.ChunkedArray):
        parts = [pop_arrow(chunk, names, case_sensitive=case_sensitive) for chunk in stored.chunks]
        return (
            pyarrow.chunked_array([found for found, _ in parts], pyarrow.string()),
            pyarrow.chunked_array([rest for _, rest in parts], ENTRIES),
        )

    rows = len(stored)
    if not rows:
        return pyarrow.nulls(0, pyarrow.string()), stored
    compute = pyarrow.compute
    entries = compute.list_flatten(stored)
    if not len(entries):
        return pyarrow.nulls(rows, pyarrow.string()), stored
    parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    keys = compute.struct_field(entries, "key")
    wanted = pyarrow.array(names, pyarrow.string())
    if not case_sensitive:
        keys = column_names(keys)
        wanted = column_names(wanted)
    matches = compute.fill_null(compute.is_in(keys, value_set=wanted), False)
    if not compute.any(matches, min_count=0).as_py():
        return pyarrow.nulls(rows, pyarrow.string()), stored

    matched_parents = compute.filter(parents, matches)
    previous = pyarrow.concat_arrays(
        [
            pyarrow.array([-1], pyarrow.int64()),
            matched_parents.slice(0, len(matched_parents) - 1),
        ]
    )
    first = compute.not_equal(matched_parents, previous)
    found = compute.scatter(
        compute.filter(compute.struct_field(entries, "value"), matches).filter(first),
        compute.filter(matched_parents, first),
        max_index=rows - 1,
    )

    keep = compute.invert(matches)
    kept_parents = compute.filter(parents, keep)
    residual = build_list(
        ENTRIES,
        dense_counts(kept_parents, rows),
        compute.filter(entries, keep),
        null_mask(stored),
    )
    return found, residual


def _from_message_start(text: pyarrow.Array) -> pyarrow.Array:
    """Each line from where its message starts, as `parse_pairs` cuts it.

    A log writes its own prose in front of the payload, and `seq=1092` in that
    prose is an assignment: a separator inferred from the whole line reads the
    `>` of `sending >>` as the delimiter and returns the message as one entry.
    A line that names no message keeps all of itself, which is what a generic
    `A=1;B=2` argument list is.
    """
    compute = pyarrow.compute
    begun = compute.struct_field(compute.extract_regex(text, BEGIN_VECTOR), "msg")
    bridged = compute.struct_field(compute.extract_regex(text, MARKED_VECTOR), "msg")
    return compute.coalesce(begun, bridged, text)


def _parse_generic(text: pyarrow.Array) -> pyarrow.Array:
    """Parse rows whose separator needs the complete inference rule."""
    compute = pyarrow.compute
    marked = compute.extract_regex(text, _MARKED_SEPARATOR)
    marked_sep = compute.struct_field(marked, "sep")
    marked_separator = compute.if_else(
        compute.fill_null(compute.greater(compute.binary_length(marked_sep), 0), False),
        marked_sep,
        compute.struct_field(marked, "marker"),
    )
    punctuated = compute.struct_field(
        compute.extract_regex(text, _PAIR_PUNCTUATION_SEPARATOR), "sep"
    )
    spaced = compute.struct_field(compute.extract_regex(text, _PAIR_WHITESPACE_SEPARATOR), "sep")
    trailing = compute.struct_field(compute.extract_regex(text, _TRAILING_SEPARATOR), "sep")
    separators = compute.fill_null(
        compute.coalesce(marked_separator, punctuated, spaced, trailing),
        _DEFAULT_SEPARATOR,
    )
    return _parse_grouped(text, separators)


def _parse_grouped(text: pyarrow.Array, separators: pyarrow.Array) -> pyarrow.Array:
    """Parse rows grouped by one already-settled separator."""
    compute = pyarrow.compute
    groups = list(groups_of(separators))
    if len(groups) == 1:
        return _parse_style(text, groups[0][0].as_py())
    parts, positions = [], []
    for separator, where in groups:
        parts.append(_parse_style(compute.take(text, where), separator.as_py()))
        positions.append(where)
    return scattered(parts, positions)


def _common_separators(text: pyarrow.Array) -> pyarrow.Array:
    """Pipe or SOH where it is the generic parser's first punctuation separator."""
    compute = pyarrow.compute
    assignments = compute.find_substring_regex(text, _ASSIGNMENT)
    punctuation = compute.find_substring_regex(text, _SEPARATOR_BEFORE_ASSIGNMENT)
    hashes = compute.find_substring(text, "#")
    marked = compute.starts_with(compute.utf8_ltrim_whitespace(text), "#")
    common: Any = pyarrow.nulls(len(text), pyarrow.string())
    for separator in ("|", "\x01", "\x04\x03"):
        found = compute.find_substring_regex(text, rf"{re.escape(separator)}{_WS}*{_ASSIGNMENT}")
        # A marked bridge frame declares its outer boundary by repeating the
        # marker. Inner group members may use SOH before the first outer pipe,
        # so the generic "first punctuation" rule would otherwise inspect the
        # expensive inference path and can choose the group's delimiter.
        marked_found = compute.find_substring_regex(
            text, rf"{re.escape(separator)}{_WS}*#{_BARE_KEY}{_WS}*="
        )
        marked_selected = compute.and_(marked, compute.greater_equal(marked_found, 0))
        selected = compute.and_(
            compute.and_(compute.greater_equal(found, 0), compute.equal(found, punctuation)),
            compute.and_(
                compute.less(assignments, found),
                # A marker behind the first delimiter is inside a value -- a
                # `Text <58>` quoting `#A=1` -- and reading it as the row's
                # separator cost the frame every field it had.
                compute.or_(compute.less(hashes, 0), compute.less(punctuation, hashes)),
            ),
        )
        selected = compute.or_(marked_selected, selected)
        common = compute.if_else(selected, pyarrow.scalar(separator), common)
    return common


def _parse_style(text: Any, separator: str) -> pyarrow.Array:
    """Parse one homogeneous separator style in Arrow kernels."""
    compute = pyarrow.compute
    marked_at = compute.find_substring_regex(text, rf"#{_BARE_KEY}{_WS}*=")
    unmarked_at = compute.find_substring_regex(
        text, rf"(?:^|[^A-Za-z0-9_.\-\]#]){_BARE_KEY}{_WS}*="
    )
    use_marked = compute.and_(
        compute.greater_equal(marked_at, 0),
        compute.or_(compute.less(unmarked_at, 0), compute.less(marked_at, unmarked_at)),
    )
    direct = compute.or_(compute.equal(marked_at, 0), compute.equal(unmarked_at, 0))
    if compute.all(direct, min_count=0).as_py():
        body = text
    else:
        marked_body = compute.struct_field(compute.extract_regex(text, _MARKED_BODY), "body")
        unmarked_body = compute.struct_field(compute.extract_regex(text, _UNMARKED_BODY), "body")
        extracted = compute.fill_null(compute.if_else(use_marked, marked_body, unmarked_body), "")
        body = compute.if_else(direct, text, extracted)
    # Split by the lengths a row declares where it declares one: a FIX `data`
    # value may hold the delimiter, and this stage's arguments are what the FIX
    # stage reads instead of the payload -- so a value cut here is cut for good.
    tokens = split_payload_arrow(body, separator)
    parsed = compute.extract_regex(tokens.values, _TOKEN)
    keys = compute.struct_field(parsed, "key")
    matched = compute.fill_null(compute.is_valid(keys), False)
    keys = compute.filter(keys, matched)
    if separator == "#":
        parents = compute.list_parent_indices(tokens)
        marked = compute.take(use_marked, compute.filter(parents, matched))
        keys = compute.if_else(
            marked,
            compute.binary_join_element_wise("#", keys, ""),
            keys,
        )
    values = compute.utf8_trim_whitespace(
        compute.fill_null(compute.filter(compute.struct_field(parsed, "value"), matched), "")
    )

    weights = matched.cast(pyarrow.int32())
    counted = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(weights)]
    )
    offsets = compute.take(counted, tokens.offsets)
    entries = pyarrow.StructArray.from_arrays(
        Entry.structure_arrow(keys, values), fields=list(Entry.into_field().dtype)
    )
    return pyarrow.ListArray.from_arrays(offsets, entries, type=ENTRIES)
