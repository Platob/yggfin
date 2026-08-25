"""One raw log row, before a protocol reads its payload."""

from __future__ import annotations

import functools
import re
from collections.abc import Collection, Mapping
from types import MappingProxyType
from typing import Any, Self

import pyarrow
import pyarrow.compute

from rekep.enums import EventType
from rekep.fields import scalar
from rekep.fields.arrays import build_list, dense_counts, null_mask, scattered, sequence
from rekep.market.event import Event
from rekep.text.kwargs import KWARGS, Kwarg

_CONTRACT_METADATA = MappingProxyType({"version": "4"})
_EVENT_CODE = pyarrow.int32()
_NO_PROTOCOL = "OTHER"
_DISCRIMINATOR_END = r"[ \t\r\n\f\x0b]*(?:\^A|[\x01|^;#]|$)"
_TOKEN_START = r"(?:^|\^A|[\x01|^;#])"
_MSG_TYPE_VALUE = r"^[A-Za-z0-9]+$"
_MSG_TYPE_VALUE_RE = re.compile(_MSG_TYPE_VALUE, re.ASCII)
_CHECKSUM_KEYS = ("10", "checksum", "trailer.10", "trailer.checksum")
_CHECKSUM_TOKEN = (
    rf"(?is){_TOKEN_START}[ \t\r\n\f\x0b]*#?"
    rf"(?:10|checksum|trailer\.10|trailer\.checksum)[ \t\r\n\f\x0b]*="
)

# Finding one discriminator is deliberately cheaper than tokenising the whole
# payload. Captures can contain long prose and stack traces; only a row that
# names a message kind is allowed into the key/value splitter.
_WIRE_MSG_TYPE = (
    rf"(?s){_TOKEN_START}[ \t\r\n\f\x0b]*#?35[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*(?P<value>[A-Za-z0-9]+){_DISCRIMINATOR_END}"
)
_NAMED_MSG_TYPE = (
    rf"(?is){_TOKEN_START}[ \t\r\n\f\x0b]*#?MsgType[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*(?P<value>[A-Za-z0-9]+){_DISCRIMINATOR_END}"
)
_FIX_BEGIN = (
    r"(?s)(?:^|[^A-Za-z0-9_.\-])#?8[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*FIXT?\.[0-9]+\.[0-9]+(?:SP[0-9]+)?{_DISCRIMINATOR_END}"
)


@scalar(slots=True)
class Message(Event):
    """One log header, its provenance, and its protocol-neutral payload."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with raw-message schemas."""
        return _CONTRACT_METADATA

    source_url: str = ""
    """Path of the log the row came from, as its filesystem addresses it."""

    source_rownum: int = 0
    """1-based physical line number of the header; 0 when not read from a file."""

    thread_name: str = ""
    """Contents of the first bracketed header field."""

    plugin_code: str = ""
    """Contents of the second bracketed header field."""

    message: str = ""
    """Payload after the fixed log header, with continuation lines folded in."""

    protocol_code: str = _NO_PROTOCOL
    """Protocol syntax detected without interpreting its fields."""

    MsgType: str | None = None
    """First FIX message discriminator when the payload names one."""

    kwargs: list[Kwarg] = None  # type: ignore[assignment]
    """Ordered payload arguments other than the promoted message discriminator."""

    def __post_init__(self) -> None:
        """Normalize arguments and promote the protocol-neutral discriminator."""
        Event.__post_init__(self)
        implicit_kwargs = self.kwargs is None
        if implicit_kwargs:
            self.kwargs = []
        if implicit_kwargs and self.message:
            parsed = self.parse_arrow(pyarrow.array([self.message]))
            self.kwargs = parsed["kwargs"][0].as_py()
            if self.protocol_code == _NO_PROTOCOL:
                self.protocol_code = parsed["protocol_code"][0].as_py()
            if self.MsgType is None:
                self.MsgType = parsed["MsgType"][0].as_py()
            if self.etype == EventType.UNKNOWN:
                self.etype = EventType(parsed["etype"][0].as_py())

        self.kwargs = [Kwarg.from_stored(entry) for entry in self.kwargs]
        wire, named, self.kwargs = _scalar_message_types(self.kwargs)
        if self.MsgType is None:
            hybrid = wire and wire.startswith("U") and named
            self.MsgType = named if hybrid else wire or named
        if self.MsgType is None and self.etype == EventType.UNKNOWN:
            self.etype = EventType.MISC

    @classmethod
    def parse_arrow(
        cls,
        messages: Any,
        msg_type_event_types: Mapping[str, EventType | int | str] | None = None,
        plugins: Any | None = None,
        technical_msg_types: Collection[str] = (),
        technical_plugin_codes: Collection[str] = (),
        protocol_rules: Any | None = None,
    ) -> dict[str, Any]:
        """Promote discriminators and parse only structured payload rows."""
        if isinstance(messages, pyarrow.ChunkedArray):
            offsets, parts = 0, []
            for chunk in messages.chunks:
                plugin_chunk = None if plugins is None else plugins.slice(offsets, len(chunk))
                parts.append(
                    cls.parse_arrow(
                        chunk,
                        msg_type_event_types,
                        plugin_chunk,
                        technical_msg_types,
                        technical_plugin_codes,
                        protocol_rules,
                    )
                )
                offsets += len(chunk)
            return {
                "etype": pyarrow.chunked_array([part["etype"] for part in parts], _EVENT_CODE),
                "protocol_code": pyarrow.chunked_array(
                    [part["protocol_code"] for part in parts], pyarrow.string()
                ),
                "MsgType": pyarrow.chunked_array(
                    [part["MsgType"] for part in parts], pyarrow.string()
                ),
                "kwargs": pyarrow.chunked_array([part["kwargs"] for part in parts], KWARGS),
            }

        rows = len(messages)
        if not rows:
            return {
                "etype": pyarrow.array([], _EVENT_CODE),
                "protocol_code": pyarrow.array([], pyarrow.string()),
                "MsgType": pyarrow.nulls(0, pyarrow.string()),
                "kwargs": pyarrow.array([], type=KWARGS),
            }

        compute = pyarrow.compute
        text = compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
        wire_match = compute.extract_regex(text, _WIRE_MSG_TYPE)
        named_match = compute.extract_regex(text, _NAMED_MSG_TYPE)
        wire_values = compute.struct_field(wire_match, "value")
        named_values = compute.struct_field(named_match, "value")
        checksum_at = compute.find_substring_regex(text, _CHECKSUM_TOKEN)
        wire_at = compute.find_substring_regex(text, _WIRE_MSG_TYPE)
        named_at = compute.find_substring_regex(text, _NAMED_MSG_TYPE)
        wire_probe = _before_checksum(wire_at, checksum_at)
        named_probe = _before_checksum(named_at, checksum_at)
        missing = pyarrow.scalar(None, pyarrow.string())
        wire_values = compute.if_else(wire_probe, wire_values, missing)
        named_values = compute.if_else(named_probe, named_values, missing)
        begins_fix = compute.fill_null(compute.match_substring_regex(text, _FIX_BEGIN), False)
        wrapped = compute.and_(
            compute.fill_null(compute.starts_with(wire_values, "U"), False), named_probe
        )
        probed_msg_types = compute.if_else(
            wrapped, named_values, compute.coalesce(wire_values, named_values)
        )
        technical = _technical_rows(
            probed_msg_types,
            plugins,
            technical_msg_types,
            technical_plugin_codes,
        )
        candidates = compute.or_(
            compute.or_(compute.or_(wire_probe, named_probe), begins_fix),
            _structured_rows(text, technical),
        )
        candidates = compute.and_(candidates, compute.invert(technical))
        kwargs = _candidate_kwargs(text, candidates)
        parsed_msg_types, kwargs = _message_types(kwargs)
        msg_types = compute.if_else(technical, probed_msg_types, parsed_msg_types)
        event_types = _event_types(msg_types, msg_type_event_types)
        protocols = (
            _configured_protocol_codes(text, plugins, technical, protocol_rules)
            if protocol_rules is not None
            else compute.if_else(
                technical,
                pyarrow.scalar("MISC"),
                _protocol_codes(wire_values, wire_probe, named_probe, begins_fix),
            )
        )
        return {
            "etype": compute.if_else(
                technical,
                pyarrow.scalar(int(EventType.MISC), _EVENT_CODE),
                event_types,
            ),
            "protocol_code": protocols,
            "MsgType": msg_types,
            "kwargs": kwargs,
        }

    def identify(self) -> Self:
        """Give this raw row its provenance-scoped content identity."""
        if not self.hash:
            self.hash = self.hash_of(self.message, self.source_url, self.source_rownum)
        self.xhash = self.hash
        return self

    @classmethod
    def identified(
        cls, columns: dict[str, Any], schema: pyarrow.Schema, rows: int
    ) -> pyarrow.RecordBatch:
        """Build a batch after assigning raw row identities in Arrow kernels."""
        columns["hash"] = cls.hash_arrow(
            columns["message"], columns["source_url"], columns["source_rownum"]
        )
        columns["xhash"] = columns["hash"]
        return pyarrow.RecordBatch.from_arrays(
            [columns[name] for name in schema.names], schema=schema
        )


def _scalar_message_types(
    kwargs: list[Kwarg],
) -> tuple[str | None, str | None, list[Kwarg]]:
    """Valid top-level discriminators before the FIX checksum."""
    wire = named = None
    residual: list[Kwarg] = []
    ended = False
    for entry in kwargs:
        key = entry.key.removeprefix("#")
        folded = key.lower()
        if folded in _CHECKSUM_KEYS:
            ended = True
        is_wire = not ended and folded == "35"
        is_named = not ended and folded == "msgtype"
        valid = _MSG_TYPE_VALUE_RE.fullmatch(entry.value) is not None
        if is_wire and valid:
            wire = entry.value if wire is None else wire
        elif is_named and valid:
            named = entry.value if named is None else named
        else:
            residual.append(entry)
    return wire, named, residual


def _message_types(stored: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Promote parsed top-level discriminators before each row's checksum."""
    rows = len(stored)
    if not rows:
        return pyarrow.nulls(0, pyarrow.string()), stored
    compute = pyarrow.compute
    entries = compute.list_flatten(stored)
    if not len(entries):
        return pyarrow.nulls(rows, pyarrow.string()), stored

    parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    positions = sequence(len(entries))
    keys = compute.struct_field(entries, "key")
    values = compute.struct_field(entries, "value")
    normalized = compute.utf8_lower(
        compute.replace_substring_regex(keys, pattern=r"^#", replacement="")
    )
    checksums = compute.fill_null(
        compute.is_in(normalized, value_set=pyarrow.array(_CHECKSUM_KEYS)), False
    )
    checksum_at = _first_by_parent(positions, parents, checksums, rows)
    before_checksum = compute.fill_null(
        compute.less(positions, compute.take(checksum_at, parents)), True
    )
    valid_values = compute.fill_null(compute.match_substring_regex(values, _MSG_TYPE_VALUE), False)
    eligible = compute.and_(before_checksum, valid_values)
    wire_mask = compute.and_(eligible, compute.equal(normalized, "35"))
    named_mask = compute.and_(eligible, compute.equal(normalized, "msgtype"))
    wire = _first_by_parent(values, parents, wire_mask, rows)
    named = _first_by_parent(values, parents, named_mask, rows)
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire, "U"), False),
        compute.is_valid(named),
    )
    msg_types = compute.if_else(wrapped, named, compute.coalesce(wire, named))

    keep = compute.invert(compute.or_(wire_mask, named_mask))
    kept_parents = compute.filter(parents, keep)
    residual = build_list(
        KWARGS,
        dense_counts(kept_parents, rows),
        compute.filter(entries, keep),
        null_mask(stored),
    )
    return msg_types, residual


def _first_by_parent(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    selected: pyarrow.Array,
    rows: int,
) -> pyarrow.Array:
    """First selected value for each dense parent row."""
    selected_parents = pyarrow.compute.filter(parents, selected)
    if not len(selected_parents):
        return pyarrow.nulls(rows, values.type)
    previous = pyarrow.concat_arrays(
        [
            pyarrow.array([-1], pyarrow.int64()),
            selected_parents.slice(0, len(selected_parents) - 1),
        ]
    )
    first = pyarrow.compute.not_equal(selected_parents, previous)
    return pyarrow.compute.scatter(
        pyarrow.compute.filter(values, selected).filter(first),
        pyarrow.compute.filter(selected_parents, first),
        max_index=rows - 1,
    )


def _candidate_kwargs(text: pyarrow.Array, candidates: pyarrow.Array) -> pyarrow.Array:
    """Parse candidate rows and scatter empty lists into skipped prose rows."""
    compute = pyarrow.compute
    rows = len(text)
    if not compute.any(candidates, min_count=0).as_py():
        return pyarrow.repeat(pyarrow.scalar([], KWARGS), rows)
    if compute.all(candidates, min_count=0).as_py():
        return Kwarg.parse_arrow(text)

    positions = sequence(rows)
    selected_at = compute.filter(positions, candidates)
    skipped_at = compute.filter(positions, compute.invert(candidates))
    parsed = Kwarg.parse_arrow(compute.filter(text, candidates))
    skipped = pyarrow.repeat(pyarrow.scalar([], KWARGS), len(skipped_at))
    return scattered([parsed, skipped], [selected_at, skipped_at])


def _structured_rows(text: pyarrow.Array, skipped: pyarrow.Array) -> pyarrow.Array:
    """Structured-pair probes only on rows not already known as technical."""
    compute = pyarrow.compute
    if not compute.any(skipped, min_count=0).as_py():
        return Kwarg.looks_structured_arrow(text)
    keep = compute.invert(skipped)
    if not compute.any(keep, min_count=0).as_py():
        return pyarrow.repeat(False, len(text))
    positions = sequence(len(text))
    kept_at = compute.filter(positions, keep)
    skipped_at = compute.filter(positions, skipped)
    return scattered(
        [
            Kwarg.looks_structured_arrow(compute.filter(text, keep)),
            pyarrow.repeat(False, len(skipped_at)),
        ],
        [kept_at, skipped_at],
    )


def _event_types(
    msg_types: pyarrow.Array,
    declared: Mapping[str, EventType | int | str] | None,
) -> pyarrow.Array:
    """Map known discriminators, separating absent from unknown values."""
    compute = pyarrow.compute
    rows = len(msg_types)
    unknown = pyarrow.scalar(int(EventType.UNKNOWN), _EVENT_CODE)
    found: Any = pyarrow.repeat(unknown, rows)
    if declared:
        keys = [str(key) for key in declared]
        codes = pyarrow.array([_event_code(value) for value in declared.values()], _EVENT_CODE)
        indices = compute.index_in(msg_types, value_set=pyarrow.array(keys, pyarrow.string()))
        found = compute.fill_null(compute.take(codes, indices), unknown)
    return compute.if_else(
        compute.is_null(msg_types),
        pyarrow.scalar(int(EventType.MISC), _EVENT_CODE),
        found,
    ).cast(_EVENT_CODE, safe=False)


def _protocol_codes(
    wire_values: pyarrow.Array,
    wire_probe: pyarrow.Array,
    named_probe: pyarrow.Array,
    begins_fix: pyarrow.Array,
) -> pyarrow.Array:
    """Protocol syntax classified before the payload is discarded."""
    compute = pyarrow.compute
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire_values, "U"), False), named_probe
    )
    named = compute.or_(wrapped, compute.and_(named_probe, compute.invert(wire_probe)))
    return compute.if_else(
        named,
        pyarrow.scalar("UL"),
        compute.if_else(
            compute.or_(begins_fix, wire_probe),
            pyarrow.scalar("FIX"),
            pyarrow.scalar(_NO_PROTOCOL),
        ),
    ).cast(pyarrow.string(), safe=False)


def _configured_protocol_codes(
    text: pyarrow.Array,
    plugins: Any | None,
    skipped: pyarrow.Array,
    rules: Any,
) -> pyarrow.Array:
    """Classify only rows not excluded by registry-owned technical metadata."""
    compute = pyarrow.compute
    rows = len(text)
    keep = compute.invert(skipped)
    if not compute.any(keep, min_count=0).as_py():
        return pyarrow.repeat(pyarrow.scalar("MISC"), rows)
    if compute.all(keep, min_count=0).as_py():
        return rules.into_arrow_protocol_array(text, plugins)
    positions = sequence(rows)
    kept_at = compute.filter(positions, keep)
    skipped_at = compute.filter(positions, skipped)
    selected_plugins = None if plugins is None else compute.filter(plugins, keep)
    classified = rules.into_arrow_protocol_array(compute.filter(text, keep), selected_plugins)
    misc = pyarrow.repeat(pyarrow.scalar("MISC"), len(skipped_at))
    return scattered([classified, misc], [kept_at, skipped_at])


def _before_checksum(candidate_at: pyarrow.Array, checksum_at: pyarrow.Array) -> pyarrow.Array:
    """A discriminator exists and precedes the first checksum token."""
    compute = pyarrow.compute
    exists = compute.greater_equal(candidate_at, 0)
    return compute.and_(
        exists,
        compute.or_(compute.less(checksum_at, 0), compute.less(candidate_at, checksum_at)),
    )


def _technical_rows(
    msg_types: pyarrow.Array,
    plugins: Any | None,
    technical_msg_types: Collection[str],
    technical_plugin_codes: Collection[str],
) -> pyarrow.Array:
    """Configured operational traffic excluded before protocol translation."""
    compute = pyarrow.compute
    found = pyarrow.repeat(False, len(msg_types))
    if technical_msg_types:
        found = compute.fill_null(
            compute.is_in(
                msg_types,
                value_set=pyarrow.array(tuple(technical_msg_types), pyarrow.string()),
            ),
            False,
        )
    if plugins is not None and technical_plugin_codes:
        values = pyarrow.array(
            sorted({str(value).casefold() for value in technical_plugin_codes}),
            pyarrow.string(),
        )
        plugin_rows = compute.fill_null(
            compute.is_in(
                compute.utf8_lower(plugins.cast(pyarrow.string(), safe=False)),
                value_set=values,
            ),
            False,
        )
        found = compute.or_(found, plugin_rows)
    return found


def _event_code(value: EventType | int | str) -> int:
    """One configurable event spelling as its stable stored integer."""
    if isinstance(value, str):
        try:
            return int(EventType[value.upper()])
        except KeyError:
            return int(value)
    return int(value)
