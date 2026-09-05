"""Vectorized parsing of raw message bodies at the FIX boundary."""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.enums import Direction, EventType, Protocol
from rekep.fields import column_name
from rekep.fields.arrays import build_list, dense_counts, list_parts, null_mask, sequence
from rekep.fix.columns import DECLARATIONS, SESSION
from rekep.fix.message import (
    FIX_MSG_TYPE_PATTERN,
    NAMED_MSG_TYPE_PATTERN,
    TOKEN_START,
    rendered_name,
)
from rekep.text.entries import ENTRIES, Entry, referential_payload_arrow, xml_payload_arrow

#: Storage types emitted beside the parser's residual entries.
_DIRECTION_CODE = Direction.into_storage_type()
_EVENT_CODE = pyarrow.int64()
_PROTOCOL_CODE = Protocol.into_storage_type()
_WS = r"[ \t\r\n\f\x0b]"
_MSG_TYPE_VALUE = r"^[A-Za-z0-9]+$"
_MSG_TYPE_VALUE_RE = re.compile(_MSG_TYPE_VALUE, re.ASCII)
_CHECKSUM_KEYS = tuple(
    column_name(name) for name in ("10", "CheckSum", "Trailer.10", "Trailer.CheckSum")
)


@functools.cache
def _default_protocol_codec() -> Any:
    """Packaged registry and rules for direct parser calls."""
    from rekep.fix.transcribe import FixCodec

    return FixCodec()


#: Session fields promoted while the body is tokenized. `CheckSum <10>` bounds
#: the frame. `XmlDataLen <212>` and `XmlData <213>` remain together so payload
#: expansion can read the complete token pair.
_UNLIFTED: frozenset[str] = frozenset({"CheckSum", "XmlDataLen", "XmlData"})

SESSION_NAMES: tuple[tuple[str, str], ...] = tuple(
    (DECLARATIONS[tag].fix.canonical, str(tag))
    for tag, _ in SESSION
    if DECLARATIONS[tag].fix.canonical not in _UNLIFTED
)

#: Folded session column and tag pairs used by the Arrow parser.
SESSION_FIELDS: tuple[tuple[str, str], ...] = tuple(
    (column_name(name), tag) for name, tag in SESSION_NAMES
)

#: The one session field whose value the standard constrains, and the one whose
#: `U`-prefixed wire spelling defers to a rendered name beside it.
_MSG_TYPE = column_name("MsgType")


#: `{folded spelling: column}` for every session field, which is the lookup a
#: parse actually does -- one probe per entry rather than one pass per field.
#:
#: The **tag** only, except for the discriminator. A bridge that renders its
#: header writes its own names -- `#BeginString=`, `#SendingTime=` -- and this
#: stage keeps a rendered spelling exactly as it arrived, because which name a
#: feed uses is data. `MsgType` is the one field that has always answered to
#: both, and it keeps doing so: a `35=U1` wrapper naming its real type beside
#: it is the whole reason the rendered spelling is read at all.
_SESSION_BY_KEY: Mapping[str, str] = MappingProxyType(
    {**{tag: name for name, tag in SESSION_FIELDS}, "msgtype": _MSG_TYPE}
)

#: The same lookup as a number, which is how a batch answers it: a key column
#: is read through its distinct spellings, so one `take` off the folded
#: dictionary gives every entry the code of the field it spells and the
#: per-field masks below are integer comparisons rather than thirty passes over
#: the strings. The two negative codes are the answers that are not a column: a
#: checksum token, which bounds every lift, and everything else.
_UNCLAIMED = -1
_CHECKSUM_CODE = -2
_MSG_TYPE_CODE = next(code for code, (name, _) in enumerate(SESSION_FIELDS) if name == _MSG_TYPE)
#: The rendered spelling of the discriminator, coded apart from its tag because
#: each has to agree with itself before the rule between them applies.
_NAMED_MSG_TYPE_CODE = len(SESSION_FIELDS)
_SESSION_CODES: Mapping[str, int] = MappingProxyType(
    {
        **{tag: code for code, (_, tag) in enumerate(SESSION_FIELDS)},
        "msgtype": _NAMED_MSG_TYPE_CODE,
        **dict.fromkeys(_CHECKSUM_KEYS, _CHECKSUM_CODE),
    }
)

#: A checksum token, however the trailer spells it: the boundary a promoted
#: discriminator has to stand in front of.
_CHECKSUM_TOKEN = (
    rf"(?is){TOKEN_START}{_WS}*#?"
    rf"(?:10|{rendered_name('CheckSum')}|{rendered_name('Trailer.10')}|"
    rf"{rendered_name('Trailer.CheckSum')}){_WS}*="
)


class MessageParser:
    """Vectorized protocol parsing for raw message bodies."""

    @classmethod
    def parse_arrow(
        cls,
        bodies: Any,
        msg_type_event_types: Mapping[str, EventType | int | str] | None = None,
        plugins: Any | None = None,
        protocol_codec: Any | None = None,
        plugin_keys: Mapping[str, Mapping[str, str]] | None = None,
        null_values: Any = (),
    ) -> dict[str, Any]:
        """Promote discriminators and parse only structured payload rows."""
        if isinstance(bodies, pyarrow.ChunkedArray):
            offsets, parts = 0, []
            for chunk in bodies.chunks:
                plugin_chunk = None if plugins is None else plugins.slice(offsets, len(chunk))
                parts.append(
                    cls.parse_arrow(
                        chunk,
                        msg_type_event_types,
                        plugin_chunk,
                        protocol_codec,
                        plugin_keys,
                        null_values,
                    )
                )
                offsets += len(chunk)
            return {
                **{
                    name: pyarrow.chunked_array([part[name] for part in parts], pyarrow.string())
                    for name, _ in SESSION_FIELDS
                },
                "eventtype": pyarrow.chunked_array(
                    [part["eventtype"] for part in parts], _EVENT_CODE
                ),
                "protocol": pyarrow.chunked_array(
                    [part["protocol"] for part in parts], _PROTOCOL_CODE
                ),
                "entries": pyarrow.chunked_array([part["entries"] for part in parts], ENTRIES),
                "parseerror": pyarrow.chunked_array(
                    [part["parseerror"] for part in parts], pyarrow.string()
                ),
                "direction": pyarrow.chunked_array(
                    [part["direction"] for part in parts], _DIRECTION_CODE
                ),
            }

        rows = len(bodies)
        if not rows:
            return {
                **{name: pyarrow.nulls(0, pyarrow.string()) for name, _ in SESSION_FIELDS},
                "eventtype": pyarrow.array([], _EVENT_CODE),
                "protocol": pyarrow.array([], _PROTOCOL_CODE),
                "entries": pyarrow.array([], type=ENTRIES),
                "parseerror": pyarrow.array([], pyarrow.string()),
                "direction": pyarrow.array([], _DIRECTION_CODE),
            }

        compute = pyarrow.compute
        text = _body_text_arrow(bodies)
        raw_entries, token_errors = Entry.payload_arrow_with_diagnostics(text)
        entries = Entry.normalized_arrow(raw_entries, plugins, plugin_keys, null_values)
        # The pairs this stage just split are what a protocol is decided by, so
        # they are handed over rather than parsed a second time -- and before
        # the header is lifted out of them, because a frame whose every numbered
        # tag is a session field is still a frame.
        codec = _default_protocol_codec() if protocol_codec is None else protocol_codec
        rules = codec.rules
        protocols = rules.into_arrow_protocol_array(text, plugins, entries)
        families = Protocol.into_family_arrow(protocols)
        xml = compute.equal(families, Protocol.XML.into_stored())
        referential = compute.equal(families, Protocol.REFERENTIAL.into_stored())
        entries, parse_errors = _reparsed_entries(
            xml_payload_arrow, bodies, xml, entries, plugins, plugin_keys, null_values
        )
        entries, referential_errors = _reparsed_entries(
            referential_payload_arrow,
            bodies,
            referential,
            entries,
            plugins,
            plugin_keys,
            null_values,
        )
        reparsed = compute.or_(xml, referential)
        token_errors = compute.if_else(
            reparsed,
            pyarrow.nulls(rows, pyarrow.string()),
            token_errors,
        )
        parse_errors = _merge_error_columns(token_errors, parse_errors)
        parse_errors = _merge_error_columns(parse_errors, referential_errors)
        session, entries = _session_columns(entries)
        protocols = codec.into_versioned_protocols(
            entries,
            session.get("beginstring"),
            session.get("applverid"),
            protocols,
        )
        msg_types = _resolved_msg_types(session[_MSG_TYPE], text)
        event_types = _event_types(msg_types, msg_type_event_types)
        event_types = compute.if_else(
            referential,
            pyarrow.scalar(int(EventType.INSTRUMENT), _EVENT_CODE),
            event_types,
        )
        # Direction is resolved at the parsing boundary while the raw line and
        # its protocol coexist. Parsed tables never retain `body`.
        direction = rules.into_arrow_direction_array(text, protocols)
        return {
            **session,
            "eventtype": event_types,
            "protocol": protocols,
            _MSG_TYPE: msg_types,
            "entries": entries,
            "parseerror": parse_errors,
            "direction": direction,
        }


#: Rows decoded in Python once a run refuses to validate. Halving stops here:
#: a smaller leaf spends more on casts than it saves, and a larger one drags
#: valid neighbours through `to_pylist`. One dirty row among 65,536 costs
#: 0.19 MiB of Python heap at this size and 183 MiB with no leaf at all.
_REPAIR_ROW_SIZE = 64


def repaired_text_arrow(binary: pyarrow.Array) -> pyarrow.Array:
    """Bytes read as UTF-8, decoding in Python only the rows Arrow refuses.

    Arrow rejects the whole array and never names the row, so one dirty body
    used to send every row beside it through `to_pylist` -- heap the Arrow pool
    cannot see, and which the allocator does not give back. The rows are found
    by halving instead: a run that validates is cast inside Arrow, and only a
    `_REPAIR_ROW_SIZE` leaf reaches Python. Arrow's validator and CPython's
    strict decoder accept the same bytes, so a run Arrow casts holds exactly
    what `decode("utf-8", "replace")` would have returned for it.
    """
    try:
        return binary.cast(pyarrow.string())
    except pyarrow.ArrowInvalid:
        pieces: list[pyarrow.Array] = []
        _repaired_pieces(binary, pieces)
        return pyarrow.concat_arrays(pieces)


def _repaired_pieces(binary: pyarrow.Array, pieces: list[pyarrow.Array]) -> None:
    """Append `binary` as UTF-8, halving until a run validates or is a leaf."""
    try:
        pieces.append(binary.cast(pyarrow.string()))
        return
    except pyarrow.ArrowInvalid:
        pass
    if len(binary) <= _REPAIR_ROW_SIZE:
        pieces.append(
            pyarrow.array(
                [
                    None if value is None else value.decode("utf-8", "replace")
                    for value in binary.to_pylist()
                ],
                pyarrow.string(),
            )
        )
        return
    half = len(binary) // 2
    _repaired_pieces(binary.slice(0, half), pieces)
    _repaired_pieces(binary.slice(half), pieces)


def _body_text_arrow(bodies: Any) -> pyarrow.Array:
    """A fault-tolerant UTF-8 parsing view over exact binary bodies."""
    if isinstance(bodies, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [_body_text_arrow(chunk) for chunk in bodies.chunks], pyarrow.string()
        )
    binary = bodies.cast(pyarrow.binary(), safe=False)
    return pyarrow.compute.fill_null(repaired_text_arrow(binary), "")


def _merged_reason(current: str | None, added: str) -> str:
    """Append one parser diagnostic without hiding an earlier row reason."""
    return f"{current}; {added}" if current else added


def _reparsed_entries(
    parse: Any,
    bodies: Any,
    selected: Any,
    entries: Any,
    plugins: Any,
    plugin_keys: Any,
    null_values: Any,
) -> tuple[Any, pyarrow.Array]:
    """One protocol's own reading of the rows it claims, merged over the batch.

    `if_else` on `ENTRIES` has no all-false short circuit, so a batch the
    protocol never appears in still paid a whole copy of the column to
    overwrite no row: a 200k-row FIX batch peaked at 255.5 MiB of Arrow and
    4170 ms against 203.6 MiB and 3722 ms once both branches are gated. The
    diagnostics stay full length and all-null on the skipped batch, because
    `_merge_error_columns` reads them beside it either way.
    """
    compute = pyarrow.compute
    if not selected.null_count and not compute.any(selected, min_count=0).as_py():
        return entries, pyarrow.nulls(len(entries), pyarrow.string())
    parsed, errors = parse(bodies, selected)
    parsed = Entry.normalized_arrow(parsed, plugins, plugin_keys, null_values)
    return compute.if_else(selected, parsed, entries), errors


def _merge_error_columns(current: Any, added: Any) -> pyarrow.Array:
    """Append nullable parser diagnostics without hiding an earlier one."""
    compute = pyarrow.compute
    left = compute.fill_null(current.cast(pyarrow.string(), safe=False), "")
    right = compute.fill_null(added.cast(pyarrow.string(), safe=False), "")
    both = compute.and_(compute.not_equal(left, ""), compute.not_equal(right, ""))
    joined = compute.binary_join_element_wise(left, compute.if_else(both, "; ", ""), right, "")
    return compute.if_else(
        compute.equal(joined, ""), pyarrow.nulls(len(joined), pyarrow.string()), joined
    )


def _scalar_session_values(entries: list[Entry]) -> tuple[dict[str, str], list[Entry]]:
    """Every standard header field before the checksum, and what is left.

    The scalar twin of `_session_columns`, rule for rule: a field spelled
    twice with two readings is not lifted, and a `U`-prefixed wire
    discriminator defers to a rendered name beside it.
    """
    claimed: dict[str, list[int]] = {}
    residual: list[int] = []
    # The discriminator's two spellings are claimed apart, because each has to
    # agree with itself before the rule between them applies.
    spellings: dict[str, list[int]] = {"35": [], "msgtype": []}
    ended = False
    for index, entry in enumerate(entries):
        folded = column_name(entry.key)
        if folded in _CHECKSUM_KEYS:
            ended = True
        column = None if ended else _SESSION_BY_KEY.get(folded)
        if column == _MSG_TYPE:
            if _MSG_TYPE_VALUE_RE.fullmatch(entry.value) is None:
                column = None
            else:
                spellings["35" if folded == "35" else "msgtype"].append(index)
        if column is None:
            residual.append(index)
        else:
            claimed.setdefault(column, []).append(index)

    def agreed(where: list[int]) -> str | None:
        """The one value those entries state, or None when they state two."""
        values = {entries[index].value for index in where}
        if len(values) == 1:
            return values.pop()
        # Two readings of one fact is not one statement of it: both stay where
        # a reader can see them, and the column says nothing.
        residual.extend(where)
        return None

    found: dict[str, str] = {}
    for column, where in claimed.items():
        if column == _MSG_TYPE:
            # The discriminator has a rule of its own for its two spellings, so
            # they disagreeing is expected rather than torn.
            wire = agreed(spellings["35"]) if spellings["35"] else None
            named = agreed(spellings["msgtype"]) if spellings["msgtype"] else None
            hybrid = wire and wire.startswith("U") and named
            value = named if hybrid else (wire or named)
            if value is not None:
                found[column] = value
            continue
        value = agreed(where)
        if value is not None:
            found[column] = value
    residual.sort()
    return found, [entries[index] for index in residual]


def _session_columns(stored: pyarrow.Array) -> tuple[dict[str, pyarrow.Array], pyarrow.Array]:
    """Lift every standard header field out of `entries`, before each checksum.

    One pass for all of them: every entry gets the code of the field it spells
    from one `take` off its column's distinct spellings, the entries no field
    spells leave before any per-field work runs, and each field is then an
    integer mask over the fifth of the list that is left. A field a row spells
    twice with two different values is left where it is and its column stays
    null -- the same rule the FIX stage applies when it lifts, because a bridge
    that writes one fact twice on purpose is telling the reader something a
    first-wins pop would throw away.
    """
    rows = len(stored)
    empty = {name: pyarrow.nulls(rows, pyarrow.string()) for name, _ in SESSION_FIELDS}
    if not rows:
        return {name: pyarrow.nulls(0, pyarrow.string()) for name in empty}, stored
    compute = pyarrow.compute
    _, entries = list_parts(stored)
    if not len(entries):
        return empty, stored

    all_parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    all_positions = sequence(len(entries))
    codes = _session_codes(compute.struct_field(entries, "key"))
    checksum_at = _first_by_parent(
        all_positions, all_parents, compute.equal(codes, _CHECKSUM_CODE), rows
    )
    row_checksum_at = compute.take(checksum_at, all_parents)
    in_frame = compute.fill_null(
        compute.or_(
            compute.less(row_checksum_at, 0),
            compute.less_equal(all_positions, row_checksum_at),
        ),
        True,
    )
    claims = compute.fill_null(compute.greater_equal(codes, 0), False)
    # Everything below reads only the entries a session field could claim --
    # a fifth of an ordinary list -- so `positions` doubles as where each of
    # them came from, which is what puts the claimed ones back at the end.
    positions = compute.filter(all_positions, claims)
    if not len(positions):
        return empty, build_list(
            ENTRIES,
            dense_counts(compute.filter(all_parents, in_frame), rows),
            compute.filter(entries, in_frame),
            null_mask(stored),
        )
    codes = compute.filter(codes, claims)
    parents = compute.filter(all_parents, claims)
    values = compute.filter(compute.struct_field(entries, "value"), claims)
    before_checksum = compute.fill_null(
        compute.less(positions, compute.take(checksum_at, parents)), True
    )
    present = frozenset(compute.unique(codes).to_pylist())
    spells_msg_type = _MSG_TYPE_CODE in present or _NAMED_MSG_TYPE_CODE in present

    found: dict[str, pyarrow.Array] = {}
    claimed = pyarrow.repeat(pyarrow.scalar(False), len(positions))
    for code, (name, _) in enumerate(SESSION_FIELDS):
        if code not in present and (name != _MSG_TYPE or not spells_msg_type):
            found[name] = pyarrow.nulls(rows, pyarrow.string())
            continue
        spelled = compute.equal(codes, code)
        if name == _MSG_TYPE:
            spelled = compute.or_(spelled, compute.equal(codes, _NAMED_MSG_TYPE_CODE))
        eligible = compute.and_(before_checksum, spelled)
        if name == _MSG_TYPE:
            # The discriminator has a rule of its own for its two spellings --
            # a `U`-prefixed wire type defers to a rendered name beside it --
            # so disagreement between them is expected rather than torn, and
            # its value is the one the standard constrains. *Within* one
            # spelling the general rule holds: `35=D` beside `35=8` is two
            # readings of one fact, so neither leaves `entries` and the column
            # falls back to the raw line's own first discriminator.
            eligible = compute.and_(
                eligible,
                compute.fill_null(compute.match_substring_regex(values, _MSG_TYPE_VALUE), False),
            )
            found[name], mask = _wire_or_named(values, parents, codes, eligible, rows)
            claimed = compute.or_(claimed, mask)
            continue
        first, mask = _agreed_by_parent(values, parents, eligible, rows)
        found[name] = first
        claimed = compute.or_(claimed, mask)
    # The lifted entries are the only ones that moved, so the list is rebuilt
    # from what each row had less what it gave up, and nothing counts the
    # entries no field ever looked at.
    keep = compute.and_(
        in_frame,
        compute.invert(
            compute.fill_null(
                compute.scatter(claimed, positions, max_index=len(entries) - 1), False
            )
        ),
    )
    residual = build_list(
        ENTRIES,
        dense_counts(compute.filter(all_parents, keep), rows),
        compute.filter(entries, keep),
        null_mask(stored),
    )
    return found, residual


def _session_codes(keys: pyarrow.Array) -> pyarrow.Array:
    """Which session field each entry spells, or a negative code for no field.

    A batch carries a few dozen distinct keys for hundreds of thousands of
    entries, so the fold and the thirty-way lookup run over the dictionary and
    one `take` puts the answer on every entry.
    """
    encoded = pyarrow.compute.dictionary_encode(keys)
    spelled = encoded.dictionary.to_pylist()
    codes = pyarrow.array(
        [_SESSION_CODES.get(column_name(key), _UNCLAIMED) for key in spelled], pyarrow.int8()
    )
    return pyarrow.compute.take(codes, encoded.indices)


def _wire_or_named(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    codes: pyarrow.Array,
    eligible: pyarrow.Array,
    rows: int,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(the discriminator, which entries it claims)`, tag deferring to a name.

    A bridge that wraps its own message in `35=U1` and then names the real
    type beside it means the name; everything else means the tag. Each
    spelling has to agree with itself first, so a row spelling `35=` twice
    with two values leaves both where a reader can see them.
    """
    compute = pyarrow.compute
    wire, wire_mask = _agreed_by_parent(
        values, parents, compute.and_(eligible, compute.equal(codes, _MSG_TYPE_CODE)), rows
    )
    named, named_mask = _agreed_by_parent(
        values, parents, compute.and_(eligible, compute.equal(codes, _NAMED_MSG_TYPE_CODE)), rows
    )
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire, "U"), False),
        compute.is_valid(named),
    )
    return (
        compute.if_else(wrapped, named, compute.coalesce(wire, named)),
        compute.or_(wire_mask, named_mask),
    )


def _agreed_by_parent(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    eligible: pyarrow.Array,
    rows: int,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(first value per row, which entries it claims)` -- nothing when they disagree.

    A row spelling one header field twice with two readings has not stated it
    once, so neither reading is lifted and both stay where a reader can see
    them.
    """
    compute = pyarrow.compute
    first = _first_by_parent(values, parents, eligible, rows)
    disagrees = compute.and_(
        eligible, compute.fill_null(compute.not_equal(values, compute.take(first, parents)), True)
    )
    torn = compute.greater(dense_counts(compute.filter(parents, disagrees), rows), 0)
    per_entry = compute.take(torn, parents)
    return (
        compute.if_else(torn, pyarrow.nulls(rows, values.type), first),
        compute.and_(eligible, compute.invert(per_entry)),
    )


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


def _before_checksum(candidate_at: pyarrow.Array, checksum_at: pyarrow.Array) -> pyarrow.Array:
    """A discriminator exists and precedes the first checksum token.

    Null-free, because this is also the mask the value is extracted under: a
    row that carries no payload found no discriminator either.
    """
    compute = pyarrow.compute
    exists = compute.greater_equal(candidate_at, 0)
    return compute.fill_null(
        compute.and_(
            exists,
            compute.or_(compute.less(checksum_at, 0), compute.less(candidate_at, checksum_at)),
        ),
        False,
    )


def _resolved_msg_types(stated: pyarrow.Array, text: pyarrow.Array) -> pyarrow.Array:
    """The lifted discriminator, probed off the raw line only where a row has none.

    The probe is three RE2 scans of whatever column it is given, and its
    answer survives nowhere a row lifted one of its own -- so it reads the
    rows that lifted nothing, and none at all when every row lifted a type.
    """
    compute = pyarrow.compute
    if not stated.null_count:
        return stated
    missing = compute.is_null(stated)
    probed = _msg_type_probe(compute.filter(text, missing))
    return compute.replace_with_mask(stated, missing, probed)


def _captured_values(text: pyarrow.Array, pattern: str, found: pyarrow.Array) -> pyarrow.Array:
    """`pattern`'s captured value where `found`, null everywhere else.

    The find already said which rows can keep an answer, and RE2 costs what it
    scans -- so the extract reads those rows and not the column. A capture
    whose lines carry no discriminator at all pays for the find alone.
    """
    compute = pyarrow.compute
    values = pyarrow.nulls(len(text), pyarrow.string())
    if not compute.any(found, min_count=0).as_py():
        return values
    captured = compute.struct_field(
        compute.extract_regex(compute.filter(text, found), pattern), "value"
    )
    return compute.replace_with_mask(values, found, captured)


def _msg_type_probe(text: pyarrow.Array) -> pyarrow.Array:
    """The first valid top-level discriminator, wire spelling or rendered.

    A `35=U1` wrapper naming its real type beside it defers to the rendered
    name; everything else takes the wire value where it has one. Both count
    only in front of the checksum, so a `35=` behind the trailer is log noise.
    """
    compute = pyarrow.compute
    checksum_at = compute.find_substring_regex(text, _CHECKSUM_TOKEN)
    values = []
    for pattern in (FIX_MSG_TYPE_PATTERN, NAMED_MSG_TYPE_PATTERN):
        found = _before_checksum(compute.find_substring_regex(text, pattern), checksum_at)
        values.append(_captured_values(text, pattern, found))
    wire, named = values
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire, "U"), False), compute.is_valid(named)
    )
    return compute.if_else(wrapped, named, compute.coalesce(wire, named))


def _event_code(value: EventType | int | str) -> int:
    """One configurable event spelling as its stable stored integer.

    A member, its name, its mnemonic, or its stored code. A spelling no
    member answers to is refused rather than written into the column as a
    dead code every reader maps to `UNKNOWN`.
    """
    if isinstance(value, EventType):
        return int(value)
    try:
        code = int(value)
    except (TypeError, ValueError):
        member = EventType(str(value))
        if member is EventType.UNKNOWN and str(value).strip().upper() != "UNKNOWN":
            raise ValueError(f"unknown EventType spelling {value!r}") from None
        return int(member)
    member = EventType.from_int(code)
    if member is EventType.UNKNOWN and code != 0:
        raise ValueError(f"no EventType stores id {code}")
    return int(member)
