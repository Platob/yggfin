"""When a transaction happened, out of everything a message says about time.

A FIX message carries several clocks and they mean different things: what a
venue's regulator recorded, what the message claims about its own business
event, and when somebody transmitted it. `unix` is meant to be the first of
those, so this resolves them in one place -- consulted both where a line is
parsed and where a message is translated, so the two cannot disagree about
when a row happened.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Any

from rekep.enums import EventType
from rekep.fix.entries import snake_of

#: `TrdRegTimestampType <770>`, and `SideTrdRegTimestampType <1013>`, which
#: FIX gives the same meanings. Named rather than spelled at the ranking
#: below, so the table reads as what it ranks.
EXECUTION_TIME = 1
TIME_IN = 2
TIME_OUT = 3
BROKER_RECEIPT = 4
BROKER_EXECUTION = 5
DESK_RECEIPT = 6
SUBMISSION_TO_CLEARING = 7

#: Two codes later FIX extension packs add, and which the packaged dictionary
#: does not enumerate: a venue that sends one is not sending a code this
#: package refuses, so they are ranked where they belong rather than left to
#: fall through as unknown.
ORDERBOOK_ENTRY_TIME = 9
ORDER_SUBMISSION_TIME = 10


@dataclasses.dataclass(frozen=True)
class Stamped:
    """One rung of the precedence chain: where a time may come from.

    A flat tuple of names could not say "the entry of a regulatory group whose
    type is one of these", which is what the head of the chain now is -- so the
    rung is a small record rather than a string, and every rung is one kind of
    thing.
    """

    #: What names this rung where a reading is recorded, and in the docs.
    name: str

    #: The FIX field this rung reads, or the two a date and a time are split
    #: across. Empty for a rung that reads a structured column instead.
    fields: tuple[str, ...] = ()

    #: The structured column this rung reads, and the two members of an entry
    #: it reads: the one holding the instant and the one saying which instant
    #: it is. Named as FIX names them -- the column's own member spelling is
    #: `snake_of` that, which is the rule the parsed columns are named by, so
    #: one declaration answers for both ways an entry may be held.
    column: str = ""
    instant: str = ""
    kind: str = ""

    @property
    def is_column(self) -> bool:
        """Whether this rung reads a typed column rather than a pair of fields."""
        return bool(self.column)


#: Where `unix` comes from, **best first**, and why each is where it is.
#:
#: 1. `TrdRegTimestamps` -- the regulatory statement of when the transaction
#:    happened, which is the strongest claim in the message: a venue records it
#:    because it is required to be able to say so afterwards. Which of its
#:    entries counts depends on what kind of event the row is, and `PREFERRED`
#:    below is that ranking.
#: 2. `SideTrdRegTS` -- the same statement made per side, for a message that
#:    reports both sides of a trade. After the unsided one because a row is one
#:    side and the first entry of a two-sided group is not necessarily this
#:    row's.
#: 3. `TransactTime <60>` -- "timestamp when the business transaction
#:    represented by the message occurred". The message's own claim, which is
#:    weaker than the regulatory record but stronger than any transmission.
#: 4. `MDEntryDate <272>` + `MDEntryTime <273>` -- a market-data entry's own
#:    instant, split across two fields because that is how FIX carries it.
#:    Read per *entry*, so two entries of one refresh keep their own times.
#: 5. `OrigTime <42>` -- "time of message origination", which for a relayed or
#:    republished message is nearer the transaction than the relay's own
#:    transmission.
#: 6. `OrigSendingTime <122>` -- on a `PossDupFlag <43>` resend, when the
#:    message *first* went out. Still transmission, but the original one.
#: 7. `SendingTime <52>` -- transmission, and the last FIX clock there is.
#:
#: Below all of them is the recording clock the log header stamped, which is
#: not in this table because it is not something the message said: it is
#: `runix`, and `resolve` falls back to it by name.
TRANSACTED: tuple[Stamped, ...] = (
    Stamped(
        name="TrdRegTimestamps",
        column="trd_reg_timestamps",
        instant="TrdRegTimestamp",
        kind="TrdRegTimestampType",
    ),
    Stamped(
        name="SideTrdRegTS",
        column="side_trd_reg_timestamps",
        instant="SideTrdRegTimestamp",
        kind="SideTrdRegTimestampType",
    ),
    Stamped(name="TransactTime", fields=("TransactTime",)),
    Stamped(name="MDEntry", fields=("MDEntryDate", "MDEntryTime")),
    Stamped(name="OrigTime", fields=("OrigTime",)),
    Stamped(name="OrigSendingTime", fields=("OrigSendingTime",)),
    Stamped(name="SendingTime", fields=("SendingTime",)),
)

#: What `resolve` records when no clock the message carries answered, and the
#: log's own header time is all there is.
RECORDED = "recorded"

#: What it records when there was no time anywhere.
NO_SOURCE = ""

#: Which regulatory stamp *is* the transaction, per kind of event, best first.
#: A regulatory group carries several instants and they are not interchangeable:
#: the one that means "when this happened" depends on what the row asserts.
#:
#: - An **execution** happened when it was executed: `EXECUTION_TIME <1>`, and
#:   then `BROKER_EXECUTION <5>` for a broker's own record of the same fill.
#:   The receipts are when somebody was told, which is not when it traded.
#: - An **order** happened when it was submitted: `ORDER_SUBMISSION_TIME <10>`
#:   where a venue sends it, then `ORDERBOOK_ENTRY_TIME <9>` -- when the book
#:   accepted it -- then `TIME_IN <2>`, which is the older spelling of the same
#:   arrival. `BROKER_RECEIPT <4>` and `DESK_RECEIPT <6>` come after, being
#:   somebody else's arrival rather than the venue's.
#: - A **quote** is an order in this package's model, so it ranks alike.
#: - A **book** row is a state rather than an occurrence, and the entries that
#:   build it carry their own instants, so it prefers the book's own arrival.
#:
#: The band answers for a kind the table does not name, which is what
#: `preferred_types` walks: an `EventType` not here reads under its band, and a
#: band not here reads under `_ANY`.
PREFERRED: Mapping[EventType, tuple[int, ...]] = MappingProxyType(
    {
        EventType.EXECUTION: (EXECUTION_TIME, BROKER_EXECUTION, TIME_IN),
        EventType.ORDER: (
            ORDER_SUBMISSION_TIME,
            ORDERBOOK_ENTRY_TIME,
            TIME_IN,
            BROKER_RECEIPT,
            DESK_RECEIPT,
        ),
        EventType.QUOTE: (ORDER_SUBMISSION_TIME, ORDERBOOK_ENTRY_TIME, TIME_IN, BROKER_RECEIPT),
        EventType.BOOK: (ORDERBOOK_ENTRY_TIME, TIME_IN, EXECUTION_TIME),
    }
)

#: How far apart two rows' ranks sit when the two are packed into one sort
#: key. Above every rank `_arrow_rank` can assign, so a row's worst entry
#: still sorts before the next row's best.
_RANK_STRIDE = 1 << 10

#: What any other kind of row prefers: the two that mean "it happened", then
#: the arrival. A row whose group carries none of these takes its group's first
#: entry rather than nothing, which `_typed` does.
_ANY: tuple[int, ...] = (EXECUTION_TIME, ORDER_SUBMISSION_TIME, TIME_IN)


def preferred_types(etype: EventType | int | None) -> tuple[int, ...]:
    """Which regulatory stamp types `etype` prefers, best first."""
    if etype is None:
        return _ANY
    kind = etype if isinstance(etype, EventType) else EventType.from_code(etype)
    found = PREFERRED.get(kind)
    if found is not None:
        return found
    return PREFERRED.get(kind.band, _ANY)


@dataclasses.dataclass(frozen=True)
class Transacted:
    """When a row happened, and which rung of the chain said so."""

    unix: int = 0
    source: str = NO_SOURCE

    def __bool__(self) -> bool:
        return bool(self.source)


def resolve(
    read: Callable[[str], Any],
    entries: Callable[[str], Sequence[Any]],
    *,
    etype: EventType | int | None = None,
    recorded: int | None = None,
    member: Callable[[Any, str], Any] | None = None,
) -> Transacted:
    """The most coherent transaction time, and the rung that answered.

    `read` answers one FIX field as epoch nanoseconds or None; `entries`
    answers one structured column's entries; `member` reads one member out of
    one of those entries, defaulting to the typed column's own spelling. All
    three are the caller's, because the two layers hold a row differently --
    one has a parsed message and the other has typed columns -- while the
    chain they walk is the same one.
    """
    reader = member or _member
    for rung in TRANSACTED:
        if rung.is_column:
            found, kind = _typed(entries(rung.column), rung, etype, reader)
            if found is not None:
                return Transacted(found, f"{rung.name}={kind}" if kind is not None else rung.name)
            continue
        found = read(rung.fields[0]) if len(rung.fields) == 1 else _dated(read, rung, recorded)
        if found is not None:
            return Transacted(found, rung.name)
    if recorded:
        return Transacted(recorded, RECORDED)
    return Transacted()


def _dated(read: Callable[[str], Any], rung: Stamped, recorded: int | None) -> int | None:
    """A rung FIX splits across a date field and a time field."""
    date, clock = rung.fields
    found = read(date)
    on = read(clock, found if found is not None else recorded)  # type: ignore[call-arg]
    return on if on is not None else found


def _typed(
    entries: Sequence[Any],
    rung: Stamped,
    etype: EventType | int | None,
    member: Callable[[Any, str], Any],
) -> tuple[int | None, int | None]:
    """The preferred entry of one regulatory group, and which type it was.

    Ranked, not first-wins: a group carries several instants and only one of
    them is when the thing happened. A group that carries none of the preferred
    types still answers -- with its first entry, because a regulatory stamp
    nobody ranked is still nearer the transaction than a transmission clock --
    and says which type that was, so a reader can tell the two apart.
    """
    readings = [
        (_kind_of(member(entry, rung.kind)), _instant_of(member(entry, rung.instant)))
        for entry in entries or ()
    ]
    readings = [(kind, found) for kind, found in readings if found is not None]
    if not readings:
        return None, None
    for wanted in preferred_types(etype):
        for kind, found in readings:
            if kind == wanted:
                return found, kind
    kind, found = readings[0]
    return found, kind


def _instant_of(found: Any) -> int | None:
    """One entry's instant, in the epoch nanoseconds a `*unix` column holds."""
    from rekep.market.fix import unix_value

    return unix_value(found)


def _kind_of(found: Any) -> int | None:
    """One entry's regulatory type, where it states one."""
    if found is None:
        return None
    try:
        return int(found)
    except (TypeError, ValueError):
        return None


def _member(entry: Any, name: str) -> Any:
    """One member of a typed entry: the column spelling of its FIX name.

    A parsed row holds these as a typed column whose members are `snake_of`
    the FIX name the rung declares. A caller holding the group some other way
    -- a translation holds whatever the wire keyed it by -- passes its own
    reader, which is what `member` is for.
    """
    spelled = snake_of(name)
    if isinstance(entry, Mapping):
        return entry.get(spelled)
    return getattr(entry, spelled, None)


# -- whole columns ------------------------------------------------------------


def resolve_arrow(columns: Mapping[str, Any], recorded: Any, rows: int) -> tuple[Any, Any]:
    """`(unix, unix_source)` for a whole batch of parsed rows.

    The columnar execution of `resolve`, over the columns a parsed row already
    carries: the rungs are walked best-first and each one fills only the rows
    still unanswered, so a batch pays one pass per rung rather than one per
    row. The rungs, their order and the type ranking are the same declarations
    the scalar reading walks -- this is the second execution of them, not a
    second table.
    """
    import pyarrow
    import pyarrow.compute

    compute = pyarrow.compute
    found = pyarrow.nulls(rows, pyarrow.int64())
    source = pyarrow.nulls(rows, pyarrow.string())
    etypes = columns.get("etype")
    for rung in TRANSACTED:
        if compute.all(compute.is_valid(found), min_count=0).as_py() and rows:
            break
        if rung.is_column:
            reading, kinds = _arrow_typed(columns.get(rung.column), rung, etypes, rows)
        else:
            reading, kinds = _arrow_fields(columns, rung, rows), None
        if reading is None:
            continue
        fill = compute.and_(compute.is_null(found), compute.is_valid(reading))
        if not compute.any(fill, min_count=0).as_py():
            continue
        named: Any = pyarrow.scalar(rung.name)
        if kinds is not None:
            named = compute.binary_join_element_wise(
                pyarrow.repeat(pyarrow.scalar(rung.name), rows),
                compute.fill_null(kinds.cast(pyarrow.string()), ""),
                "=",
            )
        found = compute.if_else(fill, reading, found)
        source = compute.if_else(fill, named, source)
    on_record = compute.and_(compute.is_null(found), compute.not_equal(recorded, 0))
    found = compute.if_else(on_record, recorded, found)
    source = compute.if_else(on_record, pyarrow.scalar(RECORDED), source)
    return (
        compute.fill_null(found, pyarrow.scalar(0, pyarrow.int64())),
        compute.fill_null(source, pyarrow.scalar(NO_SOURCE)),
    )


def _arrow_fields(columns: Mapping[str, Any], rung: Stamped, rows: int) -> Any:
    """One field rung over a whole batch, as epoch nanoseconds."""
    import pyarrow

    from rekep.fix.entries import snake_of

    read = [columns.get(snake_of(name)) for name in rung.fields]
    if any(column is None for column in read):
        return None
    if len(read) == 1:
        return _arrow_nanos(read[0], rows)
    date, clock = (_arrow_nanos(column, rows) for column in read)
    return pyarrow.compute.coalesce(clock, date)


def _arrow_typed(column: Any, rung: Stamped, etypes: Any, rows: int) -> tuple[Any, Any]:
    """The preferred entry of one regulatory group, per row, in kernels.

    Ranked exactly as `_typed` ranks: a row takes the first of its own kind's
    preferred types that its group carries, and its group's first entry where
    it carries none of them.
    """
    import pyarrow
    import pyarrow.compute

    from rekep.fields.arrays import sequence
    from rekep.fix.entries import snake_of

    compute = pyarrow.compute
    if column is None or not rows:
        return None, None
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if column.null_count == rows:
        return None, None
    parents = compute.list_parent_indices(column).cast(pyarrow.int64())
    entries = compute.list_flatten(column)
    instants = _arrow_nanos(compute.struct_field(entries, snake_of(rung.instant)), len(parents))
    kinds = compute.struct_field(entries, snake_of(rung.kind))
    told = compute.is_valid(instants)
    rank = _arrow_rank(kinds, etypes, parents, rows)
    # The best-ranked entry of each row, in one stable sort: the row and the
    # rank pack into one integer -- ranks are far below `_RANK_STRIDE` -- and a
    # stable order then breaks a tie by where the entry sat, which is wire
    # order. `index_in` takes the first occurrence of each row, so the entry
    # each row keeps is its best-ranked and earliest.
    keyed = compute.add(
        compute.multiply(parents, pyarrow.scalar(_RANK_STRIDE, pyarrow.int64())),
        rank.cast(pyarrow.int64()),
    )
    order = compute.array_sort_indices(keyed)
    order = compute.filter(order, compute.fill_null(compute.take(told, order), False))
    first = compute.index_in(sequence(rows), value_set=compute.take(parents, order))
    chosen = compute.take(order, first)
    return compute.take(instants, chosen), compute.take(kinds, chosen)


def _arrow_rank(kinds: Any, etypes: Any, parents: Any, rows: int) -> Any:
    """How good each entry is for the row it belongs to: lower is better.

    One pass per distinct `EventType` in the batch, because the ranking is a
    property of the kind of row and a batch carries a handful of kinds. An
    entry of a type nobody ranked sorts after every ranked one, so it is taken
    only where a row has nothing better -- which is `_typed`'s rule.
    """
    import pyarrow
    import pyarrow.compute

    compute = pyarrow.compute
    unranked = pyarrow.scalar(len(PREFERRED) + 64, pyarrow.int32())
    if etypes is None:
        return _arrow_rank_of(kinds, preferred_types(None), unranked)
    codes = compute.take(compute.fill_null(etypes.cast(pyarrow.int32(), safe=False), 0), parents)
    rank = pyarrow.repeat(unranked, len(parents))
    for code in compute.unique(codes).to_pylist():
        wanted = preferred_types(code)
        at = compute.equal(codes, code)
        rank = compute.if_else(at, _arrow_rank_of(kinds, wanted, unranked), rank)
    return rank


def _arrow_rank_of(kinds: Any, wanted: Sequence[int], unranked: Any) -> Any:
    """One ranking applied to a whole child array."""
    import pyarrow
    import pyarrow.compute

    compute = pyarrow.compute
    rank = pyarrow.repeat(unranked, len(kinds))
    for position, code in reversed(list(enumerate(wanted))):
        at = compute.fill_null(compute.equal(kinds, code), False)
        rank = compute.if_else(at, pyarrow.scalar(position, pyarrow.int32()), rank)
    return rank


def _arrow_nanos(column: Any, rows: int) -> Any:
    """One clock column as the epoch nanoseconds a `*unix` column holds."""
    import pyarrow
    import pyarrow.compute

    from rekep.fix.fields import cast_arrow_fix

    compute = pyarrow.compute
    if column is None:
        return pyarrow.nulls(rows, pyarrow.int64())
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if not pyarrow.types.is_timestamp(column.type):
        column = cast_arrow_fix(column, pyarrow.timestamp("us", tz="UTC"))
    micros = column.cast(pyarrow.timestamp("us"), safe=False).cast(pyarrow.int64())
    return compute.multiply(micros, pyarrow.scalar(1000, pyarrow.int64()))
