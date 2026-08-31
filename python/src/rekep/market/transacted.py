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

import pyarrow
import pyarrow.compute

from rekep.enums import EventType
from rekep.fields import TimestampField
from rekep.fields.arrays import sequence
from rekep.fix.fields import NANOS, SECONDS_A_DAY, cast_arrow_fix

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
    """One rung of the precedence chain: where a time may come from, and how.

    A flat tuple of names could not say "the entry of a regulatory group whose
    type is one of these", which is what the head of the chain now is -- so the
    rung is a small record rather than a string, and every rung is one kind of
    thing.

    Both readings of a rung are here: `transacted` answers for one row a
    caller holds as a message, `arrow` for a whole batch it holds as columns.
    Two executions of one declaration, on the declaration, so neither can
    drift into being a second rule.
    """

    #: What names this rung where a reading is recorded, and in the docs.
    name: str

    #: The FIX field this rung reads, or the two a date and a time are split
    #: across. Empty for a rung that reads a structured column instead.
    fields: tuple[str, ...] = ()

    #: The structured column this rung reads, and the two members of an entry
    #: it reads: the one holding the instant and the one saying which instant
    #: it is. These are column names, so they are folded as a schema folds them.
    column: str = ""
    instant: str = ""
    kind: str = ""

    @property
    def is_column(self) -> bool:
        """Whether this rung reads a typed column rather than a pair of fields."""
        return bool(self.column)

    # -- one row --------------------------------------------------------------

    def transacted(
        self,
        read: Callable[[str], Any],
        entries: Callable[[str], Sequence[Any]],
        eventtype: EventType | int | None,
        recorded: int | None,
        member: Callable[[Any, str], Any],
    ) -> Transacted | None:
        """What this rung says one row's transaction time is, or None."""
        if self.is_column:
            found, kind = self._entry(entries(self.column), eventtype, member)
            if found is None:
                return None
            return Transacted(found, f"{self.name}={kind}" if kind is not None else self.name)
        found = read(self.fields[0]) if len(self.fields) == 1 else self._dated(read, recorded)
        return None if found is None else Transacted(found, self.name)

    def _dated(self, read: Callable[[str], Any], recorded: int | None) -> int | None:
        """A rung FIX splits across a date field and a time field.

        The two halves add: the date field is the day, the clock field is the
        time on it. A clock read on its own may already be typed on the epoch's
        day, so only its within-day part is combined with the best known day.
        """
        date, clock = self.fields
        day = read(date)
        on = read(clock, day if day is not None else recorded)  # type: ignore[call-arg]
        if on is None:
            return day
        base = day if day is not None else recorded
        return on if base is None else base - base % A_DAY + on % A_DAY

    def _entry(
        self,
        entries: Sequence[Any],
        eventtype: EventType | int | None,
        member: Callable[[Any, str], Any],
    ) -> tuple[int | None, int | None]:
        """The preferred entry of one regulatory group, and which type it was.

        Ranked, not first-wins: a group carries several instants and only one
        of them is when the thing happened. A group that carries none of the
        preferred types still answers -- with its first entry, because a
        regulatory stamp nobody ranked is still nearer the transaction than a
        transmission clock -- and says which type that was, so a reader can
        tell the two apart.
        """
        readings = [
            (self._as_kind(member(entry, self.kind)), self._as_instant(member(entry, self.instant)))
            for entry in entries or ()
        ]
        readings = [(kind, found) for kind, found in readings if found is not None]
        if not readings:
            return None, None
        for wanted in preferred_types(eventtype):
            for kind, found in readings:
                if kind == wanted:
                    return found, kind
        kind, found = readings[0]
        return found, kind

    @staticmethod
    def member(entry: Any, name: str) -> Any:
        """One member of a typed entry, spelled as its column is."""
        if isinstance(entry, Mapping):
            return entry.get(name)
        return getattr(entry, name, None)

    @staticmethod
    def _as_instant(found: Any) -> int | None:
        """One entry's instant, in the epoch nanoseconds a `*unix` column holds."""
        from rekep.market.fix import unix_value

        return unix_value(found)

    @staticmethod
    def _as_kind(found: Any) -> int | None:
        """One entry's regulatory type, where it states one."""
        if found is None:
            return None
        try:
            return int(found)
        except (TypeError, ValueError):
            return None

    # -- whole columns --------------------------------------------------------

    def arrow(
        self,
        columns: Mapping[str, Any],
        eventtypes: Any,
        rows: int,
        anchor: Any | None = None,
    ) -> tuple[Any, Any]:
        """`(instant, kind)` per row for this rung, over a batch of parsed rows.

        `kind` is None for a rung that reads fields, which have no type to
        name; both are None where the batch does not carry this rung at all.
        """
        if self.is_column:
            return self._arrow_entry(columns.get(self.column), eventtypes, rows)
        return self._arrow_fields(columns, rows, anchor), None

    def _arrow_fields(self, columns: Mapping[str, Any], rows: int, anchor: Any | None) -> Any:
        """One field rung over a whole batch, as epoch nanoseconds."""
        read = [columns.get(name) for name in self.fields]
        if any(column is None for column in read):
            return None
        if len(read) == 1:
            return self._arrow_nanos(read[0], rows)
        # A rung FIX splits in two is a day and a clock within it, so the two
        # halves add. Taking the clock alone -- which is what this did -- put
        # every `MDEntry` on 1970-01-01, because a clock read on its own is
        # anchored to the epoch's day and the date half was the day it
        # belonged on.
        date, clock = (self._arrow_nanos(column, rows) for column in read)
        compute = pyarrow.compute
        within = compute.subtract(clock, self._arrow_day_floor(clock))
        base = date if anchor is None else compute.coalesce(date, anchor)
        return compute.coalesce(compute.add(self._arrow_day_floor(base), within), clock, date)

    @staticmethod
    def _arrow_day_floor(column: Any) -> Any:
        """Start of each UTC day under Arrow's truncating integer division."""
        compute = pyarrow.compute
        adjusted = compute.if_else(
            compute.less(column, 0),
            compute.subtract(column, pyarrow.scalar(A_DAY - 1, pyarrow.int64())),
            column,
        )
        return compute.multiply(compute.divide(adjusted, A_DAY), A_DAY)

    def _arrow_entry(self, column: Any, eventtypes: Any, rows: int) -> tuple[Any, Any]:
        """The preferred entry of one regulatory group, per row, in kernels.

        Ranked exactly as `_entry` ranks: a row takes the first of its own
        kind's preferred types that its group carries, and its group's first
        entry where it carries none of them.
        """
        compute = pyarrow.compute
        if column is None or not rows:
            return None, None
        if isinstance(column, pyarrow.ChunkedArray):
            column = column.combine_chunks()
        if column.null_count == rows:
            return None, None
        parents = compute.list_parent_indices(column).cast(pyarrow.int64())
        entries = compute.list_flatten(column)
        instants = self._arrow_nanos(compute.struct_field(entries, self.instant), len(parents))
        kinds = compute.struct_field(entries, self.kind)
        told = compute.is_valid(instants)
        rank = self._arrow_rank(kinds, eventtypes, parents, rows)
        # The best-ranked entry of each row, in one stable sort: the row and
        # the rank pack into one integer -- ranks are far below `_RANK_STRIDE`
        # -- and a stable order then breaks a tie by where the entry sat, which
        # is wire order. `index_in` takes the first occurrence of each row, so
        # the entry each row keeps is its best-ranked and earliest.
        keyed = compute.add(
            compute.multiply(parents, pyarrow.scalar(_RANK_STRIDE, pyarrow.int64())),
            rank.cast(pyarrow.int64()),
        )
        order = compute.array_sort_indices(keyed)
        order = compute.filter(order, compute.fill_null(compute.take(told, order), False))
        first = compute.index_in(sequence(rows), value_set=compute.take(parents, order))
        chosen = compute.take(order, first)
        return compute.take(instants, chosen), compute.take(kinds, chosen)

    @classmethod
    def _arrow_rank(cls, kinds: Any, eventtypes: Any, parents: Any, rows: int) -> Any:
        """How good each entry is for the row it belongs to: lower is better.

        One pass per distinct `EventType` in the batch, because the ranking is
        a property of the kind of row and a batch carries a handful of kinds.
        An entry of a type nobody ranked sorts after every ranked one, so it is
        taken only where a row has nothing better -- which is `_entry`'s rule.
        """
        compute = pyarrow.compute
        unranked = pyarrow.scalar(len(PREFERRED) + 64, pyarrow.int32())
        if eventtypes is None:
            return cls._arrow_rank_of(kinds, preferred_types(None), unranked)
        codes = compute.take(
            compute.fill_null(eventtypes.cast(pyarrow.int64(), safe=False), 0), parents
        )
        rank = pyarrow.repeat(unranked, len(parents))
        for code in compute.unique(codes).to_pylist():
            at = compute.equal(codes, code)
            rank = compute.if_else(
                at, cls._arrow_rank_of(kinds, preferred_types(code), unranked), rank
            )
        return rank

    @staticmethod
    def _arrow_rank_of(kinds: Any, wanted: Sequence[int], unranked: Any) -> Any:
        """One ranking applied to a whole child array."""
        compute = pyarrow.compute
        rank = pyarrow.repeat(unranked, len(kinds))
        for position, code in reversed(list(enumerate(wanted))):
            at = compute.fill_null(compute.equal(kinds, code), False)
            rank = compute.if_else(at, pyarrow.scalar(position, pyarrow.int32()), rank)
        return rank

    @staticmethod
    def _arrow_nanos(column: Any, rows: int) -> Any:
        """One clock column as the epoch nanoseconds a `*unix` column holds."""
        if column is None:
            return pyarrow.nulls(rows, pyarrow.int64())
        if isinstance(column, pyarrow.ChunkedArray):
            column = column.combine_chunks()
        if not pyarrow.types.is_timestamp(column.type):
            column = cast_arrow_fix(column, pyarrow.timestamp("us", tz="UTC"))
        micros = TimestampField.of("us").cast_arrow_array(column)
        return TimestampField.into_unix_arrow(micros)


#: One day in the nanosecond clock every rung answers in.
A_DAY = SECONDS_A_DAY * NANOS

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
#: 7. `OnBehalfOfSendingTime <370>` -- the upstream sender's transmission
#:    where a hub relayed the message. It precedes the hub's own clock.
#: 8. `SendingTime <52>` -- current transmission, and the last FIX clock there is.
#:
#: Below all of them is the recording clock the log header stamped, which is
#: not in this table because it is not something the message said: it is
#: `recunix`, and `resolve` falls back to it by name.
TRANSACTED: tuple[Stamped, ...] = (
    Stamped(
        name="TrdRegTimestamps",
        column="trdregtimestamps",
        instant="trdregtimestamp",
        kind="trdregtimestamptype",
    ),
    Stamped(
        name="SideTrdRegTS",
        column="sidetrdregts",
        instant="sidetrdregtimestamp",
        kind="sidetrdregtimestamptype",
    ),
    Stamped(name="TransactTime", fields=("transacttime",)),
    Stamped(name="MDEntry", fields=("mdentrydate", "mdentrytime")),
    Stamped(name="OrigTime", fields=("origtime",)),
    Stamped(name="OrigSendingTime", fields=("origsendingtime",)),
    Stamped(name="OnBehalfOfSendingTime", fields=("onbehalfofsendingtime",)),
    Stamped(name="SendingTime", fields=("sendingtime",)),
)

#: FIX fields that say when a lifecycle was made upstream, best first. These
#: are deliberately separate from `TRANSACTED`: transmission is useful
#: creation evidence but it does not replace a business transaction's time.
CREATED: tuple[Stamped, ...] = tuple(
    rung
    for rung in TRANSACTED
    if rung.name in {"OrigTime", "OrigSendingTime", "OnBehalfOfSendingTime", "SendingTime"}
)

#: Source recorded when the package-owned event-time field states `unix`
#: directly rather than leaving it to the standard FIX clock chain.
STATED_EVENT_TIME = "REKEP.Unix"

#: What `resolve` records when no clock the message carries answered, and the
#: log's own header time is all there is.
RECORDED = "recorded"

#: What it records when no clock answered at all -- a row with no time.
#: Spelled apart from `transcribe.NO_SOURCE`, which is "no version evidence":
#: two facts about a row, and one name for both would read as one fact.
NO_CLOCK = ""

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


def preferred_types(eventtype: EventType | int | None) -> tuple[int, ...]:
    """Which regulatory stamp types `eventtype` prefers, best first."""
    if eventtype is None:
        return _ANY
    kind = eventtype if isinstance(eventtype, EventType) else EventType.from_int(eventtype)
    found = PREFERRED.get(kind)
    if found is not None:
        return found
    return PREFERRED.get(kind.band, _ANY)


@dataclasses.dataclass(frozen=True)
class Transacted:
    """When a row happened, and which rung of the chain said so."""

    unix: int = 0
    source: str = NO_CLOCK

    def __bool__(self) -> bool:
        return bool(self.source)


def resolve(
    read: Callable[[str], Any],
    entries: Callable[[str], Sequence[Any]],
    *,
    eventtype: EventType | int | None = None,
    recorded: int | None = None,
    stated: int | None = None,
    anchor: int | None = None,
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
    if stated is not None:
        return Transacted(stated, STATED_EVENT_TIME)
    reader = member or Stamped.member
    for rung in TRANSACTED:
        day = anchor if anchor is not None else recorded
        found = rung.transacted(read, entries, eventtype, day, reader)
        if found is not None:
            return found
    if recorded:
        return Transacted(recorded, RECORDED)
    return Transacted()


def resolve_created(read: Callable[[str], Any], *, stated: int | None = None) -> int:
    """Lifecycle creation from an explicit value or FIX origination evidence."""
    if stated is not None:
        return stated
    for rung in CREATED:
        found = read(rung.fields[0])
        if found is not None:
            return found
    return 0


def resolve_recorded(local: int | None, stated: int | None = None) -> int:
    """Recording time, preferring the capture's own clock to a carried value."""
    return local or stated or 0


# -- whole columns ------------------------------------------------------------


def _residual_mdentry_columns(columns: Mapping[str, Any], rows: int) -> dict[str, Any]:
    """Top-level MDEntry clocks projected without consuming their residual entries.

    A `NoMDEntries` row is deliberately left alone: its clocks belong to each
    entry, not to the enclosing message. The scalar market reader segments that
    group and remains the authority for those per-entry instants.
    """
    entries = columns.get("entries")
    if entries is None or not rows:
        return {}
    if isinstance(entries, pyarrow.ChunkedArray):
        entries = entries.combine_chunks()
    if entries.null_count == rows:
        return {}
    compute = pyarrow.compute
    items = compute.list_flatten(entries)
    if not len(items):
        return {}
    parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
    keys = compute.struct_field(items, "key")
    values = compute.struct_field(items, "value")
    comp = compute.struct_field(items, "comp")
    grouped = compute.fill_null(compute.equal(keys, "NoMDEntries"), False)
    grouped_rows = compute.filter(parents, grouped)
    outside_counted = (
        compute.invert(compute.is_in(parents, value_set=grouped_rows))
        if len(grouped_rows)
        else pyarrow.repeat(True, len(parents))
    )
    outside = compute.and_(outside_counted, compute.is_null(comp))
    row_ids = sequence(rows)
    projected: dict[str, Any] = {}
    for column, key in (("mdentrydate", "MDEntryDate"), ("mdentrytime", "MDEntryTime")):
        wanted = compute.and_(outside, compute.fill_null(compute.equal(keys, key), False))
        matched_parents = compute.filter(parents, wanted)
        projected[column] = compute.take(
            compute.filter(values, wanted),
            compute.index_in(row_ids, value_set=matched_parents),
        )
    return projected


def resolve_arrow(
    columns: Mapping[str, Any],
    recorded: Any,
    rows: int,
    *,
    stated: Any | None = None,
) -> tuple[Any, Any]:
    """`(unix, unixsource)` for a whole batch of parsed rows.

    The columnar execution of `resolve`, over the columns a parsed row already
    carries: the rungs are walked best-first and each one fills only the rows
    still unanswered, so a batch pays one pass per rung rather than one per
    row. The rungs, their order and the type ranking are the same declarations
    the scalar reading walks -- this is the second execution of them, not a
    second table.
    """
    compute = pyarrow.compute
    residual = _residual_mdentry_columns(columns, rows)
    if residual:
        columns = {**columns, **residual}
    sending = Stamped._arrow_nanos(columns.get("sendingtime"), rows)
    anchor = compute.coalesce(sending, recorded.cast(pyarrow.int64(), safe=False))
    found = (
        pyarrow.nulls(rows, pyarrow.int64())
        if stated is None
        else stated.cast(pyarrow.int64(), safe=False)
    )
    source = compute.if_else(
        compute.is_valid(found),
        pyarrow.scalar(STATED_EVENT_TIME),
        pyarrow.scalar(None, pyarrow.string()),
    )
    eventtypes = columns.get("eventtype")
    for rung in TRANSACTED:
        if compute.all(compute.is_valid(found), min_count=0).as_py() and rows:
            break
        reading, kinds = rung.arrow(columns, eventtypes, rows, anchor)
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
        compute.fill_null(source, pyarrow.scalar(NO_CLOCK)),
    )


def resolve_created_arrow(
    columns: Mapping[str, Any], rows: int, *, stated: Any | None = None
) -> Any:
    """`resolve_created` over typed columns, preserving an explicit epoch zero."""
    compute = pyarrow.compute
    found = (
        pyarrow.nulls(rows, pyarrow.int64())
        if stated is None
        else stated.cast(pyarrow.int64(), safe=False)
    )
    for rung in CREATED:
        reading, _ = rung.arrow(columns, None, rows)
        if reading is not None:
            found = compute.coalesce(found, reading)
    return compute.fill_null(found, pyarrow.scalar(0, pyarrow.int64()))


def resolve_recorded_arrow(local: Any, stated: Any | None, rows: int) -> Any:
    """`resolve_recorded` over columns; zero is the envelope's absent sentinel."""
    compute = pyarrow.compute
    carried = (
        pyarrow.nulls(rows, pyarrow.int64())
        if stated is None
        else stated.cast(pyarrow.int64(), safe=False)
    )
    recorded = (
        pyarrow.nulls(rows, pyarrow.int64())
        if local is None
        else local.cast(pyarrow.int64(), safe=False)
    )
    local_present = compute.and_(compute.is_valid(recorded), compute.not_equal(recorded, 0))
    found = compute.if_else(local_present, recorded, carried)
    return compute.fill_null(found, pyarrow.scalar(0, pyarrow.int64()))
