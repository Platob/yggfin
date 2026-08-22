"""FIX messages as market events: what a venue said, as rows this package stores.

Two things live here. `market_tags` is the name-to-tag mapping the shapes
themselves publish -- every `fix_tag(...)` declaration on `MarketEvent`,
`Order`, `Execution` and `Instrument`, plus the header and market-data tags the
translation needs -- so a caller resolving rendered keys needs no dictionary
scrape and no network. `FixEvents` is the translation: a message in, market
events out.

**Which timestamp is the event's own** is the decision this module exists to
make, and FIX answers it directly. `TransactTime <60>` is defined as
"timestamp when the business transaction represented by the message occurred",
which is exactly `MarketEvent.unix`; `SendingTime <52>` is "time of message
transmission", which is `Event.runix` and is not the same instant. Reading them
as interchangeable is how a latency measurement comes out as zero and how two
venues' events interleave wrongly. The full order is in `TRANSACTED`.
"""

from __future__ import annotations

import dataclasses
import datetime
import functools
import re
import types
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, ClassVar

from rekep.convert import Convertible
from rekep.fields import StructField
from rekep.fix.message import FixMessage
from rekep.market.enums import (
    AssetKind,
    ExecKind,
    IdSource,
    OptionKind,
    OrderKind,
    Side,
    State,
    TimeInForce,
    UpdateAction,
)
from rekep.market.event import MarketEvent
from rekep.market.instrument import Instrument, Leg
from rekep.market.orders import Execution, Order

#: Tags the translation reads that no market column declares -- the header,
#: the market-data group, and the few request-side fields that decide what a
#: message *is*. Named, because a bare number in a rule is the mistake
#: `tests/market/test_fix.py` exists to catch.
CARRIED_TAGS: dict[str, int] = {
    "MsgType": 35,
    "SenderCompID": 49,
    "TargetCompID": 56,
    "SendingTime": 52,
    "OrigSendingTime": 122,
    "OrigTime": 42,
    "PossDupFlag": 43,
    "TransactTime": 60,
    "TradeDate": 75,
    "ExpireTime": 126,
    "ExpireDate": 432,
    "OrdStatus": 39,
    "ExecTransType": 20,
    "CxlRejReason": 102,
    "NoMDEntries": 268,
    "MDEntryType": 269,
    "MDEntryPx": 270,
    "MDEntrySize": 271,
    "MDEntryDate": 272,
    "MDEntryTime": 273,
    "MDEntryID": 278,
    "MDUpdateAction": 279,
    "NumberOfOrders": 346,
    "TrdMatchID": 880,
    "SecurityExchange": 207,
    "ExDestination": 100,
    # The four abstract slots' tags across every subclass. Each shape declares
    # the one *it* holds, so a tag another shape holds is unclaimed here and
    # would otherwise be repeated into `metadata` -- a fill's `LastPx <31>`
    # stored both as `px` and as an extra.
    "Price": 44,
    "OrderQty": 38,
    "LastPx": 31,
    "LastQty": 32,
    # The repeating groups an instrument is read out of. A group's own
    # `NumInGroup` count and the members that land somewhere other than a
    # column of their own -- an alternative identifier becomes a key of
    # `alt_ids`, so its two tags are read and stored and are not extras.
    "NoSecurityAltID": 454,
    "SecurityAltID": 455,
    "SecurityAltIDSource": 456,
    "NoLegs": 555,
    # The older way to say when a contract expires, which `maturity` falls back
    # to. A venue that sends it usually sends no `MaturityDate <541>` at all.
    "MaturityMonthYear": 200,
    "LegMaturityMonthYear": 610,
}

#: What frames the message rather than describing the event. `BodyLength <9>`
#: is a byte count and `CheckSum <10>` is a checksum of those bytes: both are
#: properties of the encoding, they are recomputed by anything that re-emits
#: the message, and a row carrying them says nothing about the market.
#: `BeginString <8>` is *not* here, because which FIX version a venue speaks
#: is a real fact about what arrived.
FRAMING = frozenset({"9", "10"})

#: Where `MarketEvent.unix` comes from, **best first**, and why each is where
#: it is. Every one is a real FIX field, and the order is the standard's own
#: definitions rather than a preference:
#:
#: 1. `TransactTime <60>` -- "timestamp when the business transaction
#:    represented by the message occurred". The thing being asked for.
#: 2. `MDEntryDate <272>` + `MDEntryTime <273>` -- a market-data entry's own
#:    instant, split across two fields because that is how FIX carries it.
#:    Read per *entry*, so two entries of one refresh keep their own times.
#: 3. `OrigTime <42>` -- "time of message origination", which for a relayed
#:    or republished message is nearer the transaction than the relay's own
#:    transmission.
#: 4. `OrigSendingTime <122>` -- on a `PossDupFlag <43>` resend, when the
#:    message *first* went out. Still transmission, but the original one.
#: 5. `SendingTime <52>` -- transmission. Last, and only because a row with
#:    no time at all sorts nowhere: it is the recording clock, and it is what
#:    `runix` holds regardless.
TRANSACTED: tuple[Any, ...] = (60, (272, 273), 42, 122, 52)

#: MsgType <35> values that carry an order, and the state each *asserts* when
#: the message itself does not say. A request is not an acknowledgement: a
#: NewOrderSingle is what a participant asked for, and the venue has not
#: agreed to anything yet, so it is `PENDING_NEW` rather than `NEW`.
ORDERED: dict[str, State] = {
    "D": State.PENDING_NEW,
    "F": State.PENDING_CANCEL,
    "G": State.PENDING_REPLACE,
    "9": State.UNKNOWN,
}

#: MsgType <35> values that carry a book or a trade, entry by entry.
ENTRIED = frozenset({"W", "X"})

#: MDEntryType <269> to the side of the book an entry belongs to. Everything
#: else it enumerates -- an index value, a settlement price, a session high,
#: an imbalance -- is a statistic about the market rather than an order in it,
#: and is not a market event this package stores.
ENTRY_SIDES: dict[str, Side] = {"0": Side.BID, "1": Side.ASK}

#: Every tag `FixEvents.instrument` reads, the two repeating groups included.
#: An entry of a refresh that names none of them is not describing another
#: instrument -- it is describing a level of the one the header named -- so it
#: takes the header's instrument whole rather than building a poorer copy.
INSTRUMENT_TAGS: frozenset[str] = frozenset(
    {
        "15",  # Currency
        "22",  # SecurityIDSource
        "48",  # SecurityID
        "55",  # Symbol
        "100",  # ExDestination
        "107",  # SecurityDesc
        "167",  # SecurityType
        "200",  # MaturityMonthYear
        "201",  # PutOrCall
        "202",  # StrikePrice
        "207",  # SecurityExchange
        "231",  # ContractMultiplier
        "454",  # NoSecurityAltID
        "461",  # CFICode
        "541",  # MaturityDate
        "555",  # NoLegs
        "561",  # RoundLot
        "969",  # MinPriceIncrement
    }
)

#: The MDEntryType <269> that is a trade rather than a resting interest.
ENTRY_TRADE = "2"

#: An execution's own state, where it is not the state its FIX code already
#: names. `ExecType <150>` and `OrdStatus <39>` share their lifecycle codes,
#: so `State.from_fix` reads `4`, `5`, `8`, `C`, `9`, `7`, `0`, `A`, `6`, `E`,
#: `3` and `B` directly; only the four that are about a *trade* rather than
#: about the order need saying. A fill is `FILLED` the instant it exists --
#: that is the execution's own life, not the order's, which is on the `Order`
#: row this same report produces.
EXECUTED: dict[ExecKind, State] = {
    ExecKind.TRADED: State.FILLED,
    ExecKind.PARTIAL_FILL: State.FILLED,
    ExecKind.FILL: State.FILLED,
    ExecKind.TRADE_CORRECT: State.REPLACED,
    ExecKind.TRADE_CANCEL: State.CANCELLED,
}

#: `MDUpdateAction <279>` to what it does to the order it names. A delete is
#: the end of that resting interest, which is `CANCELLED` -- the same state a
#: cancelled order reaches, because it is the same thing happening.
ENTRY_STATES: dict[UpdateAction, State] = {
    UpdateAction.NEW: State.NEW,
    UpdateAction.CHANGE: State.OPEN,
    UpdateAction.OVERLAY: State.OPEN,
    UpdateAction.DELETE: State.CANCELLED,
    UpdateAction.DELETE_THRU: State.CANCELLED,
    UpdateAction.DELETE_FROM: State.CANCELLED,
}

#: A FIX timestamp, date or time-of-day, in one pattern. The standard fixes
#: `UTCTimestamp` as `YYYYMMDD-HH:MM:SS[.sss...]`, `UTCDateOnly` as
#: `YYYYMMDD` and `UTCTimeOnly` as `HH:MM:SS[.sss...]`; the separator is a
#: `-`, and `T` and a space are admitted because logs rewrite it. Both halves
#: are optional so one pattern reads all three, and a trailing `Z` is tolerated
#: for the feeds that add one.
_STAMP = re.compile(
    r"^[ \t]*(?:(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}))?"
    r"(?:[-T ]?(?P<hour>\d{2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?"
    r"(?:\.(?P<fraction>\d{1,9}))?)?"
    r"[ \t]*Z?[ \t]*$",
    re.ASCII,
)

#: `datetime.date(1970, 1, 1).toordinal()`. The proleptic Gregorian day count
#: Python counts from is not the epoch, and the difference is a constant.
_EPOCH_ORDINAL = 719163

NANOS = 1_000_000_000
SECONDS_A_DAY = 86_400


def market_tags() -> Mapping[str, int]:
    """Every FIX field name the market shapes declare, to its tag.

    Built from the declarations rather than typed out again, which is the
    whole reason it cannot drift: a column that gains a `fix_tag` is in here,
    and one that loses it is not. `CARRIED_TAGS` adds what the translation
    reads but stores nowhere -- a header field, a market-data entry -- and
    those are the only hand-written numbers in this module.

    Hand it to `FixMessage.from_pairs` to resolve rendered keys offline, with
    no dictionary scrape: this *is* the dictionary, for the fields that
    matter here. A caller wanting all seven thousand asks `FixRegistry`.

    Built once and returned as a **read-only view of the same object** every
    time. Both halves matter: the declarations do not change while a process
    runs, so rebuilding it per message was pure waste; and one stable object
    is what lets `from_pairs` fold it once for a whole stream instead of once
    per message. Read-only because a shared mutable dictionary that a caller
    can edit is a bug waiting for the second caller.
    """
    global _TAGS
    if _TAGS is None:
        found = dict(CARRIED_TAGS)
        for shape in (MarketEvent, Order, Execution, Instrument):
            _declared_tags(shape.FIELD, found)
        _TAGS = types.MappingProxyType(found)
    return _TAGS


_TAGS: Mapping[str, int] | None = None


def _declared_tags(struct: StructField, into: dict[str, int]) -> None:
    """Every `fix:` tag under `struct`, nested members included."""
    for member in struct.fields:
        tag = member.fix.get("tag")
        name = member.fix.get("name")
        if tag and name:
            into.setdefault(str(name), int(tag))
        if member.fields:
            _declared_tags(member, into)


@functools.lru_cache(maxsize=8192)
def unix_of(text: str | None, day: int | None = None) -> int | None:
    """A FIX timestamp, date or time-of-day as nanoseconds since the epoch, UTC.

    Cached because a message asks the same question several times over: the
    order `TRANSACTED` probes in runs per event, `SendingTime <52>` is one
    string for every entry of a refresh, and a whole capture spells the same
    date all day. Pure -- text in, nanoseconds out -- so the cache cannot be
    stale, and bounded so a capture of a million distinct instants does not
    keep them all.

    One reading for all three of the standard's spellings, because a caller
    asking "when" should not have to know which of them a field is declared
    as -- and real feeds disagree with their own dictionary about that more
    often than is comfortable.

    `day` is the nanosecond instant a *time-only* value belongs to, which is
    the one thing a `UTCTimeOnly` does not carry: `MDEntryTime <273>` without
    its `MDEntryDate <272>` is a time of day, and the message's own date is
    what places it. Without `day` a time-only value reads as that time on the
    epoch's own day, which is honest -- it is what the value says -- and
    visibly wrong rather than quietly plausible.

    None for anything that is not a timestamp, including an empty string and
    a date that does not exist: a `0` there would be the epoch, and the epoch
    is a real instant that a sort would put first.
    """
    if not text:
        return None
    match = _STAMP.match(text)
    if match is None:
        return None
    year, month, dayof, hour, minute, second, fraction = match.group(
        "year", "month", "day", "hour", "minute", "second", "fraction"
    )
    if year is None and hour is None:
        return None
    if year is None:
        base = day - day % (SECONDS_A_DAY * NANOS) if day is not None else 0
    else:
        try:
            ordinal = datetime.date(int(year), int(month), int(dayof)).toordinal()
        except ValueError:
            return None
        base = (ordinal - _EPOCH_ORDINAL) * SECONDS_A_DAY * NANOS
    if hour is None:
        return base
    hours, minutes, secs = int(hour), int(minute), int(second) if second else 0
    # Range-checked, because `\d{2}` is not: `99:99:99` parsed as a shape and came
    # out as four days past midnight, which is a plausible-looking instant and a
    # wrong one. `60` seconds is deliberately allowed -- the standard admits it
    # for a leap second -- and nothing else past the clock is.
    if hours > 23 or minutes > 59 or secs > 60:
        return None
    seconds = hours * 3600 + minutes * 60 + secs
    # A fraction is a decimal fraction of a second, so its scale is its own
    # width: `.5` is half a second and `.000000001` is one nanosecond. Padding
    # to nine and reading it as an integer is that, without a float.
    nanos = int(fraction.ljust(9, "0")) if fraction else 0
    return base + seconds * NANOS + nanos


@dataclasses.dataclass
class FixEvents(Convertible):
    """The market events one FIX message carries, in the order it carries them.

    A view over a message rather than a converter object: build it from
    whatever you have -- a wire line, a list of pairs, a parsed `FixMessage` --
    and iterate it for the `Order`s and `Execution`s inside. One message is
    often several events: an `ExecutionReport <8>` that filled says both what
    the order is now *and* what traded, and a `MarketDataIncrementalRefresh
    <X>` says one thing per entry.

    `runix` and `venue` are what the *reader* knows and the message does not:
    when the line was recorded, and which feed it came off. Both are carried
    onto every event produced, because a row that cannot say where it came
    from cannot be reconciled against the venue that sent it.
    """

    REDIRECTS: ClassVar[dict[Any, str]] = {**Convertible.REDIRECTS, str: "text"}

    message: FixMessage = dataclasses.field(default_factory=FixMessage)
    """The message being read."""

    venue: str | None = None
    """Which feed this came off, when the reader knows and the message does not."""

    runix: int = 0
    """When the line was recorded, which is the reader's clock and not the venue's."""

    # -- building -----------------------------------------------------------

    @classmethod
    def from_text(cls, text: str | bytes, **carried: Any) -> FixEvents:
        """Events out of one log line, however it spells its separator."""
        return cls(message=FixMessage.from_text(text), **carried)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
        **carried: Any,
    ) -> FixEvents:
        """Events out of `(key, value)` pairs, keys named or numbered.

        `names` defaults to `market_tags()` -- the tags these shapes declare --
        so `[("Side", "1"), ("Price", 100.5)]` resolves with nothing loaded and
        nothing fetched. A key that names no field is kept as it was given,
        which is what makes a venue's own extensions survive the trip into
        `metadata`.
        """
        resolved = market_tags() if names is None else names
        return cls(message=FixMessage.from_pairs(pairs, resolved), **carried)

    # -- reading ------------------------------------------------------------

    @functools.cached_property
    def by_tag(self) -> dict[str, str]:
        """The message as one value per key, first occurrence winning.

        `FixMessage.get` is the right shape for a *message*: a scan for the
        exact key, then a regex scan for the rendered spellings of it, which
        costs nothing to build and is fine read once or twice. The translation
        reads nearly forty fields off one message, so it paid that forty times
        -- 434 regex matches per message, and 45% of the conversion
        (`benchmarks/bench_market.py`). One pass and forty probes instead.

        Built through `from_pairs`, so a message whose keys are rendered names
        -- a log that printed `Side=1` rather than `54=1` -- resolves here and
        every tag lookup below finds it. First occurrence wins because that is
        what `get` does, and a repeating tag's other values are still on
        `message.pairs` where `values` reads them.
        """
        pairs = self.message.pairs
        if any(not (key.isascii() and key.isdigit()) for key, _ in pairs):
            # Only a message that actually spells a key as a name pays for the
            # resolution. A wire message is already all tags, which is most of
            # a feed, and running the pass over it re-resolved eighteen keys
            # to themselves -- 29% of the conversion, for nothing.
            pairs = FixMessage.from_pairs(pairs, market_tags()).pairs
        found: dict[str, str] = {}
        for key, value in pairs:
            found.setdefault(key, value)
        return found

    def get(self, tag: int | str) -> str | None:
        """The message's value for one tag, or None."""
        return self.by_tag.get(str(tag))

    def __iter__(self) -> Iterator[MarketEvent]:
        """Every market event the message carries, in the order it carries them.

        Dispatch is on `MsgType <35>`, which is the standard's own answer to
        "what is this". A message with no MsgType -- a fragment, a list of
        pairs a test built -- is read from the fields it actually has, because
        a decoder that only works on complete headers is no use on a log.

        A message that carries nothing this package stores yields nothing:
        a heartbeat, a logon, a market-data entry that is a settlement price.
        That is an empty iterator and not an error, because a feed is mostly
        made of them.
        """
        kind = self.message.msg_type or self._inferred()
        if kind in ENTRIED:
            yield from self._entries(kind)
        elif kind == "8":
            yield from self._reported()
        elif kind == "AE":
            yield self.into_execution()
        elif kind in ORDERED:
            yield self.into_order(ORDERED[kind])

    def _inferred(self) -> str:
        """What a message with no MsgType <35> is, from the fields it carries.

        The fields are the evidence, most specific first: a market-data entry
        type means a refresh, an `ExecType <150>` or an `ExecID <17>` means an
        execution report, and an order's own identifiers mean an order. An
        empty string for anything else, which dispatches to nothing.
        """
        get = self.get
        if get(269) is not None:
            return "X"
        if get(150) is not None or get(17) is not None:
            return "8"
        if get(11) is not None or get(40) is not None or get(39) is not None:
            return "D"
        return ""

    def _reported(self) -> Iterator[MarketEvent]:
        """An ExecutionReport <8>: the order's new state, and the fill if there was one.

        Both, and in that order. FIX uses one message for "your order is now
        partially filled" and "here is the fill that did it", and they are two
        rows here: the `Order` carries `OrderQty <38>` and `Price <44>` -- what
        was asked for -- while the `Execution` carries `LastQty <32>` and
        `LastPx <31>` -- what moved. Storing only one of them loses the other,
        and storing them in one row makes `sum(qty)` mean two things.

        The order comes first because the execution points at it: `from_events`
        downstream folds them in the order yielded, and a fill whose order it
        has not seen has nothing to attach to.
        """
        order = self.into_order(State.from_fix(self.get(39), State.UNKNOWN))
        yield order
        kind = ExecKind.from_fix(self.get(150), ExecKind.UNKNOWN)
        if kind.band == ExecKind.TRADE or kind in (ExecKind.TRADE_CORRECT, ExecKind.TRADE_CANCEL):
            # Completed *from the order*, not from a previous report: the
            # running totals a venue leaves out of a fill -- how much is done
            # now, how much is left, what the average is -- are all statements
            # about the order this report is on, and the order row is the one
            # thing here that already holds them.
            yield self.into_execution(order).with_previous(order)

    def _entries(self, kind: str) -> Iterator[MarketEvent]:
        """One market-data refresh, entry by entry.

        A snapshot (`W`) and an incremental (`X`) differ in exactly one way
        here: an incremental entry says what it does to the book through
        `MDUpdateAction <279>`, and a snapshot entry does not because every
        entry in one is present by definition. So a snapshot entry with no
        action reads as `NEW`, which is what a full refresh means.

        Each entry becomes a message of its own -- the entry's fields in front
        of the parent's -- so `MDEntryPx <270>` is read per entry while
        `Symbol <55>` and `SendingTime <52>` still resolve from the header.
        """
        for entry in self.message.group(268) or self.message.indexed_group("NoMDEntries"):
            inside = FixEvents(message=FixMessage(pairs=entry), venue=self.venue, runix=self.runix)
            # The entry's own fields in front of the header's, so `MDEntryPx
            # <270>` is read per entry while `Symbol <55>` and `SendingTime
            # <52>` still resolve. Handed over already built rather than
            # rebuilt per entry: resolving the whole header once per entry is
            # the same work N times, and a five-entry refresh is the common
            # case.
            own = inside.by_tag
            if INSTRUMENT_TAGS.isdisjoint(own):
                # The entry says nothing about *what* is trading, only about a
                # level of it, so it is the message's instrument -- alternative
                # identifiers and legs included, which are read off the pairs
                # rather than off `by_tag` and so were lost here before.
                inside.__dict__["instrument"] = self.instrument
            inside.__dict__["by_tag"] = {**self.by_tag, **own}
            entry_type = inside.get(269)
            if entry_type in ENTRY_SIDES:
                yield inside.into_entry_order(ENTRY_SIDES[entry_type], snapshot=kind == "W")
            elif entry_type == ENTRY_TRADE:
                yield inside.into_entry_execution()

    # -- converting ---------------------------------------------------------

    def into_order(self, state: State = State.UNKNOWN) -> Order:
        """The order this message is about, in the state the message puts it in."""
        get = self.get
        unix = self.unix
        return Order(
            unix=unix,
            cunix=unix,
            runix=self.runix or unix,
            eunix=self._expires(),
            state=state,
            side=Side.from_fix(get(54), Side.UNKNOWN),
            px=_number(get(44)),
            qty=_number(get(38)),
            kind=OrderKind.from_fix(get(40), OrderKind.UNKNOWN),
            tif=TimeInForce.from_fix(get(59), TimeInForce.UNKNOWN),
            stop_px=_number(get(99)),
            display_qty=_number(get(111)),
            filled_qty=_number(get(14)),
            leaves_qty=_number(get(151)),
            avg_px=_number(get(6)),
            order_id=get(37),
            client_order_id=get(11),
            prev_client_order_id=get(41),
            reason_code=_integer(get(103) or get(102)),
            reason=get(58),
            **self._shared(),
        ).with_previous(None)

    def into_execution(self, order: Order | None = None) -> Execution:
        """What traded, as the report says it. `px` is `LastPx <31>`, not `Price <44>`."""
        get = self.get
        unix = self.unix
        kind = ExecKind.from_fix(get(150), ExecKind.UNKNOWN)
        return Execution(
            unix=unix,
            cunix=unix,
            runix=self.runix or unix,
            state=EXECUTED.get(kind) or State.from_fix(get(150), State.UNKNOWN),
            side=Side.from_fix(get(54), Side.UNKNOWN),
            px=_number(get(31)),
            qty=_number(get(32)),
            kind=kind,
            exec_id=get(17),
            trade_id=get(1003) or get(880),
            order_xhash=order.xhash if order is not None else None,
            parent_hash=[order.hash] if order is not None and order.hash else [],
            order_id=get(37),
            client_order_id=get(11),
            filled_qty=_number(get(14)),
            leaves_qty=_number(get(151)),
            avg_px=_number(get(6)),
            aggressor=_flag(get(1057)),
            reason_code=_integer(get(378)),
            reason=get(58),
            **self._shared(),
        ).with_previous(None)

    def into_entry_order(self, side: Side, snapshot: bool = False) -> Order:
        """One market-data entry as the resting interest it describes.

        A price level with a size *is* an order, aggregated or not, and reading
        it as one is what lets a book be folded from a feed and from an order
        stream by the same code. `MDEntryID <278>` is the venue's own handle on
        that interest, so it is the lifecycle identity when there is one.
        """
        get = self.get
        unix = self.unix
        action = UpdateAction.from_fix(
            get(279), UpdateAction.NEW if snapshot else UpdateAction.UNKNOWN
        )
        return Order(
            unix=unix,
            cunix=unix,
            runix=self.runix or unix,
            state=ENTRY_STATES.get(action, State.OPEN),
            side=side,
            px=_number(get(270)),
            qty=_number(get(271)),
            kind=OrderKind.LIMIT_ORDER,
            # An entry with no id of its own is a *level*, not an order, so
            # the price is what persists across its updates: that is what
            # `MDUpdateAction <279>` addresses when it says Change or Delete,
            # and it is what makes a level's own lifecycle findable.
            order_id=get(278) or (f"{side.name}@{get(270)}" if get(270) else None),
            **self._shared(),
        ).with_previous(None)

    def into_entry_execution(self) -> Execution:
        """One market-data entry of type Trade <2> as the execution it reports."""
        get = self.get
        unix = self.unix
        return Execution(
            unix=unix,
            cunix=unix,
            runix=self.runix or unix,
            state=State.FILLED,
            side=Side.from_fix(get(54), Side.UNKNOWN),
            px=_number(get(270)),
            qty=_number(get(271)),
            kind=ExecKind.TRADED,
            exec_id=get(278),
            trade_id=get(1003) or get(880),
            **self._shared(),
        ).with_previous(None)

    def into_instrument(self) -> Instrument:
        """What the message says is being traded, groups and all."""
        return self.instrument

    @functools.cached_property
    def instrument(self) -> Instrument:
        """What the message says is being traded, groups and all.

        `kind` is read twice over, best first: the first character of `CFICode
        <461>` is ISO 10962's own category letter and `AssetKind` is coded on
        it, so an instrument carrying a CFI classifies itself exactly.
        `SecurityType <167>` is the fallback, because a venue that sends no CFI
        very often sends `CS`, `FUT` or `OPT` instead -- and an instrument that
        carries neither is `UNKNOWN` rather than guessed from the shape of its
        symbol.

        Cached like `by_tag`, and for the same reason: a message is *one*
        instrument, and an `ExecutionReport` yields two events off it while a
        refresh yields one per entry. Reading eighteen tags and two repeating
        groups once per event was that work N times -- a fifth of the cost of
        reading a five-entry refresh.
        """
        get = self.get
        cfi = get(461)
        security_type = get(167)
        return Instrument(
            symbol=get(55) or "",
            kind=_classified(cfi, security_type),
            security_id=get(48),
            security_id_source=get(22),
            alt_ids=self.into_alt_ids() or None,
            security_type=security_type,
            cfi=cfi,
            exchange=get(207) or get(100),
            currency=get(15),
            multiplier=_number(get(231)),
            tick=_number(get(969)),
            lot=_number(get(561)),
            maturity=_date(get(541)) or _month_year(get(200)),
            strike=_number(get(202)),
            option_kind=OptionKind.from_fix(get(201), OptionKind.UNKNOWN),
            label=get(107),
            legs=self.into_legs() or None,
        )

    def into_alt_ids(self) -> dict[str, str]:
        """Every alternative identifier the message carried, by the scheme's name.

        The `NoSecurityAltID <454>` group: `SecurityAltID <455>` under
        `SecurityAltIDSource <456>`, which shares its enumeration with
        `SecurityIDSource <22>` -- so one reading serves the identifier an
        instrument leads with and every alternative beside it.

        Keyed by `IdSource`'s **name** and not by the FIX character, because
        the map is what a consumer reads: `alt_ids["ISIN"]` is a question, and
        `alt_ids["4"]` is a lookup table away from being one. A scheme this
        build has never seen keeps the character it came as, which is the only
        honest key left for it.
        """
        found: dict[str, str] = {}
        for entry in self._group(454, "NoSecurityAltID"):
            named = entry.get("455")
            scheme = IdSource.from_fix(entry.get("456"), IdSource.UNKNOWN)
            if not named:
                continue
            key = scheme.name if scheme is not IdSource.UNKNOWN else (entry.get("456") or "")
            found.setdefault(key or IdSource.UNKNOWN.name, named)
        return found

    def into_legs(self) -> list[Leg]:
        """The legs of a multileg instrument, from the `NoLegs <555>` group.

        Every member is the instrument field with a `Leg` in front of it --
        `LegSymbol <600>` is `Symbol <55>` for the leg -- so the reading is the
        same one, against a different set of tags.
        """
        built = []
        for entry in self._group(555, "NoLegs"):
            cfi, security_type = entry.get("608"), entry.get("609")
            built.append(
                Leg(
                    symbol=entry.get("600") or "",
                    side=Side.from_fix(entry.get("624"), Side.UNKNOWN),
                    ratio=_number(entry.get("623")),
                    kind=_classified(cfi, security_type),
                    security_id=entry.get("602"),
                    security_id_source=entry.get("603"),
                    cfi=cfi,
                    security_type=security_type,
                    exchange=entry.get("616"),
                    currency=entry.get("556"),
                    multiplier=_number(entry.get("614")),
                    maturity=_date(entry.get("611")) or _month_year(entry.get("610")),
                    strike=_number(entry.get("612")),
                    option_kind=OptionKind.from_fix(entry.get("1358"), OptionKind.UNKNOWN),
                )
            )
        return built

    def _group(self, count_tag: int, name: str) -> list[dict[str, str]]:
        """One repeating group's entries, each as first-value-by-tag.

        Both spellings, because a log prints whichever its bridge felt like:
        the wire's `NumInGroup` followed by entries in order, which
        `FixMessage.group` reads by the standard's own rules, and the indexed
        rendering (`NoLegs[0].LegSymbol`), which `indexed_group` folds back.

        An entry comes out as a dict rather than as pairs because the readings
        above probe it by tag a dozen times each, and a scan per probe is what
        `by_tag` exists to have stopped doing.
        """
        entries = self.message.group(count_tag) or self.message.indexed_group(name)
        found = []
        for entry in entries:
            resolved: dict[str, str] = {}
            for key, value in FixMessage.from_pairs(entry, market_tags()).pairs:
                resolved.setdefault(key, value)
            found.append(resolved)
        return found

    # -- what every event carries ------------------------------------------

    @property
    def unix(self) -> int:
        """When the transaction happened, by `TRANSACTED` -- not when it was sent.

        The one derivation this module exists for. Zero when the message
        carries none of them, which is a row that says it does not know
        rather than one that claims the epoch.
        """
        get = self.get
        recorded = unix_of(get(52)) or self.runix or None
        for source in TRANSACTED:
            if isinstance(source, tuple):
                date, time = source
                found = unix_of(get(date))
                clock = unix_of(get(time), day=found or recorded)
                found = clock if clock is not None else found
            else:
                found = unix_of(get(source))
            if found is not None:
                return found
        return 0

    def _expires(self) -> int | None:
        """`ExpireTime <126>`, or the end of `ExpireDate <432>` when only a day is given.

        The end, not the start: a GTD order dated today is good *through*
        today, and an expiry stamped at midnight would retire it before it
        ever traded.
        """
        stamped = unix_of(self.get(126))
        if stamped is not None:
            return stamped
        dated = unix_of(self.get(432))
        return None if dated is None else dated + SECONDS_A_DAY * NANOS - 1

    def _shared(self) -> dict[str, Any]:
        """The envelope every event off this message carries alike."""
        instrument = self.into_instrument()
        return {
            "seq": _integer(self.get(34)),
            "symbol": instrument.symbol,
            "venue": self.venue or self.get(30) or self.get(207) or self.get(49),
            "instrument": instrument,
            "px_unit": instrument.currency or "",
            "metadata": self.extras,
        }

    @property
    def extras(self) -> dict[str, str]:
        """Every field the shapes have no column for, under the key it arrived as.

        What makes this usable on a real venue: the fields no dictionary has,
        the ones a bridge renamed, and the ones a later FIX version added all
        land here instead of being dropped. The keys are the message's own, so
        a value put in by `from_pairs` under a custom name comes back out
        under it.

        One value per key, the first, because `metadata` is a `map` a row
        holds as a Python dict and a dict has no room for a second. A tag that
        really does repeat -- a group's -- is still whole on
        `message.pairs`, where `values` reads it.
        """
        claimed = _claimed_tags()
        return {key: value for key, value in self.by_tag.items() if key not in claimed}


def _claimed_tags() -> frozenset[str]:
    """Tags a column already holds, so `extras` does not repeat them.

    Derived from `market_tags` rather than listed, for the same reason that
    mapping is: a column that gains a tag stops being an extra, with nothing
    to remember. `FRAMING` joins them, being neither a column nor an extra.
    """
    global _CLAIMED
    if _CLAIMED is None:
        _CLAIMED = frozenset(str(tag) for tag in market_tags().values()) | FRAMING
    return _CLAIMED


_CLAIMED: frozenset[str] | None = None


#: `SecurityType <167>` to what an instrument settles as, for the venues that
#: send no `CFICode <461>`. FIX enumerates a hundred and eighteen of these and
#: most of them are one kind of bond, so this maps the **bands** rather than
#: the list: a value not here classifies as nothing, which is what `UNKNOWN`
#: is for and better than a guess. Read off the dictionary in `data/fix.zip`,
#: and checked against it by `tests/market/test_fix.py`.
SECURITY_TYPES: dict[str, AssetKind] = {
    # Equity
    "CS": AssetKind.EQUITY,
    "PS": AssetKind.EQUITY,
    # Collective investment
    "MF": AssetKind.FUND,
    # Derivatives
    "FUT": AssetKind.FUTURE,
    "OPT": AssetKind.OPTION,
    "OOF": AssetKind.OPTION,
    "OOP": AssetKind.OPTION,
    "OOC": AssetKind.OPTION,
    "WAR": AssetKind.WARRANT,
    "MLEG": AssetKind.MULTILEG,
    # Swaps, which FIX spells one per underlying
    "CDS": AssetKind.SWAP,
    "IRS": AssetKind.SWAP,
    "FXSWAP": AssetKind.SWAP,
    # Currency
    "FXSPOT": AssetKind.CURRENCY,
    "FXFWD": AssetKind.FORWARD,
    "FXNDF": AssetKind.FORWARD,
    "FORWARD": AssetKind.FORWARD,
    "CASH": AssetKind.CURRENCY,
    # Financing
    "REPO": AssetKind.REPO,
    "BUYSELL": AssetKind.REPO,
    "SECLOAN": AssetKind.LOAN,
    "SECPLEDGE": AssetKind.LOAN,
    "TERM": AssetKind.LOAN,
    "RVLV": AssetKind.LOAN,
    "RVLVTRM": AssetKind.LOAN,
    "BRIDGE": AssetKind.LOAN,
    "SWING": AssetKind.LOAN,
    # Debt: the long tail, by what a reader would call it
    "CORP": AssetKind.DEBT,
    "CB": AssetKind.DEBT,
    "TBOND": AssetKind.DEBT,
    "TNOTE": AssetKind.DEBT,
    "TBILL": AssetKind.DEBT,
    "TIPS": AssetKind.DEBT,
    "MUNI": AssetKind.DEBT,
    "GO": AssetKind.DEBT,
    "REV": AssetKind.DEBT,
    "MTN": AssetKind.DEBT,
    "CP": AssetKind.DEBT,
    "CD": AssetKind.DEBT,
    "ABS": AssetKind.DEBT,
    "MBS": AssetKind.DEBT,
    "CMO": AssetKind.DEBT,
    "FRN": AssetKind.DEBT,
    "EUCORP": AssetKind.DEBT,
    "EUSOV": AssetKind.DEBT,
    "BRADY": AssetKind.DEBT,
}


def _classified(cfi: str | None, security_type: str | None) -> AssetKind:
    """What an instrument settles as, from its CFI code or from FIX's own word.

    The CFI first, because ISO 10962's category letter *is* what `AssetKind` is
    coded on and it classifies exactly. `SecurityType <167>` after it, because
    a venue that sends no CFI very often sends `CS`, `FUT` or `OPT` instead --
    and a reading that stopped at the CFI left every one of those `UNKNOWN`.
    """
    if cfi:
        found = AssetKind.from_fix(cfi[:1], AssetKind.UNKNOWN)
        if found is not AssetKind.UNKNOWN:
            return found
    if security_type:
        return SECURITY_TYPES.get(security_type.strip().upper(), AssetKind.UNKNOWN)
    return AssetKind.UNKNOWN


def _month_year(text: str | None) -> datetime.date | None:
    """`MaturityMonthYear <200>` as a date -- the first of the month it names.

    FIX's `MonthYear` is `YYYYMM`, optionally with a day or a week after it.
    It is the older of the two ways to say when a contract expires, and a venue
    that sends it usually sends no `MaturityDate <541>` at all -- so reading it
    is the difference between a dated future and an undated one.

    The first of the month where no day is given, which is a date the venue did
    not state. It is the only reading that turns a month into an instant at
    all, and `maturity` is documented as when the contract expires rather than
    as what the message said.
    """
    if not text:
        return None
    trimmed = text.strip()
    if len(trimmed) < 6 or not trimmed[:6].isdigit():
        return None
    day = trimmed[6:8]
    try:
        return datetime.date(int(trimmed[:4]), int(trimmed[4:6]), int(day) if day.isdigit() else 1)
    except ValueError:
        return None


def _number(text: str | None) -> float | None:
    """A FIX `Price`, `Qty` or `float` as a float; None for anything that is not.

    None rather than zero, and that is the whole point: a price the venue did
    not send and a price of zero are different facts, and a market order has
    no limit at all.
    """
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _integer(text: str | None) -> int | None:
    """A FIX `int`, `SeqNum` or reject code as an int; None for anything else."""
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _flag(text: str | None) -> bool | None:
    """A FIX `Boolean`: `Y` or `N`, and None for a venue that sent neither."""
    if not text:
        return None
    first = text.strip()[:1].upper()
    return True if first == "Y" else False if first == "N" else None


def _date(text: str | None) -> datetime.date | None:
    """A FIX `LocalMktDate` or `UTCDateOnly` as a date."""
    stamped = unix_of(text)
    if stamped is None:
        return None
    return datetime.date.fromordinal(stamped // (SECONDS_A_DAY * NANOS) + _EPOCH_ORDINAL)
