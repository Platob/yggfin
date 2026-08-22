"""Stable, banded, sorted integer codes: what a state, a side or a kind is stored as.

A market notion is stored as an `int32` and read back through one of these
enums. That split is the whole point:

- **The column is the integer**, so nothing a producer sends can make it
  unrepresentable. A code from a version this reader has never seen is stored,
  compared and partitioned exactly like one it knows, and `from_code` hands it
  back rather than raising -- an enum that refused would turn a forward-
  compatible column into an outage.
- **The enum is how code reads it**, so a call site says `State.FILLED` and not
  `410`, and the FIX character the wire carried is on the member itself.

The values are *banded*: every member sits in a hundred, and the hundreds are
ordered by how far through its life the thing is. That makes the questions a
query actually asks into **one range predicate**, which every engine below
Arrow can push down and prune on -- Iceberg on a manifest's min/max, Spark and
Doris on the same statistics::

    state >= State.TERMINAL          # done, whatever the reason
    State.OPEN <= state < State.DONE # live
    kind >= ExecKind.TRADE           # shares actually moved

A set of `IN` literals cannot prune like that: the manifest knows the minimum
and the maximum of a partition, not which values are in it. Ordering the codes
is what turns a filter into a skipped file.

**The floor of every band is itself a member**, because that is what makes an
unknown code degrade to something true. A state a later release adds at 440
reads back here as `DONE` -- still terminal, still countable -- and not as
`UNKNOWN`. Where the coarse answer *is* the plain one, the plain member takes
the floor: `Side.BUY` is 100, not 110, so an unknown buy code still signs `+1`.

**A value, once given, is never reused.** Adding a member means taking the next
free step in its band; renumbering one rewrites the meaning of data already on
disk. Steps of ten leave room to add without moving anything, and everything
from `Ranged.PRIVATE` up is reserved for a venue or a vendor, so a code added
here can never collide with one added there.
"""

from __future__ import annotations

import enum
from typing import Any, Self


class Ranged(enum.IntEnum):
    """One banded integer code, with the FIX character it was read from.

    Subclass it, declare `UNKNOWN = 0` and then `NAME = (value, "fix")` per
    member. The FIX character lives on the member because it is the only place
    it cannot drift from the value beside it -- a parallel mapping is a second
    thing to update and the one that gets forgotten.

    A band is a hundred: `State.FILLED.band` is `State.DONE`, and comparing
    against a band floor is the range predicate the docstring above is about.
    """

    #: How wide a band is. A member's band is its value floored to this.
    WIDTH = enum.nonmember(100)

    #: First value this package will never assign, left to venues and vendors.
    #: A feed with its own states puts them here and keeps them across upgrades.
    PRIVATE = enum.nonmember(9000)

    def __new__(cls, value: int, fix_code: str = "") -> Self:
        """Build the member as its integer, carrying the FIX character beside it.

        The band is computed here too, once per member, rather than on every
        read: a member is a singleton and its band is arithmetic on a constant,
        so recomputing it was a division and a multiplication per comparison --
        and `sign`, `moves_shares` and `is_a` are all comparisons. It was the
        single hottest call in the book fold, at 1.1M evaluations per four
        thousand events (`benchmarks/bench_market.py`).
        """
        member = int.__new__(cls, value)
        member._value_ = value
        member._fix_code = fix_code
        member._band = value // cls.WIDTH * cls.WIDTH
        return member

    # -- reading ------------------------------------------------------------

    @property
    def band(self) -> int:
        """This member's band floor -- the value a range predicate compares against."""
        return self._band

    @classmethod
    def band_of(cls, value: int) -> int:
        """The band floor of a raw code, without needing it to be a known member.

        The arithmetic a filter does, spelled once: a stored code this build
        has never seen still lands in the band its producer put it in, which
        is what keeps `>= TERMINAL` true for a state added after this release.
        """
        return int(value) // cls.WIDTH * cls.WIDTH

    def into_fix(self) -> str:
        """The FIX character this code was read from, or an empty string."""
        return self._fix_code

    # -- building -----------------------------------------------------------

    @classmethod
    def from_code(cls, value: Any, default: Self | None = None) -> Self:
        """A stored code as a member, falling back to its band and then to `UNKNOWN`.

        Never raises, because the column is the integer and the enum is only a
        reading of it: a code from a newer producer must degrade to something
        true -- its band, which is the part this build can still reason about
        -- rather than take the reader down. What is *stored* is untouched, so
        nothing is lost by reading coarsely.
        """
        try:
            return cls(int(value))
        except (ValueError, TypeError):
            pass
        try:
            return cls(cls.band_of(value))
        except (ValueError, TypeError):
            return default if default is not None else cls(0)

    @classmethod
    def from_fix(cls, code: Any, default: Self | None = None) -> Self:
        """The member a FIX character names, `UNKNOWN` when nothing does.

        Case-sensitive on purpose: FIX distinguishes `A` from `a` in several
        enumerations, and folding them would merge two meanings into one.
        """
        member = cls._fix_codes().get(str(code).strip() if code is not None else "")
        if member is not None:
            return member
        return default if default is not None else cls(0)

    @classmethod
    def _missing_(cls, value: Any) -> Self | None:
        """Accept a member's **name** as well as its value.

        So a configuration file can say `etype: ORDER` instead of `etype: 110`,
        and mean the same member -- which is the difference between a rule
        somebody can read and a number they have to look up. A name that is not
        a member falls through to the usual `ValueError`, so `from_code` still
        degrades rather than guessing.
        """
        if isinstance(value, str):
            return cls.__members__.get(value.strip().upper())
        return None

    @classmethod
    def _fix_codes(cls) -> dict[str, Self]:
        """FIX character -> member, built once per class on first lookup.

        Cached on the class rather than recomputed: reading a batch of orders
        calls `from_fix` once per row, and rebuilding the map each time would
        make a hash lookup into a walk of every member.
        """
        cached = cls.__dict__.get("_FIX_CODES")
        if cached is None:
            cached = {
                member._fix_code: member
                for member in cls
                if member._fix_code  # type: ignore[misc]
            }
            cls._FIX_CODES = cached
        return cached


class State(Ranged):
    """Where an event is in its life, ordered by how done it is.

    The bands are the three questions worth asking, and each is one comparison:
    below `OPEN` nothing is live yet, `OPEN <= state < DONE` is live, and
    `state >= TERMINAL` is over. `TERMINAL` is `DONE` under another name so a
    filter can say what it means without hard-coding 400.

    A pending replace or cancel sits in `OPEN`, not in `PENDING`: the request
    is in flight but **the order is still live**, and putting it anywhere else
    would make "live" the wrong answer for the seconds that matter most.

    Read from FIX `OrdStatus <39>` and `ExecType <150>`, which spell these
    states with the same characters.
    """

    #: First terminal state: `state >= State.TERMINAL` is the whole question.
    TERMINAL = enum.nonmember(400)

    UNKNOWN = 0
    """Nothing has been said about this event yet."""

    PENDING = 100
    """Band floor: requested, not yet acknowledged by the venue."""

    PENDING_NEW = 110, "A"
    """Sent, awaiting the venue's first acknowledgement."""

    OPEN = 200
    """Band floor: live at the venue, nothing done."""

    NEW = 210, "0"
    """Acknowledged and working."""

    ACCEPTED = 220, "D"
    """Taken for bidding or quoting, not yet working."""

    PENDING_REPLACE = 230, "E"
    """Amendment in flight; the original is still live."""

    PENDING_CANCEL = 240, "6"
    """Cancellation in flight; the order is still live."""

    SUSPENDED = 250, "9"
    """Held by the venue, resumable."""

    STOPPED = 260, "7"
    """Stopped at a price, awaiting the trade that follows."""

    PARTIAL = 300
    """Band floor: live and partly done."""

    PARTIALLY_FILLED = 310, "1"
    """Some quantity traded, the rest still working."""

    DONE = 400
    """Band floor, and the first terminal state: over, and completed."""

    FILLED = 410, "2"
    """Every share traded."""

    DONE_FOR_DAY = 420, "3"
    """Over for the session; what is left does not carry."""

    CALCULATED = 430, "B"
    """Priced and closed out by the venue."""

    CLOSED = 500
    """Band floor: over without completing."""

    CANCELLED = 510, "4"
    """Withdrawn before completing."""

    REPLACED = 520, "5"
    """Superseded by an amendment; the successor carries the lifecycle on."""

    EXPIRED = 530, "C"
    """Reached its expiry -- the `eunix` an `Event` declares -- while still live."""

    FAILED = 600
    """Band floor: over because it was refused."""

    REJECTED = 610, "8"
    """Refused; `reason` and `reason_code` say by whom and why."""

    @property
    def is_live(self) -> bool:
        """Whether the thing is working at the venue right now."""
        return State.OPEN <= self < State.TERMINAL

    @property
    def is_terminal(self) -> bool:
        """Whether nothing further will happen to it."""
        return self >= State.TERMINAL


class Side(Ranged):
    """Which way the interest points, banded so that direction is arithmetic.

    `BID` and `ASK` are the same members as `BUY` and `SELL` -- a book's bid
    side *is* its buy side, and two spellings of one code would let a filter on
    one silently miss rows written with the other.

    The band carries the sign, so a signed quantity is `side.sign * qty` and,
    over a column, one comparison against `Side.SELL` rather than a lookup
    table. Everything that is neither one-way nor the other -- crosses, the
    multileg relatives -- sits above both and signs to zero.

    Read from FIX `Side <54>`.
    """

    UNKNOWN = 0
    """No side stated."""

    BUY = 100, "1"
    """Buying, and the bid side of a book."""

    BID = 100
    """The bid side of a book, which is the buy side under its other name."""

    BUY_MINUS = 110, "3"
    """Buy, not above the last differing price."""

    BORROW = 120, "G"
    """Borrowing collateral (financing)."""

    SUBSCRIBE = 130, "D"
    """Subscribing to a fund."""

    SELL = 200, "2"
    """Selling, and the ask side of a book."""

    ASK = 200
    """The ask side of a book, which is the sell side under its other name."""

    SELL_PLUS = 210, "4"
    """Sell, not below the last differing price."""

    SELL_SHORT = 220, "5"
    """Selling stock not held."""

    SELL_SHORT_EXEMPT = 230, "6"
    """Selling short, exempt from the price test."""

    LEND = 240, "F"
    """Lending collateral (financing)."""

    REDEEM = 250, "E"
    """Redeeming a fund holding."""

    CROSS = 300, "8"
    """Both sides are the same participant."""

    CROSS_SHORT = 310, "9"
    """A cross whose sell leg is short."""

    CROSS_SHORT_EXEMPT = 320, "A"
    """A cross whose sell leg is short and exempt."""

    AS_DEFINED = 330, "B"
    """The direction the multileg instrument itself defines."""

    OPPOSITE = 340, "C"
    """The opposite of what the multileg instrument defines."""

    UNDISCLOSED = 350, "7"
    """Withheld, as an indication of interest may."""

    @property
    def sign(self) -> int:
        """`+1` buying, `-1` selling, `0` for anything two-sided or unstated."""
        band = self._band
        if band == _BUY_BAND:
            return 1
        if band == _SELL_BAND:
            return -1
        return 0

    @property
    def opposite(self) -> Side:
        """The plain other side; a cross or an unstated side is its own opposite."""
        if self._band == _BUY_BAND:
            return Side.SELL
        if self._band == _SELL_BAND:
            return Side.BUY
        return self


#: The two band floors `Side.sign` compares against, read once. Inside the
#: class they would be members of it, and reaching for them through
#: `Side.BUY.band` on every comparison was two extra attribute walks per sign.
_BUY_BAND = Side.BUY.band
_SELL_BAND = Side.SELL.band


class TimeInForce(Ranged):
    """How long an order lives, banded by whether it rests.

    Below `SESSION` nothing rests -- what does not trade on arrival is gone --
    so `tif < TimeInForce.SESSION` is the whole of "this never sat in a book",
    which is the split that matters when reading a day of orders back.

    `GTD` is the one that needs a date, and it does not carry its own: the
    expiry is `Event.eunix`, where every other expiry in this package already
    lives.

    Read from FIX `TimeInForce <59>`.
    """

    UNKNOWN = 0
    """No validity stated; the venue's default applies."""

    IMMEDIATE = 100
    """Band floor: does not rest -- trade now or not at all."""

    IOC = 110, "3"
    """Immediate or cancel: trade what can, cancel the rest."""

    FOK = 120, "4"
    """Fill or kill: trade all of it now, or none of it."""

    SESSION = 200
    """Band floor: rests, but not past the session."""

    DAY = 210, "0"
    """Good for the session."""

    AT_OPEN = 220, "2"
    """Only in the opening auction."""

    AT_CLOSE = 230, "7"
    """Only in the closing auction."""

    GTX = 240, "5"
    """Good till crossing: cancelled if it would cross."""

    RESTING = 300
    """Band floor: rests past the session."""

    GTC = 310, "1"
    """Good till cancelled."""

    GTD = 320, "6"
    """Good till a date, which the event's `eunix` carries."""

    @property
    def rests(self) -> bool:
        """Whether an unfilled order sits in the book instead of dying."""
        return self >= TimeInForce.SESSION


class OrderKind(Ranged):
    """How an order is priced, banded by what the price means.

    The band answers whether `px` is a limit, a trigger or nothing at all, so
    reading a mixed stream back needs no per-member table: a `MARKET` order's
    `px` is null and that is not missing data, a `STOP` order's `px` is the
    trigger and `stop_px` is where it triggers.

    Read from FIX `OrdType <40>`.
    """

    UNKNOWN = 0
    """No order type stated."""

    MARKET = 100
    """Band floor: no price -- take what is there."""

    MARKET_ORDER = 110, "1"
    """Plain market order."""

    MARKET_IF_TOUCHED = 120, "J"
    """Becomes a market order once the price is touched."""

    MARKET_TO_LIMIT = 130, "K"
    """Trades at market, the remainder resting as a limit at the last price."""

    LIMIT = 200
    """Band floor: `px` is a limit, and it is never null."""

    LIMIT_ORDER = 210, "2"
    """Plain limit order."""

    LIMIT_ON_CLOSE = 220, "B"
    """Limit, in the closing auction only."""

    LIMIT_OR_BETTER = 230, "7"
    """Limit that the venue may improve."""

    STOP = 300
    """Band floor: `stop_px` triggers, and `px` limits once it has."""

    STOP_ORDER = 310, "3"
    """Becomes a market order at `stop_px`."""

    STOP_LIMIT = 320, "4"
    """Becomes a limit order at `px` when `stop_px` is reached."""

    PEGGED = 400
    """Band floor: the price follows a reference rather than being stated."""

    PEGGED_ORDER = 410, "P"
    """Pegged to a reference price."""

    PREVIOUSLY_QUOTED = 420, "D"
    """Priced from a quote already given."""

    PREVIOUSLY_INDICATED = 430, "E"
    """Priced from an indication already given."""


class ExecKind(Ranged):
    """What an execution report says happened, banded by whether shares moved.

    `kind >= ExecKind.TRADE` is the one predicate that separates real fills
    from the acknowledgements, restatements and lifecycle notices that share
    the same message -- and summing quantity without it is how a volume figure
    ends up counting every ack as a trade.

    An amendment (`TRADE_CORRECT`, `TRADE_CANCEL`) is above `TRADE` because it
    also moves shares: it undoes or restates a fill already counted, and a
    reader that skipped it would keep the version that was withdrawn.

    Read from FIX `ExecType <150>`.
    """

    UNKNOWN = 0
    """Nothing stated."""

    STATUS = 100
    """Band floor: the venue is talking about the order, not about shares."""

    ACK = 110, "0"
    """The order was received."""

    PENDING_NEW = 120, "A"
    """Receipt is pending."""

    PENDING_CANCEL = 130, "6"
    """A cancellation is in flight."""

    PENDING_REPLACE = 140, "E"
    """An amendment is in flight."""

    ORDER_STATUS = 150, "I"
    """An answer to a status request, unsolicited by any change."""

    RESTATED = 160, "D"
    """The venue restated the order; `reason_code` says why."""

    CALCULATED = 170, "B"
    """The venue priced and closed the order out."""

    DONE_FOR_DAY = 180, "3"
    """Over for the session."""

    LIFECYCLE = 200
    """Band floor: the order changed, and no shares moved."""

    CANCELLED = 210, "4"
    """The order was withdrawn."""

    REPLACED = 220, "5"
    """The order was superseded by an amendment."""

    REJECTED = 230, "8"
    """The order was refused."""

    EXPIRED = 240, "C"
    """The order reached its expiry."""

    SUSPENDED = 250, "9"
    """The order was held."""

    STOPPED = 260, "7"
    """The order was stopped at a price."""

    TRADE = 300
    """Band floor, and the first kind that moves shares."""

    TRADED = 310, "F"
    """A fill, partial or complete -- what FIX 4.4 and later send."""

    PARTIAL_FILL = 320, "1"
    """A partial fill, as versions before 4.4 spelled it."""

    FILL = 330, "2"
    """A complete fill, as versions before 4.4 spelled it."""

    AMEND = 400
    """Band floor: a trade already reported changed."""

    TRADE_CORRECT = 410, "G"
    """A reported trade was restated."""

    TRADE_CANCEL = 420, "H"
    """A reported trade was withdrawn."""

    @property
    def moves_shares(self) -> bool:
        """Whether this report changes a position -- the only ones worth summing."""
        return self >= ExecKind.TRADE


class UpdateAction(Ranged):
    """What one book update does to a level, banded by whether it removes.

    `action >= UpdateAction.REMOVE` is every deletion, including the ranged
    ones a venue sends to clear a whole end of the book at once -- which is
    exactly the set a replay must apply before anything else, and exactly the
    set a naive `== DELETE` misses.

    Read from FIX `MDUpdateAction <279>`.
    """

    UNKNOWN = 0
    """Nothing stated."""

    APPLY = 100
    """Band floor: the level exists afterwards."""

    NEW = 110, "0"
    """A level that was not there."""

    CHANGE = 120, "1"
    """A level that was there, at a new size or price."""

    OVERLAY = 130, "5"
    """A level replaced wholesale, without saying what it was."""

    REMOVE = 200
    """Band floor: the level does not exist afterwards."""

    DELETE = 210, "2"
    """One level, gone."""

    DELETE_THRU = 220, "3"
    """Every level from the top of the book through this one."""

    DELETE_FROM = 230, "4"
    """Every level from this one to the bottom of the book."""

    @property
    def removes(self) -> bool:
        """Whether applying this update takes liquidity out of the book."""
        return self >= UpdateAction.REMOVE


class AssetKind(Ranged):
    """What is being traded, banded by how it settles.

    `kind >= AssetKind.DERIVATIVE` is everything whose value is another
    instrument's, which is the split that decides whether `multiplier`,
    `strike` and `maturity` mean anything on the row.

    Read from the first character of FIX `CFICode <461>` (ISO 10962), which is
    the only classification every venue publishes the same way; `SecurityType
    <167>` and `Product <460>` spell the same thing differently per venue and
    are kept verbatim on the instrument instead.
    """

    UNKNOWN = 0
    """Unclassified."""

    CASH = 100
    """Band floor: the instrument settles as itself."""

    EQUITY = 110, "E"
    """Shares and depositary receipts."""

    DEBT = 120, "D"
    """Bonds, notes and bills."""

    FUND = 130, "C"
    """Collective investment vehicles."""

    CURRENCY = 140, "T"
    """Cash currency pairs."""

    COMMODITY = 150, "J"
    """Physical commodities."""

    INDEX = 160, "M"
    """An index, which trades only through something else."""

    DERIVATIVE = 200
    """Band floor: the value is another instrument's."""

    FUTURE = 210, "F"
    """Exchange-traded futures."""

    OPTION = 220, "O"
    """Options, listed or otherwise."""

    SWAP = 230, "S"
    """Swaps of every leg count."""

    WARRANT = 240, "R"
    """Warrants and entitlements."""

    FORWARD = 250
    """Forwards, which FIX classifies per venue rather than by CFI."""

    STRUCTURED = 300
    """Band floor: built out of more than one instrument."""

    SPREAD = 310
    """Two legs quoted as their difference."""

    MULTILEG = 320
    """More than two legs quoted as one."""

    BASKET = 330
    """A weighted set traded as one."""

    FINANCING = 400
    """Band floor: the instrument is a loan against something else."""

    REPO = 410
    """Repurchase agreements."""

    LOAN = 420
    """Securities lending."""

    @property
    def is_derivative(self) -> bool:
        """Whether `multiplier`, `strike` and `maturity` mean anything here."""
        return self >= AssetKind.DERIVATIVE


class IdSource(Ranged):
    """Which scheme an instrument identifier is issued in, banded by who issues it.

    FIX numbers these on `SecurityIDSource <22>`, and `SecurityAltIDSource
    <456>` uses the same enumeration -- which is what lets one reading serve
    both the identifier an instrument leads with and every alternative it
    carries beside it.

    The bands are about **what the identifier is worth as an identity**, which
    is the question `Instrument.identify` asks:

    - `REGISTERED` -- issued by a numbering agency to one security, globally.
      An ISIN is the same ISIN at every venue and to every vendor, which is
      what makes it the strongest key an instrument can have.
    - `VENDOR` -- issued by somebody selling data. Unique and stable, but only
      inside that vendor's universe.
    - `LOCAL` -- a national scheme. Unique where it is used and unheard of
      elsewhere.
    - `VENUE` -- assigned by the market. Unique on it, and reused off it.
    - `OTHER` -- everything FIX also puts here, which is not an instrument
      identifier at all: a currency code, a country, a URL.
    """

    UNKNOWN = 0
    """No scheme was named, so the identifier means nothing on its own."""

    REGISTERED = 100
    """Band floor: issued by a numbering agency, and global."""

    ISIN = 110, "4"
    """ISO 6166. The strongest key an instrument carries, and what `isin_code` is."""

    CUSIP = 120, "1"
    """North American, issued by CUSIP Global Services."""

    SEDOL = 130, "2"
    """UK and Ireland, issued by the London Stock Exchange."""

    COMMON = 140, "G"
    """The Clearstream and Euroclear "Common Code"."""

    VENDOR = 200
    """Band floor: issued by a data vendor, and unique inside its universe."""

    RIC = 210, "5"
    """Refinitiv Instrument Code."""

    BLOOMBERG = 220, "A"
    """Bloomberg ticker."""

    LOCAL = 300
    """Band floor: a national scheme, unique where it is used."""

    WERTPAPIER = 310, "B"
    """German."""

    DUTCH = 320, "C"
    """Dutch."""

    VALOREN = 330, "D"
    """Swiss."""

    SICOVAM = 340, "E"
    """French."""

    BELGIAN = 350, "F"
    """Belgian."""

    QUIK = 360, "3"
    """Russian."""

    VENUE = 400
    """Band floor: assigned by a market, and reused off it."""

    EXCHANGE = 410, "8"
    """The exchange's own symbol for it."""

    CTA = 420, "9"
    """Consolidated Tape Association symbol."""

    OPRA = 430, "J"
    """Options Price Reporting Authority."""

    CLEARING = 440, "H"
    """Assigned by a clearing house."""

    MARKETPLACE = 450, "M"
    """Assigned by the marketplace, in the venue's own scheme."""

    OTHER = 500
    """Band floor: FIX puts these here and none of them names a security."""

    CURRENCY = 510, "6"
    """An ISO 4217 currency code, which is not an instrument identifier."""

    COUNTRY = 520, "7"
    """An ISO 3166 country code, likewise."""

    ISDA_SPEC = 530, "I"
    """An ISDA/FpML product specification, carried as XML elsewhere."""

    ISDA_URL = 540, "K"
    """An ISDA/FpML product URL, carried in `SecurityID` itself."""

    CREDIT_LETTER = 550, "L"
    """A letter of credit."""

    @property
    def is_registered(self) -> bool:
        """Whether an identifier in this scheme is issued rather than chosen.

        The test `Instrument.identify` makes: an issued identifier is the same
        one at every venue and to every vendor, which is what a lifecycle key
        has to be.
        """
        return self.band == IdSource.REGISTERED


class OptionKind(Ranged):
    """Which way an option points.

    Two members and a band each, which looks like ceremony until the
    alternative is spelled out: the alternative is an `int32` column holding
    FIX's own `0` and `1` with nothing in the schema saying which is which,
    and every reader deciding for itself. Every other protocol notion in this
    package is a `Ranged`, so this one is too.

    Read from FIX `PutOrCall <201>`.
    """

    UNKNOWN = 0
    """Not an option, or not stated."""

    PUT = 100, "0"
    """The right to sell at the strike."""

    CALL = 200, "1"
    """The right to buy at the strike."""


class EventType(Ranged):
    """What kind of thing an event is, banded by what it asserts.

    Every shape here is its own table, so within one table the column is
    constant and costs nothing -- run-length and dictionary encoding both
    collapse it to a handful of bytes. It exists for the read that spans them:
    a union of orders, executions and books is one stream of `Event`s, and
    `etype` is the only thing that says which row is which without looking at
    the columns that happen to be null.

    The bands are what a row *asserts*, which is the split a reader actually
    branches on: an intent is what somebody asked for and may never happen, a
    fact happened and cannot be taken back except by another fact, and a state
    is what was true at an instant. `etype >= EventType.STATE` is every row
    that is a snapshot of something rather than a thing in its own right.
    """

    UNKNOWN = 0
    """Nothing stated."""

    INTENT = 100
    """Band floor: somebody asked for something that may never happen."""

    ORDER = 110
    """An order, in any version of its life."""

    QUOTE = 120
    """A two-sided price somebody is willing to trade at."""

    FACT = 200
    """Band floor: it happened, and only another fact undoes it."""

    EXECUTION = 210
    """Something traded, or a report about an order that did not."""

    STATE = 300
    """Band floor: what was true at an instant, assembled from the rest."""

    BOOK_SIDE = 310
    """One side of one book."""

    BOOK = 320
    """Both sides of one book."""

    REFERENCE = 400
    """Band floor: what a thing *is*, rather than anything that happened to it."""

    INSTRUMENT = 410
    """One tradable instrument."""

    @property
    def is_snapshot(self) -> bool:
        """Whether the row is a picture of something rather than a thing itself."""
        return self >= EventType.STATE
