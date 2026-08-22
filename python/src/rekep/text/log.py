"""The shape of one parsed log line, what decides which event it is, and what fills it."""

from __future__ import annotations

import dataclasses
from typing import Annotated, Any, ClassVar, Protocol, runtime_checkable

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, field
from rekep.fix.rules import NO_PROTOCOL
from rekep.fix.transcribe import FIX_TAGS, KEYVAL
from rekep.market.enums import EventType
from rekep.market.event import Event


@field
class Log(Event):
    """One parsed line of a trading log.

    An `Event` like everything else this package stores, which is what lets a
    parsed log be read beside the orders and books it describes rather than
    beside nothing: `unix` is the instant the line is stamped with, `hash` is
    the digest of the raw line -- so the same capture read twice deduplicates
    itself -- and `etype` is what the line is *about*, decided by `LogRules`.

    `xhash` is the line's own `hash`: a log line is one version of one thing
    and never changes, so its lifecycle is itself. The rest of the envelope --
    `version`, `state`, the previous-version columns -- is constant here and
    costs nothing on disk, where a column of one repeated value encodes away.
    """

    url: str = ""
    """Path of the log the line came from, as its filesystem addresses it."""

    thread_name: str = ""
    """Contents of the first bracketed field."""

    driver_name: str = ""
    """Contents of the second bracketed field -- the emitting module."""

    message: str = ""
    """Payload with the header and level stripped, continuation lines folded in."""

    protocol: str = NO_PROTOCOL
    """Which protocol the line carries; OTHER is a line that carries none."""

    # A **map**, and both of them, because tags repeat -- a repeating group *is*
    # tags repeating -- and an Arrow map is the one nested type that keeps
    # duplicate keys in the order they arrived, which is why the parser already
    # returns one.
    #
    # Nullable, and null is not an empty map: a line carrying no message has no
    # pairs, a message that carried nothing has none *left*, and a store that
    # spelled those the same way could not tell a bridge that sent an empty
    # payload from a stack trace. The values inside are NOT NULL -- a pair
    # without a value is not a pair (`FixCodec.drop_null_values`).
    fix_tags: Annotated[dict[int, str] | None, Field(arrow_type=FIX_TAGS)] = None
    """The message's fields under the tags FIX gives them, in wire order."""

    keyval: Annotated[dict[str, str] | None, Field(arrow_type=KEYVAL)] = None
    """The fields no FIX tag answers for, spelled as the log spelled them."""

    # -- what a message says, flattened ---------------------------------------
    #
    # The FIX session layer and the fields the components a trading log is made
    # of carry (`rekep.fix.columns`). Each is a column of the type the
    # dictionary declares, and is **not** in `fix_tags` as well -- one fact
    # stored twice is one that can disagree with itself. A tag that repeats in
    # a line stays in the map instead, because it belongs to a repeating group
    # and no one value of it is the line's.
    #
    # Ordered by what they mean rather than by tag number. Two are `Event`'s
    # own: `symbol` is already declared as tag 55 and `seq` as tag 34. The
    # names live in `rekep/fix/columns.py`, the types here, and
    # `tests/text/test_log.py` pins both against the published dictionary.

    # The envelope itself.

    begin_string: str | None = None
    """`BeginString <8>`: which FIX version the message says it is."""

    body_length: int | None = None
    """`BodyLength <9>`, as the message counted it."""

    msg_type: str | None = None
    """`MsgType <35>`: what the message is, on the wire."""

    check_sum: str | None = None
    """`CheckSum <10>`: three digits, so a string -- `010` read as `10` no longer verifies."""

    # Who sent it, and to whom.

    sender_comp_id: str | None = None
    """`SenderCompID <49>`: who sent it."""

    sender_sub_id: str | None = None
    """`SenderSubID <50>`: which desk of theirs."""

    sender_location_id: str | None = None
    """`SenderLocationID <142>`."""

    target_comp_id: str | None = None
    """`TargetCompID <56>`: who it was sent to."""

    target_sub_id: str | None = None
    """`TargetSubID <57>`."""

    target_location_id: str | None = None
    """`TargetLocationID <143>`."""

    # And on whose behalf, when a hub relayed it.

    on_behalf_of_comp_id: str | None = None
    """`OnBehalfOfCompID <115>`: who the sender was speaking for."""

    on_behalf_of_sub_id: str | None = None
    """`OnBehalfOfSubID <116>`."""

    on_behalf_of_location_id: str | None = None
    """`OnBehalfOfLocationID <144>`."""

    deliver_to_comp_id: str | None = None
    """`DeliverToCompID <128>`: who it is ultimately for."""

    deliver_to_sub_id: str | None = None
    """`DeliverToSubID <129>`."""

    deliver_to_location_id: str | None = None
    """`DeliverToLocationID <145>`."""

    # Where it sits in the session's stream, and whether it is a repeat.

    last_msg_seq_num_processed: int | None = None
    """`LastMsgSeqNumProcessed <369>`: how far the sender had read."""

    poss_dup_flag: bool | None = None
    """`PossDupFlag <43>`: a retransmission of a message already sent."""

    poss_resend: bool | None = None
    """`PossResend <97>`: the same business content under a new sequence."""

    # When it was sent, which is not when anything happened. Nanoseconds since
    # the epoch, like every other instant here, and not an Arrow timestamp:
    # Iceberg's is microseconds, so `timestamp[ns]` cannot be stored and
    # `timestamp[us]` would truncate a value whose text has just been lifted
    # out of the map. It also makes a latency a subtraction -- `unix -
    # sending_unix` -- rather than a conversion.

    sending_unix: int | None = None
    """`SendingTime <52>`: when it was transmitted."""

    orig_sending_unix: int | None = None
    """`OrigSendingTime <122>`: the original transmission, on a resend."""

    on_behalf_of_sending_unix: int | None = None
    """`OnBehalfOfSendingTime <370>`."""

    # Which application version speaks, under FIXT.

    appl_ver_id: str | None = None
    """`ApplVerID <1128>`."""

    cstm_appl_ver_id: str | None = None
    """`CstmApplVerID <1129>`."""

    appl_ext_id: int | None = None
    """`ApplExtID <1156>`."""

    # How the payload is written, when it is not plain ASCII.

    message_encoding: str | None = None
    """`MessageEncoding <347>`."""

    xml_data_len: int | None = None
    """`XmlDataLen <212>`."""

    xml_data: bytes | None = None
    """`XmlData <213>`, as the bytes it is."""

    # And how it is sealed.

    secure_data_len: int | None = None
    """`SecureDataLen <90>`."""

    secure_data: bytes | None = None
    """`SecureData <91>`, as the bytes it is."""

    signature_length: int | None = None
    """`SignatureLength <93>`."""

    signature: bytes | None = None
    """`Signature <89>`, as the bytes it is."""

    # What was traded.

    security_id: str | None = None
    """`SecurityID <48>`, under the scheme `security_id_source` names."""

    security_id_source: str | None = None
    """`SecurityIDSource <22>`: which scheme `security_id` is in -- `4` is ISIN."""

    security_type: str | None = None
    """`SecurityType <167>`."""

    cfi_code: str | None = None
    """`CFICode <461>`: what kind of instrument it is, as ISO 10962 spells it."""

    security_exchange: str | None = None
    """`SecurityExchange <207>`: the market the instrument is listed on."""

    currency: str | None = None
    """`Currency <15>`, which is what the prices below are in."""

    # Who asked, and under which identifiers.

    account: str | None = None
    """`Account <1>`."""

    cl_ord_id: str | None = None
    """`ClOrdID <11>`: the client's own identifier for the order."""

    orig_cl_ord_id: str | None = None
    """`OrigClOrdID <41>`: which order an amendment or cancel is about."""

    order_id: str | None = None
    """`OrderID <37>`: the venue's identifier for it."""

    exec_id: str | None = None
    """`ExecID <17>`: the venue's identifier for this execution report."""

    # On what terms.

    side: str | None = None
    """`Side <54>`: `1` buy, `2` sell, and the rest of the standard's codes."""

    ord_type: str | None = None
    """`OrdType <40>`: `1` market, `2` limit, ..."""

    time_in_force: str | None = None
    """`TimeInForce <59>`: `0` day, `1` GTC, `3` IOC, ..."""

    # Where it stands.

    ord_status: str | None = None
    """`OrdStatus <39>`: where the order stands."""

    exec_type: str | None = None
    """`ExecType <150>`: what this report is reporting."""

    # For how much, at what price.

    order_qty: float | None = None
    """`OrderQty <38>`: how much was asked for."""

    price: float | None = None
    """`Price <44>`: the limit, when there is one."""

    avg_px: float | None = None
    """`AvgPx <6>`: the average of what has filled so far."""

    cum_qty: float | None = None
    """`CumQty <14>`: how much has filled."""

    leaves_qty: float | None = None
    """`LeavesQty <151>`: how much is still working."""

    last_px: float | None = None
    """`LastPx <31>`: the price of this fill."""

    last_qty: float | None = None
    """`LastQty <32>`: the size of this fill."""

    # When it happened, and whatever was said about it.

    transact_unix: int | None = None
    """`TransactTime <60>`: when the business event happened, in nanoseconds."""

    text: str | None = None
    """`Text <58>`: whatever the counterparty wrote, often the reject reason."""


@runtime_checkable
class MessageCodec(Protocol):
    """What a source calls to turn a message column into the columns a row carries.

    Five verbs and no more, which is the point: `TextFile` holds one of these
    and never learns which protocol it is reading. `rekep.fix.FixCodec` is the
    implementation this package ships; a second one over another protocol --
    market data, an internal binary envelope, a venue's own text format --
    implements the same five and the pipeline above it does not change.

    Every verb is per **batch** and takes whole columns: a seam that handed
    over one row at a time would put a Python loop in the middle of the hot
    path (`docs/logs.md`).
    """

    def categorise(self, messages: Any, drivers: Any = None) -> Any:
        """One `protocol` name per row."""
        ...

    def into_pairs(self, messages: Any, protocol: str = NO_PROTOCOL) -> Any:
        """One `map<string, string>` per row: the message as the line spells it.

        Addressed by the name `categorise` gave the row, because that is what
        the batch carries. Null, not an empty map, for a protocol that reads
        nothing.
        """
        ...

    def into_fix_pairs(self, pairs: Any, version: str | None = None) -> tuple[Any, Any]:
        """`pairs` split into the keys the protocol names and the keys it does not."""
        ...

    def version_of(
        self, message: str | None, protocol: str = NO_PROTOCOL
    ) -> tuple[str | None, str]:
        """Which protocol version a message is read under, and where that came from.

        The one a protocol without versions answers `(None, "none")` to. Here
        rather than inside `into_fix_pairs` because the pipeline resolves it
        once per slice and hands it down; per row would put a regex back in the
        hot path.
        """
        ...

    def into_flat_columns(self, tags: Any) -> tuple[dict[str, Any], Any]:
        """The fields worth a column of their own, lifted out of `tags`.

        `{column: array}` and what is left of the map. A protocol with nothing
        to lift returns `({}, tags)` and nothing above it changes.
        """
        ...


@dataclasses.dataclass
class LogRule(Convertible):
    """One pattern, and the kind of event a line matching it is."""

    pattern: str = ""
    """RE2 regular expression, matched anywhere in the message."""

    etype: EventType = EventType.UNKNOWN
    """What a line matching `pattern` is; readable by name in a configuration."""

    label: str = ""
    """What the rule is for, when the pattern does not say it plainly."""


#: What a FIX-carrying trading log is made of, by the two spellings every one
#: of them uses: the wire `35=` message type, and the name a rendered log
#: prints. Ordered most specific first, because the first match wins and a
#: single line can name more than one of them -- an execution report quoting
#: the order it fills says `ExecutionReport` *and* `NewOrderSingle`.
DEFAULT_RULES: tuple[LogRule, ...] = (
    LogRule(r"35=8(\D|$)|ExecutionReport", EventType.EXECUTION, "a fill, or a report of one"),
    LogRule(
        r"35=[DFG](\D|$)|NewOrderSingle|OrderCancel(Request|Replace)",
        EventType.ORDER,
        "an order, or an amendment to one",
    ),
    LogRule(
        r"35=X(\D|$)|MarketDataIncrementalRefresh",
        EventType.BOOK_SIDE,
        "an incremental book update",
    ),
    LogRule(r"35=W(\D|$)|MarketDataSnapshot", EventType.BOOK, "a full book snapshot"),
    LogRule(r"35=[SR](\D|$)|Quote(Request)?\b", EventType.QUOTE, "a quote, or a request for one"),
    LogRule(r"35=d(\D|$)|SecurityDefinition", EventType.INSTRUMENT, "reference data"),
)


@dataclasses.dataclass
class LogRules(Convertible):
    """Which `EventType` each line of a log is, by the first pattern that matches.

    A list of regular expressions and nothing else, so the whole thing is
    configuration: `LogRules.from_yaml("rules.yml")` reads one, and a desk with
    its own log format writes its own rather than patching this package.

    **The first match wins, and no match is `UNKNOWN`.** Both halves matter. An
    ordered list is what lets a specific rule sit in front of a general one
    without either having to know about the other, and a line nothing matches
    is still a line -- it is stored, keyed and partitioned like every other,
    under a type that says plainly that nobody has classified it. Dropping it,
    or guessing, is how a log stops being a record of what happened.

    The matching is one Arrow kernel per rule over the whole message column, so
    the cost is a handful of passes per batch rather than anything per row.
    """

    #: Rules in the order they are tried. The default reads a FIX trading log.
    rules: list[LogRule] = dataclasses.field(default_factory=lambda: list(DEFAULT_RULES))

    #: The Arrow type an `etype` column is, which is what the codes are cast to.
    CODE: ClassVar[pyarrow.DataType] = pyarrow.int32()

    def etype_arrow(self, messages: Any) -> pyarrow.Array:
        """One `etype` per message: the first rule that matches, else `UNKNOWN`.

        Applied **in reverse**, each rule overwriting what the ones after it
        decided, so the earliest rule in the list is the one that survives.
        That is the whole of "first match wins", and it is one pass per rule
        rather than a scan per row.

        A null message matches nothing rather than propagating: a line with no
        payload is unclassified, which `UNKNOWN` already says, and letting the
        null through would put a null in a NOT NULL column.
        """
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(int(EventType.UNKNOWN), self.CODE), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = compute.fill_null(compute.match_substring_regex(text, rule.pattern), False)
            found = compute.if_else(hit, pyarrow.scalar(int(rule.etype), self.CODE), found)
        return found.cast(self.CODE, safe=False)
