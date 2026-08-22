"""Which FIX tags become columns of their own, and which column each becomes.

Two sets, ordered by what the fields *mean* rather than by tag number, because
a schema is read by people: the envelope first, then what was traded, who asked
for it, on what terms, where it stands, and for how much.

`SESSION` is the whole `StandardHeader` and `StandardTrailer` -- the fields
every FIX message carries whatever it says. They are the same fields on every
message of every type, so a map is the wrong shape to hold them: who sent it,
to whom, in what order and when is what a reader filters and joins on.

`COMMON` is what the components a trading log is actually made of carry:
`Instrument`, `OrderQtyData`, and the flat body of a `NewOrderSingle` or an
`ExecutionReport`. Typed and flat they are what a desk queries; left in a map
they are text behind a lookup.

**The rule, for both: a tag is lifted only where it occurs exactly once in the
line.** A tag that repeats belongs to a repeating group -- `Symbol <55>` inside
`NoRelatedSym`, `LastPx <31>` inside `NoLegs` -- and lifting the first
occurrence out of a group would answer "the symbol" with whichever leg came
first, which is a wrong answer that looks like a right one. Those rows keep
everything in `fix_tags` and the column is null: a multi-leg order has no one
symbol, and saying so is the honest column.

`NoHops <627>` and its members are not here for the same reason: a repeating
group is not one value.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

import pyarrow

#: The session layer -- `StandardHeader` and `StandardTrailer` -- grouped by
#: what each field answers. `34` lands on the column `Event` already declares
#: for it, so a parsed log line and a market event agree on what a sequence
#: number is.
SESSION: tuple[tuple[int, str], ...] = (
    # The envelope itself: what this is, and whether it arrived intact.
    (8, "begin_string"),
    (9, "body_length"),
    (35, "msg_type"),
    (10, "check_sum"),
    # Who sent it, and to whom.
    (49, "sender_comp_id"),
    (50, "sender_sub_id"),
    (142, "sender_location_id"),
    (56, "target_comp_id"),
    (57, "target_sub_id"),
    (143, "target_location_id"),
    # And on whose behalf, when a hub relayed it.
    (115, "on_behalf_of_comp_id"),
    (116, "on_behalf_of_sub_id"),
    (144, "on_behalf_of_location_id"),
    (128, "deliver_to_comp_id"),
    (129, "deliver_to_sub_id"),
    (145, "deliver_to_location_id"),
    # Where it sits in the session's stream, and whether it is a repeat.
    (34, "seq"),
    (369, "last_msg_seq_num_processed"),
    (43, "poss_dup_flag"),
    (97, "poss_resend"),
    # When it was sent -- which is not when anything happened.
    (52, "sending_unix"),
    (122, "orig_sending_unix"),
    (370, "on_behalf_of_sending_unix"),
    # Which application version speaks, under FIXT.
    (1128, "appl_ver_id"),
    (1129, "cstm_appl_ver_id"),
    (1156, "appl_ext_id"),
    # How the payload is written, when it is not plain ASCII.
    (347, "message_encoding"),
    (212, "xml_data_len"),
    (213, "xml_data"),
    # And how it is sealed.
    (90, "secure_data_len"),
    (91, "secure_data"),
    (93, "signature_length"),
    (89, "signature"),
)

#: What the components carry, in the order somebody reading a fill would ask.
#: `55` lands on `Event.symbol`, which is already declared as tag 55 -- one
#: column, not two answers to one question.
COMMON: tuple[tuple[int, str], ...] = (
    # What was traded.
    (55, "symbol"),
    (48, "security_id"),
    (22, "security_id_source"),
    (167, "security_type"),
    (461, "cfi_code"),
    (207, "security_exchange"),
    (15, "currency"),
    # Who asked, and under which identifiers.
    (1, "account"),
    (11, "cl_ord_id"),
    (41, "orig_cl_ord_id"),
    (37, "order_id"),
    (17, "exec_id"),
    # On what terms.
    (54, "side"),
    (40, "ord_type"),
    (59, "time_in_force"),
    # Where it stands.
    (39, "ord_status"),
    (150, "exec_type"),
    # For how much, at what price.
    (38, "order_qty"),
    (44, "price"),
    (6, "avg_px"),
    (14, "cum_qty"),
    (151, "leaves_qty"),
    (31, "last_px"),
    (32, "last_qty"),
    # When it happened, and whatever was said about it.
    (60, "transact_unix"),
    (58, "text"),
)

FLAT: tuple[tuple[int, str], ...] = SESSION + COMMON

#: Tag to the column it lands in. Read-only, because one shared mutable mapping
#: is a bug waiting for the second caller.
COLUMNS: Mapping[int, str] = MappingProxyType(dict(FLAT))

#: The same tags as an Arrow value set, built once: `is_in` against it is how a
#: whole batch's liftable fields are found in one pass.
TAGS: pyarrow.Array = pyarrow.array(sorted(COLUMNS), pyarrow.int32())

#: The lifted fields that are instants, and so land as **int64 nanoseconds**
#: rather than as an Arrow timestamp. Two reasons, and both bite: Iceberg's
#: timestamp is microseconds, so a `timestamp[ns]` column cannot be stored at
#: all and a `timestamp[us]` one would truncate a value whose text has just
#: been lifted out of the map -- unrecoverably. And every other instant this
#: package stores is int64 nanos (`unix`, `runix`), so a latency is a
#: subtraction rather than a conversion.
STAMPS: frozenset[int] = frozenset({52, 122, 370, 60})
