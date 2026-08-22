"""What is known about an instrument, as a row of its own."""

from __future__ import annotations

import dataclasses
from typing import Annotated, Any, ClassVar

from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import EventType
from rekep.market.event import Event
from rekep.market.fields import MarketFieldBuilder
from rekep.market.identity import NIL
from rekep.market.instrument import Instrument


@field
class Reference(Event):
    """One version of one instrument's reference data, as a row of its own.

    `Instrument` is what a market event carries *about* what it is trading, and
    it travels nested inside one. This is the same thing as a **table**: an
    event whose subject is the instrument, stamped with when it was learnt,
    versioned like everything else here, and snapshottable -- so "what did we
    know about this instrument at 14:00" is a row rather than a replay.

    Reference data is learnt rather than read: a venue sends a symbol first, a
    CFI code with the next message and a maturity with the one after, and each
    time it says something new that is a new version of what is known. `xhash`
    is the instrument's identity and does not move; `hash` is this version's
    content, so the same knowledge twice is one row.

    `EventType.INSTRUMENT` is what it declares, and the band it sits in --
    `REFERENCE` -- is "what a thing *is*, rather than anything that happened to
    it", which is the distinction this shape exists to draw.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    EVENT_TYPE: ClassVar[EventType] = EventType.INSTRUMENT

    # The same partition every market table uses, for the same reason: one
    # instrument's history is one bucket, so reading it is one file an hour
    # rather than a scan. An identity transform would be one directory per
    # instrument, which is the metadata explosion `docs/market.md` describes.
    instrument_hash: Annotated[int, Field.partition_key("bucket[16]")] = NIL
    """Which instrument this is about -- `instrument.xhash`, flat, for the partition."""

    instrument: Instrument = dataclasses.field(default_factory=Instrument)
    """What is known about it, as of this version."""

    def __post_init__(self) -> None:
        """The envelope's own normalisation, then the instrument the row is about."""
        super().__post_init__()
        if self.instrument.xhash:
            self.instrument_hash = self.instrument.xhash
            self.xhash = self.xhash or self.instrument.xhash
        if not self.symbol:
            self.symbol = self.instrument.symbol

    def life_parts(self) -> tuple[Any, ...]:
        """A reference row's lifecycle is the instrument it is about, and only that."""
        if not self.instrument_hash:
            return super().life_parts()
        return (self.instrument_hash,)

    def version_parts(self) -> tuple[Any, ...]:
        """A version of it moves when what is known moves, which is everything in it.

        The whole instrument and not a few of its fields: the point of the row
        is what is known, so two rows differ exactly when the knowledge does --
        which is also what makes the merge that lands them idempotent.
        """
        return (self.xhash, self.version, self.unix, *dataclasses.astuple(self.instrument))
