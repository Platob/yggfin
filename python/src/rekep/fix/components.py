"""Structured views of reusable FIX components."""

from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Mapping, Sequence
from functools import cache, cached_property
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.fields import Field, column_name, scalar
from rekep.fields.arrays import build_list, dense_counts, sequence
from rekep.fix.columns import DECLARED, ENTRIES
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.quickfix import entry_of, is_group, is_reference, members_of


@scalar
class Party:
    """One entry of FIX's Parties component."""

    partyid: Annotated[str | None, DECLARED["PartyID"]] = None
    """Party identifier."""

    partyidsource: Annotated[str | None, DECLARED["PartyIDSource"]] = None
    """Scheme or class of `PartyID`."""

    partyrole: Annotated[int | None, DECLARED["PartyRole"]] = None
    """Role the party has in the transaction."""


@scalar
class TrdRegTimestamp:
    """One entry of FIX's TrdRegTimestamps component."""

    trdregtimestamp: Annotated[datetime.datetime | None, DECLARED["TrdRegTimestamp"]] = None
    """The regulatory instant itself."""

    trdregtimestamptype: Annotated[int | None, DECLARED["TrdRegTimestampType"]] = None
    """Which regulatory instant it is."""

    trdregtimestamporigin: Annotated[str | None, DECLARED["TrdRegTimestampOrigin"]] = None
    """Who or what stamped it."""


@scalar
class SideTrdRegTimestamp:
    """One entry of FIX's SideTrdRegTS component.

    The per-side variant of `TrdRegTimestamp`: a message that reports both
    sides of a trade stamps each of them, and the two are different instants.
    The type codes are `TrdRegTimestampType <770>`'s own vocabulary -- FIX
    gives `SideTrdRegTimestampType <1013>` the same meanings -- so one
    preference table reads both.
    """

    sidetrdregtimestamp: Annotated[datetime.datetime | None, DECLARED["SideTrdRegTimestamp"]] = None
    """The regulatory instant for this side."""

    sidetrdregtimestamptype: Annotated[int | None, DECLARED["SideTrdRegTimestampType"]] = None
    """Which regulatory instant it is, in `TrdRegTimestampType`'s codes."""

    sidetrdregtimestampsrc: Annotated[str | None, DECLARED["SideTrdRegTimestampSrc"]] = None
    """Who or what stamped it."""


@scalar
class SecurityAltID:
    """One entry of FIX's SecAltIDGrp component: one alternative identifier."""

    securityaltid: Annotated[str | None, DECLARED["SecurityAltID"]] = None
    """The alternative identifier itself."""

    securityaltidsource: Annotated[str | None, DECLARED["SecurityAltIDSource"]] = None
    """Scheme or class of `SecurityAltID`, in `SecurityIDSource`'s codes."""


@scalar
class Leg:
    """One entry of FIX's InstrmtLegGrp component: one leg of a multileg.

    Every member is the instrument field with a `Leg` in front of it --
    `LegSymbol <600>` is `Symbol <55>` for the leg -- and the column takes the
    generic spelling, because the nesting already says whose it is. That is
    also what makes these the names `rekep.market.instrument.Leg` carries.
    Everything else a venue sends with a leg stays in the row's residual
    `entries`, under the key the wire carried.
    """

    symbol: Annotated[str | None, DECLARED["LegSymbol"]] = None
    """Identifier as the venue spells the leg; what opens an entry."""

    securityid: Annotated[str | None, DECLARED["LegSecurityID"]] = None
    """Identifier in the scheme `LegSecurityIDSource` names."""

    securityidsource: Annotated[str | None, DECLARED["LegSecurityIDSource"]] = None
    """Which scheme `LegSecurityID` is in, as FIX numbers them."""

    securitytype: Annotated[str | None, DECLARED["LegSecurityType"]] = None
    """What the venue calls this leg, from FIX's own list."""

    cficode: Annotated[str | None, DECLARED["LegCFICode"]] = None
    """ISO 10962 classification of the leg."""

    securityexchange: Annotated[str | None, DECLARED["LegSecurityExchange"]] = None
    """Where the leg is listed, when it differs from the strategy's venue."""

    maturitydate: Annotated[datetime.date | None, DECLARED["LegMaturityDate"]] = None
    """When the leg expires; null for anything that does not."""

    maturitymonthyear: Annotated[str | None, DECLARED["LegMaturityMonthYear"]] = None
    """The older month-resolution way to say when the leg expires."""

    strikeprice: Annotated[float | None, DECLARED["LegStrikePrice"]] = None
    """Exercise price, where the leg is an option."""

    putorcall: Annotated[int | None, DECLARED["LegPutOrCall"]] = None
    """Which way the leg points, where it is an option."""

    contractmultiplier: Annotated[float | None, DECLARED["LegContractMultiplier"]] = None
    """Units of the underlying one leg contract represents."""

    currency: Annotated[str | None, DECLARED["LegCurrency"]] = None
    """ISO 4217 currency the leg is priced in."""

    side: Annotated[str | None, DECLARED["LegSide"]] = None
    """Which way the strategy takes this leg, in `Side <54>`'s codes."""

    ratioqty: Annotated[float | None, DECLARED["LegRatioQty"]] = None
    """How many of this leg one unit of the strategy is; the leg's weight."""


def _entries_type(row: type) -> pyarrow.DataType:
    """The list one component's entries land in: never a null entry, ever."""
    return pyarrow.list_(pyarrow.field("item", row.into_field().dtype, nullable=False))


PARTIES: pyarrow.DataType = _entries_type(Party)
TRD_REG_TIMESTAMPS: pyarrow.DataType = _entries_type(TrdRegTimestamp)
SIDE_TRD_REG_TIMESTAMPS: pyarrow.DataType = _entries_type(SideTrdRegTimestamp)
SECURITY_ALT_IDS: pyarrow.DataType = _entries_type(SecurityAltID)
LEGS: pyarrow.DataType = _entries_type(Leg)

_NO_PARTY_IDS = "NoPartyIDs"
_NO_TRD_REG_TIMESTAMPS = "NoTrdRegTimestamps"
_NO_SIDE_TRD_REG_TS = "NoSideTrdRegTS"
_NO_SECURITY_ALT_ID = "NoSecurityAltID"
_NO_LEGS = "NoLegs"
_UNSIGNED = r"^[0-9]{1,18}$"
_SIGNED = r"^[+-]?[0-9]{1,18}$"
_DECIMAL = r"^[+-]?(?:[0-9]{1,17}(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?$"
_GROUP_STRIDE = 2**32


@dataclasses.dataclass(eq=False)
class ComponentGroup:
    """Extract one FIX repeating group without interpreting whole messages.

    Everything below is declaration-driven: the count tags, the member names,
    the paths each member sits under and the tag that opens an entry all come
    out of the component tree, and nothing in the state machine knows the word
    "party". What a subclass adds is the shape the group projects into --
    which component, which group inside it, and which members earn a column of
    their own rather than a place in a second residual.
    """

    components: Mapping[str, Field] | Sequence[Field] | None = None
    names: Mapping[str, int] | None = None

    #: Which component to read, and which repeating group inside it.
    component: str = ""
    group: str = ""

    #: Whether an occurrence inside another group's entry belongs to that
    #: entry rather than to the message. The regulatory components hoist
    #: deliberately -- `SideTrdRegTS` lives inside `NoSides` by definition and
    #: its consumers read it off the message -- but an instrument group inside
    #: one market-data entry describes that entry's instrument, and lifting it
    #: to the message would file the identifiers under whichever instrument
    #: the header names. A scoped group is only extracted where it opens
    #: before every count that could own it; anywhere else it stays residual,
    #: which is where the per-entry readers already look.
    scoped: bool = False

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one entry of this group is."""
        raise NotImplementedError

    @classmethod
    @cache
    def into_entries_type(cls) -> pyarrow.DataType:
        """The Arrow list a whole row's entries land in."""
        return _entries_type(cls.into_row())

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`((column, FIX member name), ...)`, the group's delimiter first.

        The delimiter leads because it is what opens an entry, so its column is
        the one every entry has. Every other member named here is lifted where
        its value is one the column's type can hold, and stays residual
        where it is not -- so a malformed stamp is kept as the text that
        arrived rather than becoming a null nobody can explain.
        """
        raise NotImplementedError

    def __post_init__(self) -> None:
        """Hold stable declaration and name snapshots for repeated batches."""
        declared = self.components
        if isinstance(declared, Mapping):
            declared = tuple(declared.values())
        self.components = tuple(declared or ())
        self.names = dict(self.names or {})

    def into_arrow_arrays(self, tags: Any) -> tuple[Any, Any]:
        """Return `(entries, residual_tags)`, preserving row validity and order."""
        entries_type = self.into_entries_type()
        if isinstance(tags, pyarrow.ChunkedArray):
            parts = [self.into_arrow_arrays(chunk) for chunk in tags.chunks]
            return (
                pyarrow.chunked_array([found for found, _ in parts], type=entries_type),
                pyarrow.chunked_array([rest for _, rest in parts], type=ENTRIES),
            )
        if not isinstance(tags, pyarrow.Array) or tags.type != ENTRIES:
            actual = getattr(tags, "type", type(tags).__name__)
            raise TypeError(f"{type(self).__name__} needs {ENTRIES}, got {actual}")
        return self._extract(tags)

    def _extract(self, tags: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
        """One physical Arrow chunk through the component state machine."""
        compute = pyarrow.compute
        entries_type = self.into_entries_type()
        rows = len(tags)
        entries = compute.list_flatten(tags)
        if not len(entries):
            return pyarrow.nulls(rows, entries_type), tags

        keys = compute.struct_field(entries, "tag")
        relevant = compute.is_in(keys, value_set=self._relevant_array)
        if not compute.any(relevant, min_count=0).as_py():
            return pyarrow.nulls(rows, entries_type), tags

        parents = compute.list_parent_indices(tags).cast(pyarrow.int64())
        values = compute.struct_field(entries, "value")
        positions = sequence(len(entries))
        row_ids = sequence(rows)

        count_match = compute.is_in(keys, value_set=self._count_array)
        count_occurrences = dense_counts(compute.filter(parents, count_match), rows)
        count_positions = _first_by_parent(positions, parents, count_match, row_ids)
        count_values = _first_by_parent(values, parents, count_match, row_ids)
        count_is_numeric = compute.fill_null(
            compute.match_substring_regex(count_values, _UNSIGNED), False
        )
        declared_count = compute.if_else(count_is_numeric, count_values, pyarrow.scalar("0")).cast(
            pyarrow.int64()
        )

        allowed = compute.is_in(keys, value_set=self._member_array)
        is_delimiter = compute.is_in(keys, value_set=self._delimiter_array)
        counted_inside = _contiguous_after(
            positions, parents, count_positions, count_match, allowed
        )
        counted_delimiters = dense_counts(
            compute.filter(parents, compute.and_(counted_inside, is_delimiter)), rows
        )
        first_counted_delimiter = _first_by_parent(
            positions,
            parents,
            compute.and_(counted_inside, is_delimiter),
            row_ids,
        )
        positive = compute.greater(declared_count, 0)
        immediate = compute.if_else(
            positive,
            compute.equal(first_counted_delimiter, compute.add(count_positions, 1)),
            pyarrow.scalar(True),
        )
        counted_valid = _all(
            compute.equal(count_occurrences, 1),
            count_is_numeric,
            compute.equal(counted_delimiters, declared_count),
            compute.fill_null(immediate, False),
        )

        first_delimiter = _first_by_parent(positions, parents, is_delimiter, row_ids)
        inferred_inside = _contiguous_from(positions, parents, first_delimiter, allowed)
        inferred_delimiters = dense_counts(
            compute.filter(parents, compute.and_(inferred_inside, is_delimiter)), rows
        )
        inferred_valid = _all(
            compute.equal(count_occurrences, 0),
            compute.greater(inferred_delimiters, 0),
        )
        if self.scoped and len(self._enclosing_array):
            # An occurrence that opens after a count that could own it is one
            # entry's, not the message's: leave it residual for the per-entry
            # readers. Absent either position, there is nothing to be inside.
            enclosing = compute.is_in(keys, value_set=self._enclosing_array)
            first_enclosing = _first_by_parent(positions, parents, enclosing, row_ids)
            counted_valid = compute.and_(
                counted_valid,
                compute.fill_null(compute.less(count_positions, first_enclosing), True),
            )
            inferred_valid = compute.and_(
                inferred_valid,
                compute.fill_null(compute.less(first_delimiter, first_enclosing), True),
            )

        counted_parent = compute.take(counted_valid, parents)
        inferred_parent = compute.take(inferred_valid, parents)
        positive_parent = compute.take(positive, parents)
        members = compute.or_(
            _all(counted_inside, counted_parent, positive_parent),
            compute.and_(inferred_inside, inferred_parent),
        )
        delimiters = compute.and_(members, is_delimiter)
        valid_rows = compute.or_(counted_valid, inferred_valid)
        party_sizes = compute.if_else(
            counted_valid,
            declared_count,
            compute.if_else(inferred_valid, inferred_delimiters, 0),
        ).cast(pyarrow.int64())

        running_delimiters = compute.cumulative_sum(delimiters.cast(pyarrow.int64()))
        counted_baseline = _take_or_zero(running_delimiters, count_positions)
        inferred_baseline = compute.subtract(_take_or_zero(running_delimiters, first_delimiter), 1)
        row_baseline = compute.if_else(counted_valid, counted_baseline, inferred_baseline)
        party_rank = compute.subtract(running_delimiters, compute.take(row_baseline, parents))
        party_group = compute.add(
            compute.multiply(parents, pyarrow.scalar(_GROUP_STRIDE, pyarrow.int64())),
            party_rank,
        )
        party_groups = compute.filter(party_group, delimiters)
        row_field = self.into_row().into_field()
        lifted: list[Any] = []
        # Nothing is projected until a column can hold it, the delimiter
        # included: an entry is opened by the delimiter's *position*, which
        # `party_sizes` already counted, not by its value being readable.
        projected = pyarrow.repeat(pyarrow.scalar(False), len(keys))
        for index, (column, member) in enumerate(self.into_projection()):
            # The delimiter is the member that opens an entry, so its own
            # occurrences already are the per-entry firsts.
            matched = (
                delimiters
                if index == 0
                else compute.and_(members, compute.is_in(keys, value_set=self._tags_named(member)))
            )
            text, at = _first_for_party(values, positions, party_group, matched, party_groups)
            target = row_field.field(column).dtype
            readable = _readable(text, target)
            lifted.append(
                cast_arrow_fix(
                    compute.if_else(readable, text, pyarrow.scalar(None, pyarrow.string())),
                    target,
                )
            )
            # A value the column cannot hold is not projected, so it stays
            # in the residual `entries` as the text that arrived rather than
            # becoming a null nobody can explain. The delimiter still opens
            # its entry either way.
            projected = compute.or_(
                projected, _positions_are(positions, compute.filter(at, readable))
            )

        entry_struct = pyarrow.StructArray.from_arrays(lifted, fields=row_field.arrow_fields)
        extracted = build_list(
            entries_type,
            party_sizes,
            entry_struct,
            mask=compute.invert(valid_rows),
        )

        # Only what a column now holds leaves the residual. A member this
        # component does not project stays in `entries` under the key the wire
        # carried, which is the one place a value nothing lifted belongs --
        # there is no second residual beside it to look in as well.
        remove = compute.or_(
            projected,
            compute.and_(count_match, compute.take(counted_valid, parents)),
        )
        keep = compute.invert(remove)
        residual_sizes = dense_counts(compute.filter(parents, keep), rows)
        residual = build_list(
            ENTRIES,
            residual_sizes,
            _kept(entries, keep),
            mask=compute.is_null(tags) if tags.null_count else None,
        )
        return extracted, residual

    @cached_property
    def _declaration(
        self,
    ) -> tuple[
        set[int],
        dict[int, str],
        dict[int, tuple[str, ...]],
        dict[tuple[str, ...], set[int]],
    ]:
        """Count tags, member names, and their component paths.

        The declaration decides all four, and nothing else does: a registry
        always carries its component declarations, so a version whose tree is
        absent from `components` extracts nothing rather than falling back on
        tags this class guessed.
        """
        wanted = column_name(self.component)
        grouped = column_name(self.group)
        by_name = {column_name(component.name): component for component in self.components}
        counts: set[int] = set()
        members: dict[int, str] = {}
        paths: dict[int, tuple[str, ...]] = {}
        group_delimiters: dict[tuple[str, ...], set[int]] = {}
        name_tags = {column_name(name): int(tag) for name, tag in self.names.items()}

        def member_tags(member: Field) -> tuple[int, ...]:
            found: list[int] = []
            declared = member.fix.tag
            if declared:
                found.append(int(declared))
            mapped = name_tags.get(column_name(member.name))
            if mapped is not None and mapped not in found:
                found.append(mapped)
            return tuple(found)

        def add(member: Field, path: tuple[str, ...]) -> None:
            for tag in member_tags(member):
                members.setdefault(tag, member.name)
                paths.setdefault(tag, path)

        def first_tags(declared: Field, seen: frozenset[str]) -> tuple[int, ...]:
            for member in members_of(declared):
                if not is_reference(member):
                    tags = member_tags(member)
                    if tags:
                        return tags
                else:
                    key = column_name(member.name)
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        tags = first_tags(nested, seen | {key})
                        if tags:
                            return tags
            return ()

        def visit(declared: Field, seen: frozenset[str], path: tuple[str, ...] = ()) -> None:
            for member in members_of(declared):
                if is_group(member):
                    add(member, path)
                    nested_path = (*path, member.name)
                    entry = entry_of(member)
                    group_delimiters.setdefault(nested_path, set()).update(first_tags(entry, seen))
                    visit(entry, seen, nested_path)
                elif is_reference(member):
                    key = column_name(member.name)
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        visit(nested, seen | {key}, path)
                else:
                    add(member, path)

        def find(declared: Field, seen: frozenset[str]) -> None:
            for member in members_of(declared):
                if is_group(member) and column_name(member.name) == grouped:
                    counts.update(member_tags(member))
                    # The group's own delimiter, which the standard fixes as
                    # its first member: read off the declaration rather than
                    # named here, so a group whose entries open with something
                    # other than `PartyID` splits at the right tag.
                    entry = entry_of(member)
                    group_delimiters.setdefault((), set()).update(first_tags(entry, seen))
                    visit(entry, seen)
                elif is_reference(member):
                    key = column_name(member.name)
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        find(nested, seen | {key})

        for component in self.components:
            key = column_name(component.name)
            if key == wanted:
                find(component, frozenset({key}))
                # Hand-written declarations sometimes omit the outer group.
                visit(
                    _without(
                        component,
                        lambda one: is_group(one) and column_name(one.name) == grouped,
                    ),
                    frozenset({key}),
                )
            else:
                find(component, frozenset({key}))
        return counts, members, paths, group_delimiters

    @cached_property
    def _count_array(self) -> pyarrow.Array:
        return pyarrow.array(sorted(self._declaration[0]), pyarrow.int32())

    @cached_property
    def _member_names(self) -> dict[int, str]:
        return self._declaration[1]

    @cached_property
    def _member_paths(self) -> dict[int, tuple[str, ...]]:
        return self._declaration[2]

    @cached_property
    def _group_delimiters(self) -> dict[tuple[str, ...], set[int]]:
        return self._declaration[3]

    @cached_property
    def _delimiter_array(self) -> pyarrow.Array:
        """The tags that open one entry of the group, off its own declaration."""
        return pyarrow.array(sorted(self._group_delimiters.get((), ())), pyarrow.int32())

    @cached_property
    def _member_array(self) -> pyarrow.Array:
        return pyarrow.array(sorted(self._member_names), pyarrow.int32())

    @cached_property
    def _relevant_array(self) -> pyarrow.Array:
        """Tags that can start or belong to this group."""
        return pyarrow.array(
            sorted(self._declaration[0] | set(self._member_names)), pyarrow.int32()
        )

    def _tags_named(self, name: str) -> pyarrow.Array:
        """Every declared tag carrying one canonical member name."""
        wanted = column_name(name)
        return pyarrow.array(
            sorted(
                tag for tag, found in self._member_names.items() if column_name(found) == wanted
            ),
            pyarrow.int32(),
        )

    @cached_property
    def _enclosing_array(self) -> pyarrow.Array:
        """Count tags of every group that nests this one inside its entries.

        Read off the declaration forest like everything else here: a group
        whose subtree reaches this component's group scopes it -- an
        `NoMDEntries <268>` entry carries an `Instrument`, so a
        `NoSecurityAltID <454>` opening after `268` belongs to one entry and
        not to the message. Groups whose entries cannot reach this one --
        underlyings beside legs, legs beside alt-ids -- are not collected, so
        their presence on a line never refuses a top-level extraction.
        """
        grouped = column_name(self.group)
        by_name = {column_name(component.name): component for component in self.components}
        name_tags = {column_name(name): int(tag) for name, tag in (self.names or {}).items()}

        def contains(declared: Field, seen: frozenset[str]) -> bool:
            for member in members_of(declared):
                if is_group(member):
                    if column_name(member.name) == grouped or contains(entry_of(member), seen):
                        return True
                elif is_reference(member):
                    key = column_name(member.name)
                    nested = by_name.get(key)
                    if nested is None or key in seen:
                        continue
                    if contains(nested, seen | {key}):
                        return True
            return False

        found: set[int] = set()

        def visit(declared: Field, seen: frozenset[str]) -> None:
            for member in members_of(declared):
                if is_group(member):
                    entry = entry_of(member)
                    if column_name(member.name) != grouped and contains(entry, seen):
                        declared_tag = member.fix.tag
                        if declared_tag:
                            found.add(int(declared_tag))
                        mapped = name_tags.get(column_name(member.name))
                        if mapped is not None:
                            found.add(mapped)
                    visit(entry, seen)
                elif is_reference(member):
                    key = column_name(member.name)
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        visit(nested, seen | {key})

        for component in self.components:
            visit(component, frozenset({column_name(component.name)}))
        return pyarrow.array(sorted(found), pyarrow.int32())


def _without(declared: Field, drop: Any) -> Field:
    """One block with the members `drop` names taken out of it."""
    kept = [member.into_arrow_field() for member in members_of(declared) if not drop(member)]
    return Field(name=declared.name, dtype=pyarrow.struct(kept), nullable=declared.nullable)


def _readable(text: Any, dtype: pyarrow.DataType) -> Any:
    """Which values a column of this type can hold, as a mask over `text`.

    The mask and not the cast: `cast_arrow_fix` already nulls what it cannot
    read, and what a caller here needs is the *other* half of that answer --
    which values were not read, so they can be kept as the text that arrived.
    """
    compute = pyarrow.compute
    kinds = pyarrow.types
    if kinds.is_integer(dtype):
        pattern = _SIGNED
    elif kinds.is_floating(dtype) or kinds.is_decimal(dtype):
        pattern = _DECIMAL
    elif kinds.is_temporal(dtype):
        # The cast itself, not its pattern: the reader range-checks what the
        # shape alone admits -- an absurd clock, an impossible zone -- and a
        # gate that answered from the shape would mark such text readable,
        # project the null the cast makes of it, and drop the text nobody
        # can then explain.
        return compute.is_valid(cast_arrow_fix(text, dtype))
    else:
        return compute.is_valid(text)
    return compute.fill_null(compute.match_substring_regex(text, pattern), False)


@dataclasses.dataclass(eq=False)
class Parties(ComponentGroup):
    """FIX's Parties component, entry by entry."""

    component: str = "Parties"
    group: str = _NO_PARTY_IDS

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one party is."""
        return Party

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`PartyID` opens an entry; the scheme and the role earn columns too."""
        return (
            ("PartyID", "PartyID"),
            ("PartyIDSource", "PartyIDSource"),
            ("PartyRole", "PartyRole"),
        )


@dataclasses.dataclass(eq=False)
class TrdRegTimestamps(ComponentGroup):
    """FIX's TrdRegTimestamps component, entry by entry.

    The regulatory clock: when a venue, a gateway or a desk stamped an order,
    and which of those each stamp was. Structured for the same reason parties
    are -- the three members always arrive together and mean nothing apart, so
    a reader wanting "the venue's own stamp" should not be reassembling them
    out of a flat pair list by index.
    """

    component: str = "TrdRegTimestamps"
    group: str = _NO_TRD_REG_TIMESTAMPS

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one regulatory stamp is."""
        return TrdRegTimestamp

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`TrdRegTimestamp` opens an entry; its type and origin qualify it."""
        return (
            ("TrdRegTimestamp", "TrdRegTimestamp"),
            ("TrdRegTimestampType", "TrdRegTimestampType"),
            ("TrdRegTimestampOrigin", "TrdRegTimestampOrigin"),
        )


@dataclasses.dataclass(eq=False)
class SideTrdRegTimestamps(ComponentGroup):
    """FIX's SideTrdRegTS component, entry by entry.

    Structured for the same reason `TrdRegTimestamps` is: the three members
    arrive together and mean nothing apart. A reader wanting "when this side
    was executed" should not be reassembling them out of a flat pair list by
    index.
    """

    component: str = "SideTrdRegTS"
    group: str = _NO_SIDE_TRD_REG_TS

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one per-side regulatory stamp is."""
        return SideTrdRegTimestamp

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`SideTrdRegTimestamp` opens an entry; its type and source qualify it."""
        return (
            ("SideTrdRegTimestamp", "SideTrdRegTimestamp"),
            ("SideTrdRegTimestampType", "SideTrdRegTimestampType"),
            ("SideTrdRegTimestampSrc", "SideTrdRegTimestampSrc"),
        )


@dataclasses.dataclass(eq=False)
class SecurityAltIDs(ComponentGroup):
    """FIX's SecAltIDGrp component, entry by entry.

    Every other identifier an instrument is known by -- an ISIN beside a
    venue's own code, a CUSIP beside a Bloomberg ticker. Structured for the
    same reason parties are: the identifier and its scheme always arrive
    together and mean nothing apart, so a reader wanting "the ISIN" should not
    be reassembling them out of a flat pair list by index.
    """

    component: str = "SecAltIDGrp"
    group: str = _NO_SECURITY_ALT_ID
    scoped: bool = True

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one alternative identifier is."""
        return SecurityAltID

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`SecurityAltID` opens an entry; the scheme qualifies it."""
        return (
            ("SecurityAltID", "SecurityAltID"),
            ("SecurityAltIDSource", "SecurityAltIDSource"),
        )


@dataclasses.dataclass(eq=False)
class Legs(ComponentGroup):
    """FIX's InstrmtLegGrp component, entry by entry.

    The legs of a multileg instrument: a spread's near and far, an option
    strategy's pair. The declaration walk reads `NoLegs <555>` off *every*
    component that declares it -- the order, quote and trade-capture variants
    wrap the same `InstrumentLeg` and add members of their own -- so a leg's
    contextual members are known member tags that stay residual rather
    than breaking the entry.
    """

    component: str = "InstrmtLegGrp"
    group: str = _NO_LEGS
    scoped: bool = True

    @classmethod
    @cache
    def into_row(cls) -> type:
        """The `@scalar` class one leg is."""
        return Leg

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """`LegSymbol` opens an entry; the members an instrument reads follow.

        Each pair is `(the column, the field it reads)`: the leg's own field
        under the generic name, because the nesting says it is a leg's.
        """
        return tuple(
            (name.removeprefix("Leg"), name)
            for name in (
                "LegSymbol",
                "LegSecurityID",
                "LegSecurityIDSource",
                "LegSecurityType",
                "LegCFICode",
                "LegSecurityExchange",
                "LegMaturityDate",
                "LegMaturityMonthYear",
                "LegStrikePrice",
                "LegPutOrCall",
                "LegContractMultiplier",
                "LegCurrency",
                "LegSide",
                "LegRatioQty",
            )
        )


def _kept(entries: Any, keep: Any) -> pyarrow.StructArray:
    """The entries a mask keeps, rebuilt with every part they carry."""
    compute = pyarrow.compute
    fields = [ENTRIES.value_type.field(index) for index in range(ENTRIES.value_type.num_fields)]
    return pyarrow.StructArray.from_arrays(
        [compute.filter(compute.struct_field(entries, field.name), keep) for field in fields],
        fields=fields,
    )


def _all(*conditions: Any) -> Any:
    """Element-wise conjunction, with no nullable truth leaking through."""
    found = pyarrow.compute.fill_null(conditions[0], False)
    for condition in conditions[1:]:
        found = pyarrow.compute.and_(found, pyarrow.compute.fill_null(condition, False))
    return found


def _first_by_parent(values: Any, parents: Any, matches: Any, row_ids: Any) -> pyarrow.Array:
    """First matching value per row, null where the row has none."""
    selected_parents = pyarrow.compute.filter(parents, matches)
    selected_values = pyarrow.compute.filter(values, matches)
    return pyarrow.compute.take(
        selected_values,
        pyarrow.compute.index_in(row_ids, value_set=selected_parents),
    )


def _take_or_zero(values: Any, indices: Any) -> Any:
    """Take nullable positions while giving absent positions a safe zero."""
    taken = pyarrow.compute.take(values, indices)
    return pyarrow.compute.fill_null(taken, 0).cast(pyarrow.int64())


def _contiguous_after(
    positions: Any, parents: Any, starts: Any, start_matches: Any, allowed: Any
) -> Any:
    """Allowed entries after a row's start, stopping at its first other tag."""
    compute = pyarrow.compute
    row_start = compute.take(starts, parents)
    after = compute.fill_null(compute.greater(positions, row_start), False)
    breaks = compute.and_(after, compute.invert(allowed))
    running = compute.cumulative_sum(breaks.cast(pyarrow.int64()))
    baseline = _take_or_zero(running, starts)
    return _all(
        after,
        allowed,
        compute.equal(running, compute.take(baseline, parents)),
        compute.invert(start_matches),
    )


def _contiguous_from(positions: Any, parents: Any, starts: Any, allowed: Any) -> Any:
    """Allowed entries from a row's delimiter through its first other tag."""
    compute = pyarrow.compute
    row_start = compute.take(starts, parents)
    at_or_after = compute.fill_null(compute.greater_equal(positions, row_start), False)
    breaks = compute.and_(at_or_after, compute.invert(allowed))
    running = compute.cumulative_sum(breaks.cast(pyarrow.int64()))
    baseline = _take_or_zero(running, starts)
    return _all(
        at_or_after,
        allowed,
        compute.equal(running, compute.take(baseline, parents)),
    )


def _first_for_party(
    values: Any,
    positions: Any,
    party_group: Any,
    matches: Any,
    party_groups: Any,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """First value and physical position of one tag in every party."""
    compute = pyarrow.compute
    groups = compute.filter(party_group, matches)
    where = compute.index_in(party_groups, value_set=groups)
    return compute.take(compute.filter(values, matches), where), compute.take(
        compute.filter(positions, matches), where
    )


def _positions_are(positions: Any, selected: Any) -> Any:
    """Which physical positions occur in a nullable selected position set."""
    valid = pyarrow.compute.filter(selected, pyarrow.compute.is_valid(selected))
    if not len(valid):
        return pyarrow.repeat(pyarrow.scalar(False), len(positions))
    return pyarrow.compute.is_in(positions, value_set=valid)
