"""Structured views of reusable FIX components."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.fields import scalar
from rekep.fields.arrays import build_list, build_map, sequence
from rekep.fix.columns import DECLARATIONS
from rekep.fix.quickfix import (
    SpecComponent,
    SpecComponentRef,
    SpecFieldRef,
    SpecGroup,
    SpecMember,
)


@scalar
class Party:
    """One entry of FIX's Parties component."""

    party_id: Annotated[str | None, DECLARATIONS[448]] = None
    """Party identifier."""

    party_id_source: Annotated[str | None, DECLARATIONS[447]] = None
    """Scheme or class of `PartyID`."""

    party_role: Annotated[int | None, DECLARATIONS[452]] = None
    """Role the party has in the transaction."""

    buffer: dict[str, str] | None = None
    """Unprojected party members under unique FIX names."""


PARTIES: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Party.into_field().arrow_type, nullable=False)
)

_VALUE = pyarrow.field("value", pyarrow.string(), nullable=False)
_FIX_TAGS: pyarrow.DataType = pyarrow.list_(
    pyarrow.field(
        "item",
        pyarrow.struct(
            [
                pyarrow.field("key", pyarrow.int32(), nullable=False),
                _VALUE,
            ]
        ),
        nullable=False,
    )
)

_NO_PARTY_IDS = "NoPartyIDs"
_FALLBACK: dict[int, str] = {
    448: "PartyID",
    447: "PartyIDSource",
    452: "PartyRole",
    802: "NoPartySubIDs",
    523: "PartySubID",
    803: "PartySubIDType",
}
_FALLBACK_COUNTS = {453}
_FALLBACK_PATHS = {
    523: ("NoPartySubIDs",),
    803: ("NoPartySubIDs",),
}
_UNSIGNED = r"^[0-9]{1,18}$"
_SIGNED = r"^[+-]?[0-9]{1,18}$"
_GROUP_STRIDE = 2**32


@dataclasses.dataclass(eq=False)
class Parties:
    """Extract FIX Parties groups without interpreting whole messages."""

    components: Mapping[str, SpecComponent] | Sequence[SpecComponent] | None = None
    names: Mapping[str, int] | None = None
    #: Retain the standalone extractor's legacy FIX tags when no registry is supplied.
    fallback: bool = True

    def __post_init__(self) -> None:
        """Hold stable declaration and name snapshots for repeated batches."""
        declared = self.components
        if isinstance(declared, Mapping):
            declared = tuple(declared.values())
        self.components = tuple(declared or ())
        self.names = dict(self.names or {})

    def into_arrow_arrays(self, tags: Any) -> tuple[Any, Any]:
        """Return `(parties, residual_tags)`, preserving row validity and order."""
        if isinstance(tags, pyarrow.ChunkedArray):
            parts = [self.into_arrow_arrays(chunk) for chunk in tags.chunks]
            return (
                pyarrow.chunked_array([found for found, _ in parts], type=PARTIES),
                pyarrow.chunked_array([rest for _, rest in parts], type=_FIX_TAGS),
            )
        if not isinstance(tags, pyarrow.Array) or tags.type != _FIX_TAGS:
            actual = getattr(tags, "type", type(tags).__name__)
            raise TypeError(f"Parties needs {_FIX_TAGS}, got {actual}")
        return self._extract(tags)

    def _extract(self, tags: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
        """One physical Arrow chunk through the component state machine."""
        compute = pyarrow.compute
        rows = len(tags)
        entries = compute.list_flatten(tags)
        if not len(entries):
            return pyarrow.nulls(rows, PARTIES), tags

        keys = compute.struct_field(entries, 0)
        relevant = compute.is_in(keys, value_set=self._relevant_array)
        if not compute.any(relevant, min_count=0).as_py():
            return pyarrow.nulls(rows, PARTIES), tags

        parents = compute.list_parent_indices(tags).cast(pyarrow.int64())
        values = compute.struct_field(entries, 1)
        positions = sequence(len(entries))
        row_ids = sequence(rows)

        count_match = compute.is_in(keys, value_set=self._count_array)
        count_occurrences = _counts(compute.filter(parents, count_match), rows)
        count_positions = _first_by_parent(positions, parents, count_match, row_ids)
        count_values = _first_by_parent(values, parents, count_match, row_ids)
        count_is_numeric = compute.fill_null(
            compute.match_substring_regex(count_values, _UNSIGNED), False
        )
        declared_count = compute.if_else(count_is_numeric, count_values, pyarrow.scalar("0")).cast(
            pyarrow.int64()
        )

        allowed = compute.is_in(keys, value_set=self._member_array)
        is_delimiter = compute.is_in(keys, value_set=self._tags_named("PartyID"))
        counted_inside = _contiguous_after(
            positions, parents, count_positions, count_match, allowed
        )
        counted_delimiters = _counts(
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
        inferred_delimiters = _counts(
            compute.filter(parents, compute.and_(inferred_inside, is_delimiter)), rows
        )
        inferred_valid = _all(
            compute.equal(count_occurrences, 0),
            compute.greater(inferred_delimiters, 0),
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
        party_positions = compute.filter(positions, delimiters)
        party_index = compute.index_in(party_group, value_set=party_groups)
        party_ids = compute.filter(values, delimiters)

        sources, source_positions = _first_for_party(
            values,
            positions,
            party_group,
            compute.and_(
                members,
                compute.is_in(keys, value_set=self._tags_named("PartyIDSource")),
            ),
            party_groups,
        )
        role_text, role_positions = _first_for_party(
            values,
            positions,
            party_group,
            compute.and_(
                members,
                compute.is_in(keys, value_set=self._tags_named("PartyRole")),
            ),
            party_groups,
        )
        role_valid = compute.fill_null(compute.match_substring_regex(role_text, _SIGNED), False)
        role_readable = compute.replace_substring_regex(role_text, r"^\+", "")
        roles = compute.if_else(
            role_valid,
            compute.if_else(role_valid, role_readable, pyarrow.scalar("0")).cast(pyarrow.int64()),
            pyarrow.scalar(None, pyarrow.int64()),
        )

        projected = compute.or_(
            delimiters,
            compute.or_(
                _positions_are(positions, source_positions),
                _positions_are(positions, compute.filter(role_positions, role_valid)),
            ),
        )
        buffered = compute.and_(members, compute.invert(projected))
        buffer_keys, bufferable = self._buffer_keys(
            keys, members, party_index, party_positions, buffered
        )
        buffered = compute.and_(buffered, bufferable)
        buffered_party = compute.filter(party_index, buffered).cast(pyarrow.int64())
        buffer_sizes = _counts(buffered_party, len(party_groups))
        buffer_type = Party.into_field().field("buffer").arrow_type
        assert buffer_type is not None
        buffers = build_map(
            buffer_type,
            buffer_sizes,
            compute.filter(buffer_keys, buffered),
            compute.filter(values, buffered),
            mask=compute.equal(buffer_sizes, 0),
        )

        party_struct = pyarrow.StructArray.from_arrays(
            [party_ids, sources, roles, buffers], fields=Party.into_field().arrow_fields
        )
        extracted = build_list(
            PARTIES,
            party_sizes,
            party_struct,
            mask=compute.invert(valid_rows),
        )

        remove = compute.or_(
            compute.or_(projected, buffered),
            compute.and_(count_match, compute.take(counted_valid, parents)),
        )
        keep = compute.invert(remove)
        residual_sizes = _counts(compute.filter(parents, keep), rows)
        residual_entries = pyarrow.StructArray.from_arrays(
            [compute.filter(keys, keep), compute.filter(values, keep)],
            fields=[_FIX_TAGS.value_type.field(0), _FIX_TAGS.value_type.field(1)],
        )
        residual = build_list(
            _FIX_TAGS,
            residual_sizes,
            residual_entries,
            mask=compute.is_null(tags) if tags.null_count else None,
        )
        return extracted, residual

    def _buffer_keys(
        self,
        keys: Any,
        members: Any,
        party_index: Any,
        party_positions: Any,
        buffered: Any,
    ) -> tuple[pyarrow.Array, pyarrow.Array]:
        """Canonical, occurrence-qualified names for retained member values."""
        compute = pyarrow.compute
        found = pyarrow.nulls(len(keys), pyarrow.string())
        bufferable = pyarrow.repeat(pyarrow.scalar(False), len(keys))
        declarations = dict.fromkeys(
            (name, self._member_paths.get(tag, ())) for tag, name in self._member_names.items()
        )
        for name, path in declarations:
            tags = pyarrow.array(
                sorted(
                    tag
                    for tag, found_name in self._member_names.items()
                    if found_name == name and self._member_paths.get(tag, ()) == path
                ),
                pyarrow.int32(),
            )
            matches = compute.and_(members, compute.is_in(keys, value_set=tags))
            if path:
                delimiters = pyarrow.array(
                    sorted(self._group_delimiters.get(path, ())), pyarrow.int32()
                )
                group_rank, occurrences = _nested_occurrences(
                    matches,
                    compute.and_(members, compute.is_in(keys, value_set=delimiters)),
                    party_index,
                    party_positions,
                )
                prefix = ".".join((*path[:-1], f"{path[-1]}["))
                base = compute.binary_join_element_wise(
                    pyarrow.scalar(prefix),
                    compute.subtract(group_rank, 1).cast(pyarrow.string()),
                    pyarrow.scalar(f"].{name}"),
                    "",
                )
                rendered = compute.if_else(
                    compute.equal(occurrences, 1),
                    base,
                    compute.binary_join_element_wise(
                        base,
                        pyarrow.scalar("["),
                        compute.subtract(occurrences, 1).cast(pyarrow.string()),
                        pyarrow.scalar("]"),
                        "",
                    ),
                )
                valid = compute.greater(group_rank, 0)
            else:
                occurrences = _occurrences(matches, party_index, party_positions)
                rendered = compute.if_else(
                    compute.equal(occurrences, 1),
                    pyarrow.scalar(name),
                    compute.binary_join_element_wise(
                        pyarrow.scalar(f"{name}["),
                        compute.subtract(occurrences, 1).cast(pyarrow.string()),
                        pyarrow.scalar("]"),
                        "",
                    ),
                )
                valid = pyarrow.repeat(pyarrow.scalar(True), len(keys))
            accepted = compute.and_(matches, compute.fill_null(valid, False))
            found = compute.if_else(accepted, rendered, found)
            bufferable = compute.or_(bufferable, accepted)
        if compute.any(_all(buffered, bufferable, compute.is_null(found)), min_count=0).as_py():
            raise ValueError("a declared Parties member has no stable FIX name")
        return found, bufferable

    @cached_property
    def _declaration(
        self,
    ) -> tuple[
        set[int],
        dict[int, str],
        dict[int, tuple[str, ...]],
        dict[tuple[str, ...], set[int]],
    ]:
        """Count tags, member names, and their component paths."""
        by_name = {component.name.lower(): component for component in self.components}
        explicit = "parties" in by_name
        legacy = not explicit and self.fallback
        counts = set() if not legacy else set(_FALLBACK_COUNTS)
        members = {} if not legacy else dict(_FALLBACK)
        paths = {} if not legacy else dict(_FALLBACK_PATHS)
        group_delimiters: dict[tuple[str, ...], set[int]] = {}
        name_tags = {str(name).lower(): int(tag) for name, tag in self.names.items()}

        if legacy:
            for tag, name in tuple(members.items()):
                mapped = name_tags.get(name.lower())
                if mapped is not None:
                    members.setdefault(mapped, name)
                    paths.setdefault(mapped, paths.get(tag, ()))
            mapped_count = name_tags.get(_NO_PARTY_IDS.lower())
            if mapped_count is not None:
                counts.add(mapped_count)
            group_delimiters[("NoPartySubIDs",)] = {
                tag
                for tag, name in members.items()
                if name == "PartySubID" and paths.get(tag) == ("NoPartySubIDs",)
            }

        def member_tags(member: SpecMember) -> tuple[int, ...]:
            found: list[int] = []
            declared = getattr(member, "tag", 0)
            if declared:
                found.append(int(declared))
            mapped = name_tags.get(member.name.lower())
            if mapped is not None and mapped not in found:
                found.append(mapped)
            return tuple(found)

        def add(member: SpecMember, path: tuple[str, ...]) -> None:
            for tag in member_tags(member):
                members.setdefault(tag, member.name)
                paths.setdefault(tag, path)

        def first_tags(declared: Sequence[SpecMember], seen: frozenset[str]) -> tuple[int, ...]:
            for member in declared:
                if isinstance(member, SpecFieldRef | SpecGroup):
                    tags = member_tags(member)
                    if tags:
                        return tags
                elif isinstance(member, SpecComponentRef):
                    key = member.name.lower()
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        tags = first_tags(nested.members, seen | {key})
                        if tags:
                            return tags
            return ()

        def visit(
            declared: Sequence[SpecMember], seen: frozenset[str], path: tuple[str, ...] = ()
        ) -> None:
            for member in declared:
                if isinstance(member, SpecFieldRef):
                    add(member, path)
                elif isinstance(member, SpecGroup):
                    add(member, path)
                    nested_path = (*path, member.name)
                    group_delimiters.setdefault(nested_path, set()).update(
                        first_tags(member.members, seen)
                    )
                    visit(member.members, seen, nested_path)
                elif isinstance(member, SpecComponentRef):
                    key = member.name.lower()
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        visit(nested.members, seen | {key}, path)

        def find(declared: Sequence[SpecMember], seen: frozenset[str]) -> None:
            for member in declared:
                if isinstance(member, SpecGroup) and member.name.lower() == _NO_PARTY_IDS.lower():
                    counts.update(member_tags(member))
                    visit(member.members, seen)
                elif isinstance(member, SpecComponentRef):
                    key = member.name.lower()
                    nested = by_name.get(key)
                    if nested is not None and key not in seen:
                        find(nested.members, seen | {key})

        for component in self.components:
            key = component.name.lower()
            if key == "parties":
                find(component.members, frozenset({key}))
                # Hand-written declarations sometimes omit the outer group.
                visit(
                    tuple(
                        member
                        for member in component.members
                        if not (
                            isinstance(member, SpecGroup)
                            and member.name.lower() == _NO_PARTY_IDS.lower()
                        )
                    ),
                    frozenset({key}),
                )
            else:
                find(component.members, frozenset({key}))
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
    def _member_array(self) -> pyarrow.Array:
        return pyarrow.array(sorted(self._member_names), pyarrow.int32())

    @cached_property
    def _relevant_array(self) -> pyarrow.Array:
        """Tags that can start or belong to Parties."""
        return pyarrow.array(
            sorted(self._declaration[0] | set(self._member_names)), pyarrow.int32()
        )

    def _tags_named(self, name: str) -> pyarrow.Array:
        """Every declared tag carrying one canonical member name."""
        wanted = name.lower()
        return pyarrow.array(
            sorted(tag for tag, found in self._member_names.items() if found.lower() == wanted),
            pyarrow.int32(),
        )


def _all(*conditions: Any) -> Any:
    """Element-wise conjunction, with no nullable truth leaking through."""
    found = pyarrow.compute.fill_null(conditions[0], False)
    for condition in conditions[1:]:
        found = pyarrow.compute.and_(found, pyarrow.compute.fill_null(condition, False))
    return found


def _counts(ids: Any, size: int) -> pyarrow.Array:
    """Counts for dense ids `0..size`, including groups that have no value."""
    compute = pyarrow.compute
    if not len(ids):
        return pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), size)
    counted = compute.value_counts(ids)
    where = compute.index_in(sequence(size), value_set=counted.field("values"))
    return compute.fill_null(compute.take(counted.field("counts"), where), 0).cast(pyarrow.int64())


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


def _occurrences(matches: Any, party_index: Any, party_positions: Any) -> pyarrow.Array:
    """One-based occurrence of a member tag inside each party."""
    compute = pyarrow.compute
    running = compute.cumulative_sum(matches.cast(pyarrow.int64()))
    baseline = compute.take(running, party_positions)
    return compute.subtract(running, compute.take(baseline, party_index))


def _nested_occurrences(
    matches: Any, delimiters: Any, party_index: Any, party_positions: Any
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Nested group rank and member occurrence within that group."""
    compute = pyarrow.compute
    rank = _occurrences(delimiters, party_index, party_positions)
    running = compute.cumulative_sum(matches.cast(pyarrow.int64()))
    before = compute.subtract(running, matches.cast(pyarrow.int64()))
    group = compute.add(compute.multiply(party_index.cast(pyarrow.int64()), _GROUP_STRIDE), rank)
    starts = compute.filter(group, delimiters)
    baselines = compute.filter(before, delimiters)
    baseline = compute.take(baselines, compute.index_in(group, value_set=starts))
    return rank, compute.subtract(running, baseline)
