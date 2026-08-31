"""Orchestra and QuickFIX documents as one Arrow-oriented source registry."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal, Self
from xml.etree import ElementTree

import pyarrow

from rekep.fields import Field, column_name
from rekep.fields.metadata import Alias, FixFieldValue
from rekep.fix import quickfix
from rekep.fix.fields import FIX_SCALARS, arrow_type_of, documented_utc, fix_field

DefinitionKind = Literal["field", "component", "group"]
BlockKind = Literal["message", "component", "group"]

_SPACE = re.compile(r"\s+")
_VALUE = re.compile(r"^\s*([^\s:=\-]+)\s*(?:-|=|:)\s*(.+?)\s*$")
_STANDARDIZED = re.compile(
    r"ADDED\s+TO\s+FIX\b.*?\bTAG\s*:\s*(\d+)\s*\(([^)]+)\)", re.IGNORECASE | re.DOTALL
)
_TEXTUAL_NUMBER = re.compile(r"\balpha[ -]?numeric\b", re.IGNORECASE)
_UNSAFE_XML = re.compile(rb"<!\s*(?:doctype|entity)\b", re.IGNORECASE)


@dataclasses.dataclass(frozen=True)
class SourceProvenance:
    """The exact source artifact from which definitions were read."""

    source_id: str
    namespace: str
    version: str
    url: str
    format: str
    checksum: str
    license_url: str = ""
    protocol_version: str = ""

    def into_dict(self) -> dict[str, str]:
        """The deterministic source-manifest entry."""
        return {
            "source_id": self.source_id,
            "namespace": self.namespace,
            "version": self.version,
            "url": self.url,
            "format": self.format,
            "checksum": self.checksum,
            "license_url": self.license_url,
        }

    @classmethod
    def for_bytes(
        cls,
        content: bytes,
        *,
        source_id: str = "memory",
        namespace: str = "standard",
        version: str = "",
        url: str = "",
        format: str = "orchestra",
        license_url: str = "",
        protocol_version: str = "",
    ) -> Self:
        """Provenance for a complete in-memory source file."""
        checksum = f"sha256:{hashlib.sha256(content).hexdigest()}"
        return cls(
            source_id,
            namespace,
            version,
            url,
            format,
            checksum,
            license_url,
            protocol_version,
        )


@dataclasses.dataclass(frozen=True)
class Pedigree:
    """The standard lifecycle attributes shared by Orchestra entities."""

    added: str = ""
    added_ep: str = ""
    updated: str = ""
    updated_ep: str = ""
    deprecated: str = ""
    deprecated_ep: str = ""
    replaced: str = ""
    replaced_ep: str = ""
    replaced_by_field: int | None = None
    issue: str = ""
    last_modified: str = ""
    supported: str = ""

    def into_dict(self) -> dict[str, str | int]:
        """Only the pedigree facts the source states."""
        return {
            key: value
            for key, value in (
                ("added", self.added),
                ("added_ep", self.added_ep),
                ("updated", self.updated),
                ("updated_ep", self.updated_ep),
                ("deprecated", self.deprecated),
                ("deprecated_ep", self.deprecated_ep),
                ("replaced", self.replaced),
                ("replaced_ep", self.replaced_ep),
                ("replaced_by_field", self.replaced_by_field),
                ("issue", self.issue),
                ("last_modified", self.last_modified),
                ("supported", self.supported),
            )
            if value not in (None, "")
        }


@dataclasses.dataclass(frozen=True)
class MappedDatatype:
    """One external type-system mapping declared for an Orchestra datatype."""

    standard: str
    base: str
    pattern: str = ""


@dataclasses.dataclass(frozen=True)
class SourceDatatype:
    """One named Orchestra datatype and the type from which it derives."""

    name: str
    base_type: str = ""
    scenario: str = ""
    mappings: tuple[MappedDatatype, ...] = ()
    description: str = ""
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)


@dataclasses.dataclass(frozen=True)
class SourceValue:
    """One wire value in a code set or a field-local enumeration."""

    value: str
    name: str = ""
    description: str = ""
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)


@dataclasses.dataclass(frozen=True)
class SourceCodeSet:
    """A named enumeration and its underlying protocol datatype."""

    id: int
    name: str
    datatype: str
    values: tuple[SourceValue, ...] = ()
    scenario: str = ""
    description: str = ""
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)


@dataclasses.dataclass(frozen=True)
class TypeInference:
    """A source datatype resolved to the compact Arrow storage contract."""

    original: str
    datatype: str
    arrow: pyarrow.DataType
    fallback: str = ""


@dataclasses.dataclass(frozen=True)
class SourceReference:
    """The standard field which replaced an earlier definition."""

    tag: int
    name: str = ""


@dataclasses.dataclass(frozen=True)
class SourceField:
    """One tag definition with its source reading and Arrow projection."""

    tag: int
    name: str
    original_datatype: str
    datatype: str
    arrow_type: pyarrow.DataType
    source_name: str = ""
    description: str = ""
    values: tuple[SourceValue, ...] = ()
    aliases: tuple[str, ...] = ()
    scenarios: tuple[str, ...] = ()
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)
    replacement: SourceReference | None = None
    type_readings: tuple[str, ...] = ()
    fallback: str = ""
    provenance: SourceProvenance = dataclasses.field(
        default_factory=lambda: SourceProvenance.for_bytes(b"")
    )

    def into_field(self) -> Field:
        """The existing registry `Field`, with source facts under `fix:` metadata."""
        metadata = {
            "fix:namespace": self.provenance.namespace,
            "fix:source": self.provenance.source_id,
            "fix:source-version": self.provenance.version,
            "fix:source-url": self.provenance.url,
            "fix:source-format": self.provenance.format,
            "fix:source-checksum": self.provenance.checksum,
            "fix:original-type": self.original_datatype,
        }
        if self.provenance.license_url:
            metadata["fix:source-license"] = self.provenance.license_url
        if self.provenance.protocol_version:
            metadata["fix:protocol-version"] = self.provenance.protocol_version
        if self.source_name:
            metadata["fix:source-name"] = self.source_name
        if stated := self.pedigree.into_dict():
            metadata["fix:pedigree"] = json.dumps(stated, separators=(",", ":"), sort_keys=True)
        if self.replacement is not None:
            metadata["fix:replacement-tag"] = str(self.replacement.tag)
            if self.replacement.name:
                metadata["fix:replacement-name"] = self.replacement.name
        if self.fallback:
            metadata["fix:type-fallback"] = self.fallback
        built = fix_field(
            self.name,
            self.tag,
            self.datatype,
            description=self.description,
            version=self.provenance.protocol_version or self.provenance.version,
            metadata=metadata,
        )
        dtype = self.arrow_type
        if pyarrow.types.is_timestamp(dtype) and documented_utc(self.description):
            dtype = pyarrow.timestamp(dtype.unit, tz="UTC")
        built = dataclasses.replace(built, dtype=dtype)
        built.fix.enumerated = tuple(
            FixFieldValue(
                value=value.value,
                meaning=value.description,
                aliases=(value.name,) if value.name and value.name != value.value else (),
            )
            for value in self.values
        )
        if self.aliases:
            built.fix.named_aliases = tuple(
                Alias(name=name, source=self.provenance.source_id) for name in self.aliases
            )
        return built


@dataclasses.dataclass(frozen=True)
class SourceMember:
    """One field, component, or group membership in wire order."""

    kind: DefinitionKind
    id: int
    name: str = ""
    required: bool = False
    scenario: str = ""
    members: tuple[SourceMember, ...] = ()
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)


@dataclasses.dataclass(frozen=True)
class SourceBlock:
    """One message, component, or repeating-group declaration."""

    kind: BlockKind
    id: int
    name: str
    members: tuple[SourceMember, ...] = ()
    msg_type: str = ""
    scenario: str = ""
    count_id: int | None = None
    description: str = ""
    pedigree: Pedigree = dataclasses.field(default_factory=Pedigree)


@dataclasses.dataclass(frozen=True)
class SourceConflict:
    """A disputed same-source fact and the safe reading retained for it."""

    key: str
    part: str
    readings: tuple[str, ...]
    resolution: str


@dataclasses.dataclass(frozen=True)
class SourceRegistry:
    """Every definition parsed from one complete source artifact."""

    source: SourceProvenance
    repository_name: str
    repository_version: str
    metadata: tuple[tuple[str, str], ...] = ()
    datatypes: tuple[SourceDatatype, ...] = ()
    code_sets: tuple[SourceCodeSet, ...] = ()
    fields: tuple[SourceField, ...] = ()
    messages: tuple[SourceBlock, ...] = ()
    components: tuple[SourceBlock, ...] = ()
    groups: tuple[SourceBlock, ...] = ()
    conflicts: tuple[SourceConflict, ...] = ()

    @property
    def declaration_version(self) -> str:
        """The negotiated FIX version declared by fields and components."""
        return self.source.protocol_version or self.source.version

    @property
    def fallbacks(self) -> tuple[SourceField, ...]:
        """Fields stored as strings because their declared type was not reliable."""
        return tuple(field for field in self.fields if field.fallback)

    def field(self, tag: int) -> SourceField | None:
        """One source definition by numeric tag."""
        return next((field for field in self.fields if field.tag == tag), None)

    def declarations(self) -> dict[str, Field]:
        """Messages and components in the existing unexpanded `Field` shape."""
        fields = {field.tag: field for field in self.fields}
        components = _preferred_blocks(self.components)
        groups = _preferred_blocks(self.groups)
        declarations: dict[str, Field] = {}
        for declared in (*components.values(), *_preferred_blocks(self.messages).values()):
            if declared.name in declarations:
                raise ValueError(f"source block {declared.name!r} is declared twice")
            declaration = quickfix.block(
                declared.name,
                _members_into_fields(
                    declared.members, fields, components, groups, (declared.name,)
                ),
                declared.msg_type,
            )
            if self.declaration_version:
                declaration.fix["version"] = self.declaration_version
            declarations[declared.name] = declaration
        return declarations

    def group_declarations(self) -> dict[str, Field]:
        """Top-level Orchestra groups as the list `Field` each one declares."""
        fields = {field.tag: field for field in self.fields}
        components = _preferred_blocks(self.components)
        groups = _preferred_blocks(self.groups)
        found: dict[str, Field] = {}
        for declared in groups.values():
            count = fields.get(declared.count_id or -1)
            if count is None:
                raise ValueError(f"source group {declared.name!r} has no known NumInGroup field")
            group = quickfix.group_member(
                count.name,
                count.tag,
                _members_into_fields(
                    declared.members,
                    fields,
                    components,
                    groups,
                    (declared.name,),
                    frozenset({declared.id}),
                ),
            )
            if self.declaration_version:
                group.fix["version"] = self.declaration_version
            found[declared.name] = group
        return found


def infer_arrow_type(
    datatype: str,
    datatypes: Sequence[SourceDatatype] = (),
    code_sets: Sequence[SourceCodeSet] = (),
    *,
    description: str = "",
    disputed: bool = False,
) -> TypeInference:
    """Resolve a FIX, code-set, or derived datatype; uncertain values stay strings."""
    return _infer_arrow_type(
        datatype,
        _preferred_datatypes(datatypes),
        _preferred_code_sets(code_sets),
        description=description,
        disputed=disputed,
    )


def _infer_arrow_type(
    datatype: str,
    base_datatypes: Mapping[str, SourceDatatype],
    base_code_sets: Mapping[str, SourceCodeSet],
    *,
    description: str,
    disputed: bool,
) -> TypeInference:
    """Resolve through the indexes shared by every field in one source file."""
    original = str(datatype or "").strip()
    if disputed:
        return TypeInference(original, "string", pyarrow.string(), "conflicting source datatypes")
    name = original
    seen: set[str] = set()
    while name:
        key = column_name(name)
        if key in seen:
            return TypeInference(original, "string", pyarrow.string(), "recursive source datatype")
        seen.add(key)
        code_set = base_code_sets.get(key)
        if code_set is not None:
            name = code_set.datatype
            continue
        if name.strip().casefold() in FIX_SCALARS:
            resolved = name
            break
        declared = base_datatypes.get(key)
        if declared is None:
            return TypeInference(original, "string", pyarrow.string(), "unknown source datatype")
        if declared.base_type:
            name = declared.base_type
            continue
        mapped = _mapped_fix_type(declared.mappings)
        if not mapped:
            return TypeInference(original, "string", pyarrow.string(), "unmapped source datatype")
        resolved = mapped
        break
    else:
        return TypeInference(original, "string", pyarrow.string(), "missing source datatype")
    dtype = arrow_type_of(resolved)
    if _TEXTUAL_NUMBER.search(description) and (
        pyarrow.types.is_integer(dtype) or pyarrow.types.is_floating(dtype)
    ):
        return TypeInference(
            original, "string", pyarrow.string(), "prose permits alphanumeric values"
        )
    return TypeInference(original, resolved, dtype)


def parse_orchestra(
    document: bytes | str,
    provenance: SourceProvenance | None = None,
) -> SourceRegistry:
    """Parse one complete Orchestra repository without depending on its namespace year."""
    content = document.encode() if isinstance(document, str) else bytes(document)
    root = _xml_root(content, "Orchestra")
    if _local(root.tag) != "repository":
        raise ValueError("an Orchestra document must have a repository root")
    repository_name = str(root.get("name") or "").strip()
    repository_version = str(root.get("version") or "").strip()
    source = provenance or SourceProvenance.for_bytes(
        content,
        version=repository_version,
        protocol_version=_orchestra_protocol_version(repository_version),
    )
    if repository_version and not source.version:
        source = dataclasses.replace(source, version=repository_version)
    if not source.protocol_version:
        source = dataclasses.replace(
            source, protocol_version=_orchestra_protocol_version(repository_version)
        )
    datatypes = tuple(
        _datatype(element)
        for section in _sections(root, "datatypes")
        for element in _children(section, "datatype")
    )
    code_sets = tuple(
        _code_set(element)
        for section in _sections(root, "codeSets")
        for element in _children(section, "codeSet")
    )
    raw_fields = [
        _raw_field(element)
        for section in _sections(root, "fields")
        for element in _children(section, "field")
    ]
    fields, conflicts = _source_fields(raw_fields, datatypes, code_sets, source)
    groups = tuple(
        _block(element, "group")
        for section in _sections(root, "groups")
        for element in _children(section, "group")
    )
    components = tuple(
        _block(element, "component")
        for section in _sections(root, "components")
        for element in _children(section, "component")
    )
    messages = tuple(
        _block(element, "message")
        for section in _sections(root, "messages")
        for element in _children(section, "message")
    )
    return SourceRegistry(
        source=source,
        repository_name=repository_name,
        repository_version=repository_version,
        metadata=_metadata(root),
        datatypes=datatypes,
        code_sets=code_sets,
        fields=fields,
        messages=messages,
        components=components,
        groups=groups,
        conflicts=conflicts,
    )


def parse_quickfix(
    document: bytes | str,
    provenance: SourceProvenance | None = None,
) -> SourceRegistry:
    """Parse QuickFIX XML into the same definitions returned for Orchestra."""
    content = document.encode() if isinstance(document, str) else bytes(document)
    root = _xml_root(content, "QuickFIX")
    if _local(root.tag) != "fix":
        raise ValueError("a QuickFIX document must have a fix root")
    version = _quickfix_version(root)
    source = provenance or SourceProvenance.for_bytes(
        content, version=version, format="quickfix", protocol_version=version
    )
    if version and not source.version:
        source = dataclasses.replace(source, version=version)
    if version and not source.protocol_version:
        source = dataclasses.replace(source, protocol_version=version)
    specified = quickfix.parse_spec(content.decode("utf-8"))
    if not specified:
        raise ValueError("a QuickFIX document declares no fields")
    fields = tuple(
        SourceField(
            tag=field.tag,
            name=field.name,
            original_datatype=field.datatype,
            datatype=(inferred := infer_arrow_type(field.datatype)).datatype,
            arrow_type=inferred.arrow,
            values=tuple(
                SourceValue(value=value, name=name) for value, name in field.values.items()
            ),
            type_readings=(field.datatype,),
            fallback=inferred.fallback,
            provenance=source,
        )
        for field in specified.values()
    )
    parsed = quickfix.parse_declarations(content.decode("utf-8"))
    blocks = tuple(_quickfix_block(declared) for declared in parsed.values())
    groups: dict[int, SourceBlock] = {}
    for block in blocks:
        _collect_quickfix_groups(block.members, groups)
    return SourceRegistry(
        source=source,
        repository_name=str(root.get("type") or "FIX"),
        repository_version=version,
        fields=fields,
        messages=tuple(block for block in blocks if block.kind == "message"),
        components=tuple(block for block in blocks if block.kind == "component"),
        groups=tuple(groups.values()),
    )


@dataclasses.dataclass(frozen=True)
class _RawField:
    tag: int
    name: str
    datatype: str
    description: str
    values: tuple[SourceValue, ...]
    scenario: str
    pedigree: Pedigree


def _source_fields(
    raw_fields: Sequence[_RawField],
    datatypes: Sequence[SourceDatatype],
    code_sets: Sequence[SourceCodeSet],
    source: SourceProvenance,
) -> tuple[tuple[SourceField, ...], tuple[SourceConflict, ...]]:
    """Collapse same-tag scenarios while retaining and reporting every reading."""
    grouped: dict[int, list[_RawField]] = defaultdict(list)
    for field in raw_fields:
        grouped[field.tag].append(field)
    datatype_by_name = _preferred_datatypes(datatypes)
    code_set_by_name = _preferred_code_sets(code_sets)
    fields: list[SourceField] = []
    conflicts: list[SourceConflict] = []
    for tag, variants in grouped.items():
        preferred = min(
            enumerate(variants), key=lambda item: (_scenario_rank(item[1].scenario), item[0])
        )[1]
        readings = tuple(dict.fromkeys(field.datatype for field in variants if field.datatype))
        disputed = len({column_name(reading) for reading in readings}) > 1
        inferred = _infer_arrow_type(
            preferred.datatype,
            datatype_by_name,
            code_set_by_name,
            description=preferred.description,
            disputed=disputed,
        )
        if disputed:
            conflicts.append(SourceConflict(str(tag), "datatype", readings, "string"))
        aliases = tuple(
            dict.fromkeys(field.name for field in variants if field.name != preferred.name)
        )
        name, standardized_alias, replacement = _standardized_name(
            tag, preferred.name, preferred.description
        )
        if standardized_alias:
            aliases = tuple(dict.fromkeys((*aliases, standardized_alias)))
        code_set = code_set_by_name.get(column_name(preferred.datatype))
        values = code_set.values if code_set is not None else preferred.values
        if not values:
            values = _prose_values(preferred.description)
        values = _normalized_values(tag, name, values)
        fields.append(
            SourceField(
                tag=tag,
                name=name,
                original_datatype=preferred.datatype,
                datatype=inferred.datatype,
                arrow_type=inferred.arrow,
                source_name=preferred.name if name != preferred.name else "",
                description=preferred.description,
                values=values,
                aliases=aliases,
                scenarios=tuple(dict.fromkeys(field.scenario or "base" for field in variants)),
                pedigree=preferred.pedigree,
                replacement=replacement,
                type_readings=readings,
                fallback=inferred.fallback,
                provenance=source,
            )
        )
    return _unique_field_names(fields), tuple(conflicts)


def _datatype(element: ElementTree.Element) -> SourceDatatype:
    return SourceDatatype(
        name=_required(element, "name", "datatype"),
        base_type=str(element.get("baseType") or ""),
        scenario=str(element.get("scenario") or ""),
        mappings=tuple(
            MappedDatatype(
                standard=str(mapped.get("standard") or ""),
                base=str(mapped.get("base") or ""),
                pattern=str(mapped.get("pattern") or ""),
            )
            for mapped in _children(element, "mappedDatatype")
        ),
        description=_description(element),
        pedigree=_pedigree(element),
    )


def _code_set(element: ElementTree.Element) -> SourceCodeSet:
    return SourceCodeSet(
        id=_integer(element, "id", "code set"),
        name=_required(element, "name", "code set"),
        datatype=_required(element, "type", "code set"),
        values=tuple(
            SourceValue(
                value=_required(code, "value", "code"),
                name=str(code.get("name") or ""),
                description=_description(code),
                pedigree=_pedigree(code),
            )
            for code in _children(element, "code")
        ),
        scenario=str(element.get("scenario") or ""),
        description=_description(element),
        pedigree=_pedigree(element),
    )


def _raw_field(element: ElementTree.Element) -> _RawField:
    return _RawField(
        tag=_integer(element, "id", "field"),
        name=_required(element, "name", "field"),
        datatype=_required(element, "type", "field"),
        description=_description(element),
        values=tuple(
            SourceValue(
                value=_required(value, "value", "field value"),
                name=str(value.get("name") or value.get("symbol") or ""),
                description=_description(value),
                pedigree=_pedigree(value),
            )
            for value in element
            if _local(value.tag) in {"value", "code"}
        ),
        scenario=str(element.get("scenario") or ""),
        pedigree=_pedigree(element),
    )


def _block(element: ElementTree.Element, kind: BlockKind) -> SourceBlock:
    owner = next((child for child in element if _local(child.tag) == "structure"), element)
    count = next((child for child in element if _local(child.tag) == "numInGroup"), None)
    return SourceBlock(
        kind=kind,
        id=_integer(element, "id", kind),
        name=_required(element, "name", kind),
        members=tuple(
            _member(child)
            for child in owner
            if _local(child.tag) in {"fieldRef", "componentRef", "groupRef"}
        ),
        msg_type=str(element.get("msgType") or element.get("msgtype") or ""),
        scenario=str(element.get("scenario") or ""),
        count_id=_integer(count, "id", "group count") if count is not None else None,
        description=_description(element),
        pedigree=_pedigree(element),
    )


def _member(element: ElementTree.Element) -> SourceMember:
    kind = _local(element.tag).removesuffix("Ref")
    if kind not in {"field", "component", "group"}:
        raise ValueError(f"unknown Orchestra member kind {kind!r}")
    return SourceMember(
        kind=kind,  # type: ignore[arg-type]
        id=_integer(element, "id", f"{kind} reference"),
        name=str(element.get("name") or ""),
        required=str(element.get("presence") or "").casefold() == "required",
        scenario=str(element.get("scenario") or ""),
        pedigree=_pedigree(element),
    )


def _quickfix_block(declared: Field) -> SourceBlock:
    return SourceBlock(
        kind="message" if declared.fix.msgtype else "component",
        id=0,
        name=declared.name,
        members=tuple(_quickfix_member(member) for member in quickfix.members_of(declared)),
        msg_type=declared.fix.msgtype,
    )


def _quickfix_member(member: Field) -> SourceMember:
    required = member.nullable is False
    if quickfix.is_group(member):
        return SourceMember(
            "group",
            int(member.fix.tag),
            member.name,
            required,
            members=tuple(
                _quickfix_member(nested)
                for nested in quickfix.members_of(quickfix.entry_of(member))
            ),
        )
    if quickfix.is_reference(member):
        return SourceMember("component", 0, member.name, required)
    return SourceMember("field", int(member.fix.tag), member.name, required)


def _collect_quickfix_groups(
    members: Sequence[SourceMember], found: dict[int, SourceBlock]
) -> None:
    for member in members:
        if member.kind != "group":
            continue
        found.setdefault(
            member.id,
            SourceBlock("group", member.id, member.name, member.members, count_id=member.id),
        )
        _collect_quickfix_groups(member.members, found)


def _members_into_fields(
    members: Sequence[SourceMember],
    fields: Mapping[int, SourceField],
    components: Mapping[int, SourceBlock],
    groups: Mapping[int, SourceBlock],
    path: tuple[str, ...],
    group_path: frozenset[int] = frozenset(),
) -> tuple[Field, ...]:
    built: list[Field] = []
    for member in members:
        if member.kind == "field":
            field = fields.get(member.id)
            if field is None:
                raise ValueError(
                    f"source block {'.'.join(path)!r} references unknown field {member.id}"
                )
            built.append(quickfix.field_member(field.name, field.tag, required=member.required))
            continue
        if member.kind == "component":
            block = _referenced_block(member, components)
            if block is None:
                raise ValueError(
                    f"source block {'.'.join(path)!r} references unknown component "
                    f"{member.name or member.id!r}"
                )
            built.append(quickfix.reference_member(block.name, required=member.required))
            continue
        if member.members:
            count = fields.get(member.id)
            nested = member.members
            group_id = member.id
        else:
            block = _referenced_block(member, groups)
            if block is None:
                raise ValueError(
                    f"source block {'.'.join(path)!r} references unknown group "
                    f"{member.name or member.id!r}"
                )
            count = fields.get(block.count_id or -1)
            nested = block.members
            group_id = block.id
        if count is None:
            raise ValueError(f"source group {member.name or member.id!r} has no known count field")
        if group_id in group_path:
            raise ValueError(f"recursive source group {member.name or member.id!r}")
        built.append(
            quickfix.group_member(
                count.name,
                count.tag,
                _members_into_fields(
                    nested,
                    fields,
                    components,
                    groups,
                    (*path, count.name),
                    group_path | {group_id},
                ),
                required=member.required,
            )
        )
    return tuple(built)


def _referenced_block(
    member: SourceMember, blocks: Mapping[int, SourceBlock]
) -> SourceBlock | None:
    if member.id and member.id in blocks:
        return blocks[member.id]
    folded = column_name(member.name)
    return next((block for block in blocks.values() if column_name(block.name) == folded), None)


def _preferred_blocks(blocks: Sequence[SourceBlock]) -> dict[int, SourceBlock]:
    grouped: dict[int, list[tuple[int, SourceBlock]]] = defaultdict(list)
    for index, block in enumerate(blocks):
        # QuickFIX has names but no component ids; a stable negative key keeps them distinct.
        key = block.id or -(index + 1)
        grouped[key].append((index, block))
    return {
        key: min(entries, key=lambda item: (_scenario_rank(item[1].scenario), item[0]))[1]
        for key, entries in grouped.items()
    }


def _preferred_datatypes(
    datatypes: Sequence[SourceDatatype],
) -> dict[str, SourceDatatype]:
    found: dict[str, tuple[int, SourceDatatype]] = {}
    for index, datatype in enumerate(datatypes):
        key = column_name(datatype.name)
        candidate = (_scenario_rank(datatype.scenario) * len(datatypes) + index, datatype)
        if key not in found or candidate[0] < found[key][0]:
            found[key] = candidate
    return {key: value for key, (_, value) in found.items()}


def _preferred_code_sets(code_sets: Sequence[SourceCodeSet]) -> dict[str, SourceCodeSet]:
    found: dict[str, tuple[int, SourceCodeSet]] = {}
    for index, code_set in enumerate(code_sets):
        key = column_name(code_set.name)
        candidate = (_scenario_rank(code_set.scenario) * len(code_sets) + index, code_set)
        if key not in found or candidate[0] < found[key][0]:
            found[key] = candidate
    return {key: value for key, (_, value) in found.items()}


def _mapped_fix_type(mappings: Sequence[MappedDatatype]) -> str:
    xml = next(
        (
            mapping.base.casefold().removeprefix("xs:")
            for mapping in mappings
            if mapping.standard.casefold() == "xml"
        ),
        "",
    )
    if xml in {"boolean"}:
        return "boolean"
    if xml in {"decimal", "double", "float"}:
        return "float"
    if xml in {
        "byte",
        "short",
        "int",
        "integer",
        "long",
        "negativeinteger",
        "nonnegativeinteger",
        "nonpositiveinteger",
        "positiveinteger",
        "unsignedbyte",
        "unsignedint",
        "unsignedlong",
        "unsignedshort",
    }:
        return "int"
    if xml in {"date", "datetime", "time"}:
        return "timestamp"
    if xml in {"base64binary", "hexbinary"}:
        return "data"
    if xml:
        return "string"
    return ""


def _metadata(root: ElementTree.Element) -> tuple[tuple[str, str], ...]:
    section = next(iter(_sections(root, "metadata")), None)
    if section is None:
        return ()
    found: list[tuple[str, str]] = []
    for element in section.iter():
        if element is section:
            continue
        text = _text(element)
        if text:
            found.append((_local(element.tag), text))
    return tuple(found)


def _description(element: ElementTree.Element) -> str:
    documented: list[tuple[str, str]] = []
    for annotation in _children(element, "annotation"):
        for child in annotation.iter():
            if _local(child.tag) != "documentation":
                continue
            text = _documentation_text(child)
            if text:
                documented.append((str(child.get("purpose") or ""), text))
    if not documented:
        return ""
    synopsis = next((text for purpose, text in documented if purpose.casefold() == "synopsis"), "")
    return synopsis or documented[0][1]


def _prose_values(description: str) -> tuple[SourceValue, ...]:
    marker = re.search(r"\bValid\s+Values?\b", description, re.IGNORECASE)
    if marker is None:
        return ()
    values: list[SourceValue] = []
    for line in description[marker.end() :].splitlines():
        match = _VALUE.match(line)
        if match is None:
            continue
        value, meaning = match.groups()
        values.append(SourceValue(value=value, description=_SPACE.sub(" ", meaning).strip()))
    return tuple(values)


def _standardized_name(
    tag: int, name: str, description: str
) -> tuple[str, str, SourceReference | None]:
    standardized = _STANDARDIZED.search(description)
    if standardized is None:
        return name, "", None
    replacement_tag, replacement_name = standardized.groups()
    replacement_name = _SPACE.sub(" ", replacement_name).strip()
    replacement = SourceReference(int(replacement_tag), replacement_name)
    if (
        tag == 9001
        and int(replacement_tag) == 210
        and column_name(replacement_name) == "maxshow"
        and column_name(name) != "maxshow"
    ):
        return replacement_name, name, replacement
    return name, "", replacement


def _normalized_values(
    tag: int, name: str, values: tuple[SourceValue, ...]
) -> tuple[SourceValue, ...]:
    """Keep registered UDF support meanings stable across source prose revisions."""
    if tag != 9003 or column_name(name) != "udfsupportindicator":
        return values
    meanings = {
        "1": "Supports UDFs in the message",
        "2": "Supports UDFs in repeating groups",
    }
    return tuple(
        dataclasses.replace(value, description=meanings.get(value.value, value.description))
        for value in values
    )


def _unique_field_names(fields: Sequence[SourceField]) -> tuple[SourceField, ...]:
    """Make every folded source name address one tag without dropping definitions."""
    used: set[str] = set()
    unique: list[SourceField] = []
    for field in sorted(fields, key=lambda candidate: candidate.tag):
        folded = column_name(field.name)
        if folded not in used:
            used.add(folded)
            unique.append(field)
            continue
        source_name = field.source_name or field.name
        index = 1
        candidate = f"{field.name}{field.tag}"
        while column_name(candidate) in used:
            index += 1
            candidate = f"{field.name}{field.tag}{index}"
        used.add(column_name(candidate))
        unique.append(dataclasses.replace(field, name=candidate, source_name=source_name))
    return tuple(unique)


def _pedigree(element: ElementTree.Element) -> Pedigree:
    replaced_by = str(element.get("replacedByField") or "")
    return Pedigree(
        added=str(element.get("added") or ""),
        added_ep=str(element.get("addedEP") or ""),
        updated=str(element.get("updated") or ""),
        updated_ep=str(element.get("updatedEP") or ""),
        deprecated=str(element.get("deprecated") or ""),
        deprecated_ep=str(element.get("deprecatedEP") or ""),
        replaced=str(element.get("replaced") or ""),
        replaced_ep=str(element.get("replacedEP") or ""),
        replaced_by_field=int(replaced_by) if replaced_by.isdigit() else None,
        issue=str(element.get("issue") or ""),
        last_modified=str(element.get("lastModified") or ""),
        supported=str(element.get("supported") or ""),
    )


def _xml_root(content: bytes, owner: str) -> ElementTree.Element:
    if not content.strip():
        raise ValueError(f"an empty file is not a {owner} document")
    if _UNSAFE_XML.search(content):
        raise ValueError(f"a {owner} document cannot declare XML entities")
    try:
        return ElementTree.fromstring(content)  # noqa: S314
    except (ElementTree.ParseError, UnicodeError) as error:
        raise ValueError(f"malformed {owner} XML: {error}") from error


def _sections(root: ElementTree.Element, name: str) -> Iterable[ElementTree.Element]:
    return (child for child in root if _local(child.tag) == name)


def _children(element: ElementTree.Element, name: str) -> Iterable[ElementTree.Element]:
    return (child for child in element if _local(child.tag) == name)


def _local(tag: str) -> str:
    return str(tag).rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _text(element: ElementTree.Element) -> str:
    return _SPACE.sub(" ", " ".join(element.itertext())).strip()


def _documentation_text(element: ElementTree.Element) -> str:
    """Compact prose while retaining the line boundaries used by value lists."""
    raw = "".join(element.itertext())
    lines = (_SPACE.sub(" ", line).strip() for line in raw.splitlines())
    return "\n".join(line for line in lines if line)


def _required(element: ElementTree.Element, attribute: str, owner: str) -> str:
    value = str(element.get(attribute) or "").strip()
    if not value:
        raise ValueError(f"an Orchestra {owner} has no {attribute}")
    return value


def _integer(element: ElementTree.Element, attribute: str, owner: str) -> int:
    value = str(element.get(attribute) or "").strip()
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"an Orchestra {owner} has no numeric {attribute}")
    return int(value)


def _scenario_rank(scenario: str) -> int:
    return 0 if not scenario or scenario.casefold() == "base" else 1


def _quickfix_version(root: ElementTree.Element) -> str:
    protocol = str(root.get("type") or "FIX").upper()
    major = str(root.get("major") or "")
    minor = str(root.get("minor") or "")
    service_pack = str(root.get("servicepack") or "0")
    if not major or not minor:
        return ""
    version = f"{protocol}.{major}.{minor}"
    if protocol != "FIXT" and service_pack not in {"", "0"}:
        version = f"{version}.SP{service_pack}"
    return version.removeprefix("FIX.") if protocol == "FIX" else version


def _orchestra_protocol_version(repository_version: str) -> str:
    """Remove an Orchestra extension-pack revision from its protocol version."""
    return re.sub(r"_EP\d+$", "", repository_version, flags=re.IGNORECASE)
