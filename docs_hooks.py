"""Build documentation artifacts that derive from repository data."""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any

from rekep import Execution, Field, FixMsg, InstUpdate, Message, Order
from rekep import enums as rekep_enums
from rekep.enums.ascii_codes import Ascii32
from rekep.fix import FixRegistry
from rekep.market import Book
from rekep.text.message import SESSION_FIELDS

#: The six persisted contracts, in the order a row reaches them: text first,
#: then the market products a transcribed row is translated into. `stage` is
#: what the page groups them under and `source` is the product upstream, so
#: the lineage a page draws is this table and not a second hand-kept copy.
PRODUCTS: tuple[tuple[type, str, str, str | None], ...] = (
    (Message, "message", "text", None),
    (FixMsg, "fixmsg", "text", "message"),
    (InstUpdate, "instrument", "market", "fixmsg"),
    (Order, "order", "market", "fixmsg"),
    (Execution, "execution", "market", "fixmsg"),
    (Book, "book", "market", "order"),
)


def on_post_build(config: Any) -> None:
    """Write the browser artifacts from the code this checkout holds."""
    root = Path(config.config_file_path).resolve().parent
    assets = Path(config.site_dir) / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    _write(assets / "fix-registry.json", _registry_catalog(root))
    _write(assets / "product-lineage.json", _product_catalog())
    _write(assets / "enum-codes.json", _enum_catalog())


def _write(target: Path, payload: Any) -> None:
    """One artifact, as compact as JSON gets: the browser reads it, not a person."""
    target.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def _registry_catalog(root: Path) -> dict[str, Any]:
    """The browser catalog from the registry used by this checkout."""
    registry = FixRegistry(cache_dir=root / "data" / "fix")
    namespaces = registry.namespaces()
    fields_by_namespace = {
        namespace: tuple(registry.field_records(namespace).values()) for namespace in namespaces
    }
    components_by_namespace = {
        namespace: tuple(registry.component_records(namespace).values()) for namespace in namespaces
    }
    groups_by_namespace = {
        namespace: tuple(registry.repeating_group_records(namespace).values())
        for namespace in namespaces
    }
    fields = [
        _field_view(entry, namespace)
        for namespace in namespaces
        for entry in sorted(
            fields_by_namespace[namespace],
            key=lambda one: (
                one.fix.tag is None,
                one.fix.tag or 0,
                one.fix.canonical,
            ),
        )
    ]
    components = [
        (entry, namespace)
        for namespace in namespaces
        for entry in sorted(components_by_namespace[namespace], key=lambda one: one.name)
    ]
    groups = [
        (entry, namespace)
        for namespace in namespaces
        for entry in sorted(groups_by_namespace[namespace], key=lambda one: one.name)
    ]
    namespace_order = {namespace: index for index, namespace in enumerate(namespaces)}
    sources = sorted(
        (dict(source) for source in registry.source_manifest()),
        key=lambda source: (
            namespace_order.get(str(source.get("namespace", "standard")), len(namespace_order)),
            str(source.get("source_id", "")),
        ),
    )
    return {
        "versions": list(registry.versions),
        "namespaces": list(namespaces),
        "sources": sources,
        "coverage": {
            "components": len(components),
            "groups": len(groups),
            "fields": len(fields),
            "enumerations": sum(bool(field.get("values")) for field in fields),
            "versions": len(registry.versions),
            "namespaces": len(namespaces),
            "sources": len(sources),
            "by_namespace": [
                {
                    "namespace": namespace,
                    "fields": len(fields_by_namespace[namespace]),
                    "components": len(components_by_namespace[namespace]),
                    "groups": len(groups_by_namespace[namespace]),
                    "enumerations": sum(
                        bool(entry.fix.enumerated) for entry in fields_by_namespace[namespace]
                    ),
                }
                for namespace in namespaces
            ],
        },
        "components": [
            {
                **entry.into_dict(),
                "slug": entry.slug,
                "namespace": namespace,
                "record_kind": "component",
            }
            for entry, namespace in components
        ],
        "groups": [
            {
                **entry.into_dict(),
                "slug": entry.slug,
                "namespace": namespace,
                "record_kind": "group",
            }
            for entry, namespace in groups
        ],
        "fields": fields,
    }


def _enum_catalog() -> dict[str, Any]:
    """Every compiled ASCII code, so the encoder page can be checked against them.

    Built from the classes rather than from a table written beside them: the
    page's arithmetic is the packing rule, and this is what the rule is
    supposed to produce, so a page that disagreed with the package would say
    so on screen rather than in a comment nobody re-reads.

    Stored values are strings. Wide integers exceed JavaScript's exact number
    range, while sixteen-byte codes are published as their physical hex bytes.
    """
    found = []
    for name in sorted(rekep_enums.__all__):
        enum = getattr(rekep_enums, name)
        if not isinstance(enum, type) or not issubclass(enum, Ascii32) or not enum.__members__:
            continue
        found.append(
            {
                "name": name,
                "base": (
                    "Ascii32"
                    if enum.BYTE_WIDTH <= 4
                    else "Ascii64"
                    if enum.BYTE_WIDTH <= 8
                    else "Ascii128"
                ),
                "byte_width": enum.BYTE_WIDTH,
                "stored": (
                    "int32"
                    if enum.BYTE_WIDTH <= 4
                    else "int64"
                    if enum.BYTE_WIDTH <= 8
                    else "fixed_size_binary[16]"
                ),
                "open": True,
                "members": [
                    {
                        "key": key,
                        "code": member.code,
                        "value": member.stored_key(),
                        "rank": member.rank,
                        "fix": member.into_fix(),
                        "alias_of": "" if key == member.name else member.name,
                    }
                    for key, member in enum.__members__.items()
                ],
            }
        )
    return {"enums": found}


def _field_view(entry: Field, namespace: str = "standard") -> dict[str, Any]:
    """One field record as the page reads it: flat, with nothing left packed.

    A stored record is a `Field` document -- the Arrow reading at the top, the
    protocol's own under `fix`, and every list packed into one JSON string
    because Arrow metadata is bytes to bytes. That is the right shape for a
    file the registry loads and a person edits, and the wrong one for a page
    that would otherwise parse a string per key per row. So this projection
    lives in the build that serves the page, not in the store.
    """
    fix = entry.fix
    view: dict[str, Any] = {"name": fix.canonical, "namespace": namespace}
    if fix.tag is not None:
        view["tag"] = fix.tag
    for key, value in (
        ("type", str(entry.dtype) if entry.dtype is not None else "unknown"),
        ("fix_type", fix.type),
        ("description", entry.description),
        ("column", fix.column),
        ("note", fix.note),
        ("versions", list(fix.versions)),
        ("values", [one.into_dict() for one in fix.enumerated]),
        ("aliases", [alias.into_dict() for alias in fix.named_aliases]),
        ("used_in", list(fix.msgtypes)),
        ("components", list(fix.components)),
        ("event_types", {code: kind.name for code, kind in fix.event_types.items()}),
        ("states", {code: state.name for code, state in fix.states.items()}),
        ("sources", list(fix.sources)),
    ):
        if value:
            view[key] = value
    return view


def _product_catalog() -> dict[str, Any]:
    """Every product's columns, and what upstream each column is read from.

    Built from `Field.from_class` rather than from the checked-in documents
    under `schemas/`: those are dumped from these same classes, and a page
    that described the dump would describe whatever the last dump was rather
    than what this checkout does.

    Enumerations are collected once and referenced by name. `Currency` alone
    carries several hundred values, and six products naming it inline is the
    same table six times in a file the browser downloads.
    """
    enums: dict[str, Any] = {}
    products = []
    for pyclass, key, stage, source in PRODUCTS:
        document = Field.from_class(pyclass).into_dict()
        _lift_session_header(document, key)
        products.append(
            {
                "key": key,
                "name": document["name"],
                "stage": stage,
                "source": source,
                "description": document.get("description") or "",
                "namespace": document["metadata"]["namespace"],
                "version": document["metadata"]["version"],
                "columns": [_column(member, enums) for member in document["fields"]],
            }
        )
    return {"products": products, "enums": enums}


def _lift_session_header(document: dict[str, Any], key: str) -> None:
    """Give the raw stage's seven header columns the tags they are lifted from.

    `Message` is protocol-neutral, so its declaration carries no FIX origin --
    the annotation lives on `FixMsg`, where a column *is* a FIX field. But the
    raw stage does lift seven of them, and `SESSION_FIELDS` is where the code
    says which and from what tag. Read from there rather than matched on the
    column names: the names agreeing with the standard's is what makes the
    guess look safe, and a guess that looks safe is still not the declaration.
    """
    if key != "message":
        return
    tags = dict(SESSION_FIELDS)
    for member in document["fields"]:
        tag = tags.get(member["name"])
        if tag is not None:
            member["fix"] = {"tag": tag, "name": member["name"]}


def _column(
    member: dict[str, Any], enums: dict[str, Any], default_name: str = "item"
) -> dict[str, Any]:
    """One column as a lineage node: its type, its origin, and its role.

    `fix` is the whole of the origin a widget can show honestly -- the tag and
    the name a value is read from -- so the registry's version and message-type
    lists are dropped here. They belong to the field, and the registry pages
    already hold them. Struct fields, list items and map pairs stay nested
    because their path is part of the persisted contract.
    """
    column: dict[str, Any] = {
        "name": member.get("name") or default_name,
        "type": member["type"],
        "description": member.get("description") or "",
    }
    if member.get("nullable"):
        column["nullable"] = True
    origin = member.get("fix") or {}
    if origin.get("tag") or origin.get("name"):
        column["fix"] = {"tag": origin.get("tag"), "name": origin.get("name")}
    enum = member.get("enum") or {}
    if enum.get("name"):
        name = enum["name"]
        if name not in enums:
            enums[name] = json.loads(enum.get("values") or "{}")
        column["enum"] = name
    iceberg = member.get("iceberg") or {}
    if iceberg.get("primary_key"):
        column["key"] = "primary"
    if iceberg.get("partition_key"):
        column["partition"] = iceberg["partition_key"]
        column["derived_from"] = iceberg.get("derived_from")
    unit = (member.get("metadata") or {}).get("unit")
    if unit:
        column["unit"] = unit
    if member.get("fields"):
        column["fields"] = [_column(child, enums) for child in member["fields"]]
    if member.get("item"):
        column["item"] = _column(member["item"], enums, "item")
    if member.get("key"):
        column["key_field"] = _column(member["key"], enums, "key")
    if member.get("value"):
        column["value_field"] = _column(member["value"], enums, "value")
    return column


# --- diagrams -------------------------------------------------------------

#: The diagrams are authored once, for the dark scheme, and the light variant
#: is derived. Two hand-kept copies of one drawing is two things to change
#: whenever a stage moves, and the second one to be forgotten is the one
#: nobody is looking at.
DIAGRAMS = (
    "arrow-hub",
    "compatibility-tree",
    "rkp-logo",
    "schema-lineage",
    "workflow-run",
)

#: Each third-party mark and the box it draws itself in. A mark that declares
#: no `viewBox` still declares a width and a height, and that is the box.
MARKS = {
    "logos/apache-airflow.svg": (175, 175),
    "logos/apache-arrow.svg": (1350, 1181.25),
    "logos/apache-iceberg.svg": (800, 218),
    "logos/github-mark.svg": (24, 24),
}

#: Above this CIELAB chroma a colour carries meaning rather than depth. The
#: diagrams' neutrals all sit under 6 and the two brand accents over 75, so
#: nothing in them is anywhere near the line.
ACCENT_CHROMA = 20.0

#: The one rule whose colour is not depth: a third-party mark is reproduced
#: unmodified, so the card it sits on stays the ground that mark was drawn
#: for. Relit with the rest, it put the Apache and GitHub marks on black.
KEPT_SELECTOR = ".brand-plate"

_HEX = re.compile(r"#[0-9a-fA-F]{6}\b")
_STYLE = re.compile(r"(<style[^>]*>)(.*?)(</style>)", re.S)
_RULE = re.compile(r"([^{}]+)\{([^}]*)\}")
_MARK = re.compile(r'<image href="(logos/[^"]+)"([^/>]*)/>')

#: D65, the white point sRGB is defined against.
_WHITE = (0.95047, 1.0, 1.08883)


def _to_lab(rgb: tuple[int, int, int]) -> tuple[float, float, float]:
    """One sRGB colour in CIELAB, where lightness is a coordinate of its own."""

    def linear(channel: float) -> float:
        return channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4

    def curve(value: float) -> float:
        return value ** (1 / 3) if value > 216 / 24389 else (24389 / 27 * value + 16) / 116

    red, green, blue = (linear(channel / 255) for channel in rgb)
    xyz = (
        0.4124564 * red + 0.3575761 * green + 0.1804375 * blue,
        0.2126729 * red + 0.7151522 * green + 0.0721750 * blue,
        0.0193339 * red + 0.1191920 * green + 0.9503041 * blue,
    )
    fx, fy, fz = (curve(value / white) for value, white in zip(xyz, _WHITE, strict=True))
    return 116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz)


def _from_lab(lab: tuple[float, float, float]) -> tuple[int, int, int]:
    """The CIELAB coordinate back as the nearest sRGB colour."""

    def gamma(channel: float) -> float:
        channel = max(0.0, min(1.0, channel))
        return channel * 12.92 if channel <= 0.0031308 else 1.055 * channel ** (1 / 2.4) - 0.055

    def curve(value: float) -> float:
        return value**3 if value**3 > 216 / 24389 else (116 * value - 16) * 27 / 24389

    lightness, a, b = lab
    fy = (lightness + 16) / 116
    coordinates = (fy + a / 500, fy, fy - b / 200)
    x, y, z = (curve(value) * white for value, white in zip(coordinates, _WHITE, strict=True))
    return (
        round(gamma(3.2404542 * x - 1.5371385 * y - 0.4985314 * z) * 255),
        round(gamma(-0.9692660 * x + 1.8760108 * y + 0.0415560 * z) * 255),
        round(gamma(0.0556434 * x - 0.2040259 * y + 1.0572252 * z) * 255),
    )


def relight_colour(colour: str) -> str:
    """One colour as the same colour on the other ground.

    A neutral inverts its lightness and keeps its hue, so a near-black card
    becomes a near-white one and a hairline that sat *above* its ground sits
    *below* the new one by the same perceived distance. A table keyed by hex
    would have to answer "is this a ground or a label" for every value, and
    `#71717a` in these files is both.

    A saturated colour is left exactly as it is: the orange and the red are
    identity rather than depth, they read on either ground, and inverting them
    gave a burnt brown that no longer said rekep.
    """
    rgb = (int(colour[1:3], 16), int(colour[3:5], 16), int(colour[5:7], 16))
    lightness, a, b = _to_lab(rgb)
    if (a * a + b * b) ** 0.5 >= ACCENT_CHROMA:
        return colour
    red, green, blue = _from_lab((100 - lightness, a, b))
    return f"#{red:02x}{green:02x}{blue:02x}"


def relight(source: str) -> str:
    """One diagram as the same drawing on the other ground."""

    def relit(text: str) -> str:
        return _HEX.sub(lambda found: relight_colour(found[0].lower()), text)

    def one_rule(rule: re.Match[str]) -> str:
        if KEPT_SELECTOR in rule[1]:
            return rule[0]
        return f"{rule[1]}{{{relit(rule[2])}}}"

    parts, last = [], 0
    for style in _STYLE.finditer(source):
        parts.append(relit(source[last : style.start()]))
        parts.append(f"{style[1]}{_RULE.sub(one_rule, style[2])}{style[3]}")
        last = style.end()
    parts.append(relit(source[last:]))
    return "".join(parts)


def embed_marks(source: str, assets: Path) -> str:
    """The third-party marks carried in the drawing rather than referenced.

    A browser renders `<img src="a.svg">` in a context that loads no external
    resource, so `href="logos/apache-iceberg.svg"` inside one of these fetches
    nothing and the plate under it stays empty -- which is what every one of
    these diagrams has been shipping. Carried as data they render.

    One `<symbol>` per mark and a `<use>` per placement, because the Iceberg
    mark appears eight times in a single diagram and eight copies of eleven
    kilobytes is most of the file.
    """
    wanted = sorted({name for name, _ in _MARK.findall(source)})
    if not wanted:
        return source
    symbols = []
    for name in wanted:
        width, height = MARKS[name]
        encoded = base64.b64encode((assets / name).read_bytes()).decode("ascii")
        symbols.append(
            f'<symbol id="mark-{Path(name).stem}" viewBox="0 0 {width} {height}">'
            f'<image href="data:image/svg+xml;base64,{encoded}" '
            f'width="{width}" height="{height}"/></symbol>'
        )
    placed = _MARK.sub(lambda m: f'<use href="#mark-{Path(m[1]).stem}"{m[2]}/>', source)
    return placed.replace("</defs>", "".join(symbols) + "</defs>", 1)


def on_files(files: Any, config: Any) -> Any:
    """Both schemes of every diagram, with their marks carried inside them.

    Here rather than after the build, because a page links the light variant
    and `--strict` checks those links against the files mkdocs knows about --
    one written straight into the output directory is a link mkdocs reports as
    broken while the browser finds it.
    """
    from mkdocs.structure.files import File

    assets = Path(config.docs_dir) / "assets"
    for name in DIAGRAMS:
        authored = (assets / f"{name}.svg").read_text(encoding="utf-8")
        for spelling, drawing in (
            (f"assets/{name}.svg", authored),
            (f"assets/{name}-light.svg", relight(authored)),
        ):
            # The authored file is already collected; the generated one takes
            # its place, so the marks reach the page in both schemes.
            standing = files.get_file_from_path(spelling)
            if standing is not None:
                files.remove(standing)
            files.append(File.generated(config, spelling, content=embed_marks(drawing, assets)))
    return files
