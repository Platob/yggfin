import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")

with app.setup:
    import json

    import marimo as mo
    import pyarrow

    from rekep import Field
    from rekep.fix import FixRegistry, fix_codec, fix_registry

    #: What a dictionary holds beside its scalar fields. A definition is a
    #: field whatever its category, so one projection reads all three.
    CATEGORIES = ("fields", "components", "groups")

    def open_registry(location):
        """Open one registry location; blank selects rekep's bundled registry."""
        return fix_registry(location)

    def metadata_records(field, key):
        """One validated FIX metadata document as table rows.

        The document is the collection itself -- an array, in the order the
        specification states it -- so nothing is unwrapped out of a named
        member first.
        """
        document = field.fix.get(key)
        return [] if document is None else json.loads(document)

    def _typing(field):
        """What the lineage says this definition was first and last typed as."""
        lineage = metadata_records(field, "lineage")
        if not lineage:
            return "", ""
        held = lineage[-1].get("type", "")
        # A version that retyped a field spells the type as a document; the
        # one it names is what a reader of this table wants to see.
        if isinstance(held, dict):
            held = held.get("type", "")
        return held, lineage[0].get("since", "")

    def _definition_row(field, category):
        """One display row over any definition the registry holds."""
        fix = field.fix
        aliases = fix.aliases
        name = field.display or field.name
        description = fix.description or field.comment or ""
        typed, since = _typing(field)
        return {
            "_id": fix.id,
            "_name": field.name,
            "_search": " ".join(
                (
                    str(fix.tag or ""),
                    *(str(tag) for tag in fix.tags),
                    name,
                    field.name,
                    *aliases,
                    description,
                )
            ).casefold(),
            "tag": fix.tag,
            "name": name,
            "dialect": ", ".join(fix.branches) or "standard",
            "shape": "group" if field.dtype.is_nested else "field",
            "category": category,
            "Arrow kind": field.dtype.kind,
            "FIX type": typed,
            "since": since,
            "aliases": ", ".join(aliases),
            "description": description,
        }

    def into_registry_rows(dictionary):
        """Small display rows over the native registry iterator."""
        return [_definition_row(field, "fields") for field in dictionary]

    def into_definition_rows(dictionary, categories=CATEGORIES):
        """The same rows over every category a dictionary stores.

        A scalar field, a component and a repeating group are one kind of
        thing stored under three names, so a browser of a complete dictionary
        reads them through one projection rather than three.
        """
        return [
            _definition_row(field, category)
            for category in categories
            for field in dictionary.definitions(category)
        ]

    def into_msgtype_rows(dictionary):
        """Every message type the dictionary defines, by its wire code."""
        rows = []
        for msgtype in dictionary.msgtypes():
            field = msgtype.field
            rows.append(
                {
                    "_search": f"{msgtype.value} {msgtype.name}".casefold(),
                    "code": msgtype.value,
                    "name": field.display or msgtype.name,
                    "storage name": msgtype.name,
                    "identifiers": ", ".join(field.fix.identifiers or ()),
                    "description": field.fix.description or field.comment or "",
                }
            )
        rows.sort(key=lambda row: row["code"])
        return rows

    def member_rows(field):
        """Native collection explosion followed by native struct flattening."""
        if not field.dtype.is_nested:
            return []
        return [
            {
                "path": member.name,
                "tag": member.fix.tag,
                "name": member.display or member.name,
                "Arrow kind": member.dtype.kind,
                "nullable": member.nullable,
            }
            for expanded in field.explode_fields()
            for member in expanded.unnest_fields()
        ]

    def into_fixmsg_schema(dictionary):
        """The registry's full parser schema without reading a row.

        The codec answers a schema from the carrier and the dictionary alone,
        so a carrier of nothing but the payload column is the whole input.
        """
        carrier = pyarrow.schema([pyarrow.field("body", pyarrow.string(), False)])
        source = pyarrow.RecordBatchReader.from_batches(carrier, [])
        parsed = fix_codec(dictionary).parse_text_arrow_reader(source)
        try:
            return Field.from_arrow_schema(parsed.schema, name="FixMsg")
        finally:
            parsed.close()
            source.close()


@app.cell(hide_code=True)
def _():
    mo.md("""
    # FIX registry

    Inspect a rekep dictionary. Open any supported registry URI,
    search fields and repeating groups, export a definition, or snapshot the
    complete `FixMsg` parser schema without parsing a row.
    """)


@app.cell
def _():
    registry_location = mo.ui.text(
        placeholder="file:///srv/fix or s3://bucket/fix",
        label="Registry location (blank uses the rekep bundle)",
        full_width=True,
    ).form(
        submit_button_label="Open registry",
        show_clear_button=True,
        clear_button_label="Use bundled registry",
    )
    mo.vstack([registry_location])
    return (registry_location,)


@app.cell
def _(registry_location):
    _submitted = registry_location.value
    _location = None if _submitted is None else (_submitted.strip() or None)
    try:
        dictionary = open_registry(_location)
        registry_error = ""
    except (OSError, ValueError) as _error:
        dictionary = FixRegistry()
        registry_error = f"{type(_error).__name__}: {_error}"
    registry_source = "rekep bundle" if _location is None else str(_location)
    registry_rows = into_definition_rows(dictionary)
    msgtype_rows = into_msgtype_rows(dictionary)
    return dictionary, msgtype_rows, registry_error, registry_rows, registry_source


@app.cell(hide_code=True)
def _(dictionary, msgtype_rows, registry_error, registry_rows, registry_source):
    if registry_error:
        _status = mo.callout(registry_error, kind="danger")
    else:
        _counts = {
            category: sum(row["category"] == category for row in registry_rows)
            for category in CATEGORIES
        }
        _dialects = dictionary.dialects()
        _status = mo.vstack(
            [
                mo.tree(
                    {
                        "source": registry_source,
                        "dialects": ", ".join(_dialects) or "standard only",
                    }
                ),
                mo.md(
                    f"**{len(registry_rows):,} definitions** · "
                    f"{_counts['fields']:,} fields · "
                    f"{_counts['components']:,} components · "
                    f"{_counts['groups']:,} repeating groups · "
                    f"{len(msgtype_rows):,} message types"
                ),
            ]
        )
    mo.vstack([_status])


@app.cell
def _(msgtype_rows):
    mo.stop(not msgtype_rows, mo.callout("This registry defines no message type."))
    msgtype_table = mo.ui.table(
        msgtype_rows,
        selection=None,
        page_size=10,
        show_column_summaries=False,
        show_data_types=False,
        show_search=True,
        show_download=True,
        freeze_columns_left=["code"],
        visible_columns=["code", "name", "storage name", "identifiers", "description"],
        wrapped_columns=["identifiers", "description"],
        max_height=420,
    )
    mo.accordion({f"Message types · {len(msgtype_rows):,}": msgtype_table})


@app.cell
def _(dictionary, registry_rows):
    mo.stop(
        not registry_rows,
        mo.callout(
            "This registry is empty. Enter a populated registry location above.",
            kind="warn",
        ),
    )
    fixmsg_schema = into_fixmsg_schema(dictionary)
    fixmsg_schema_json = fixmsg_schema.into_json(indent=2)
    return fixmsg_schema, fixmsg_schema_json


@app.cell(hide_code=True)
def _(fixmsg_schema, fixmsg_schema_json):
    _schema_editor = mo.ui.code_editor(
        value=fixmsg_schema_json,
        language="json",
        disabled=True,
        min_height=520,
        show_copy_button=True,
        label="FixMsg Field JSON",
    )
    _schema_download = mo.download(
        data=fixmsg_schema_json.encode("utf-8"),
        filename="fixmsg-schema.json",
        mimetype="application/json",
        label="Download FixMsg schema",
    )
    mo.accordion(
        {
            f"Full FixMsg schema · {len(fixmsg_schema):,} columns": mo.vstack(
                [_schema_download, _schema_editor]
            )
        }
    )


@app.cell
def _(registry_rows):
    _dialects = sorted({row["dialect"] for row in registry_rows})
    query = mo.ui.text(
        placeholder="Tag, name, alias, or description",
        label="Search",
        debounce=250,
        full_width=True,
    )
    dialect = mo.ui.dropdown(
        {"All dialects": None, **{name: name for name in _dialects}},
        value="All dialects",
        label="Branch",
        full_width=True,
    )
    category = mo.ui.dropdown(
        {"Every category": None, **{name.title(): name for name in CATEGORIES}},
        value="Every category",
        label="Category",
        full_width=True,
    )
    return category, dialect, query


@app.cell(hide_code=True)
def _(category, dialect, query):
    mo.hstack([query, dialect, category], widths=[3, 1, 1], align="end")


@app.cell
def _(category, dialect, query, registry_rows):
    _terms = query.value.casefold().split()
    visible_registry_rows = [
        row
        for row in registry_rows
        if (dialect.value is None or row["dialect"] == dialect.value)
        and (category.value is None or row["category"] == category.value)
        and all(term in row["_search"] for term in _terms)
    ]
    mo.stop(
        not visible_registry_rows,
        mo.callout("No native FIX definitions match these filters.", kind="warn"),
    )
    registry_table = mo.ui.table(
        visible_registry_rows,
        selection="single",
        initial_selection=[0],
        page_size=20,
        show_column_summaries=False,
        show_data_types=False,
        show_search=False,
        show_download=True,
        freeze_columns_left=["tag", "name"],
        visible_columns=[
            "tag",
            "name",
            "category",
            "dialect",
            "shape",
            "Arrow kind",
            "FIX type",
            "since",
            "aliases",
            "description",
        ],
        wrapped_columns=["aliases", "description"],
        max_height=560,
        label=f"{len(visible_registry_rows):,} matching definitions",
    )
    mo.vstack([registry_table])
    return (registry_table,)


@app.cell
def _(dictionary, registry_table):
    _selected = registry_table.value
    mo.stop(not _selected, mo.callout("Select one definition to inspect it."))
    _row = _selected[0]
    # A scalar field is addressed by the identifier its tag and name make; a
    # component or a group has no tag of its own to be found by, so it is
    # addressed by name inside the category that stores it.
    _field = (
        dictionary.field_by_id(_row["_id"])
        if _row["category"] == "fields"
        else dictionary.definition(_row["category"], _row["_name"])
    )
    _fix = _field.fix
    _overview = mo.ui.table(
        [
            {
                "tag": _fix.tag,
                "identifier": _fix.id,
                "name": _field.display or _field.name,
                "storage name": _field.name,
                "dialect": ", ".join(_fix.branches) or "standard",
                "Arrow type": str(_field.dtype.into_arrow()),
                "nullable": _field.nullable,
                "description": _fix.description or _field.comment or "",
            }
        ],
        selection=None,
        pagination=False,
        show_column_summaries=False,
        show_data_types=False,
        show_search=False,
        wrapped_columns=["description"],
        max_columns=None,
    )
    _members = member_rows(_field)
    _member_view = (
        mo.ui.table(
            _members,
            selection=None,
            page_size=20,
            show_column_summaries=False,
            show_data_types=False,
            show_search=True,
            freeze_columns_left=["path"],
            max_height=520,
        )
        if _members
        else mo.callout("This definition has no nested members.")
    )
    _lineage = metadata_records(_field, "lineage")
    _lineage_view = (
        mo.ui.table(
            _lineage,
            selection=None,
            pagination=False,
            show_column_summaries=False,
            show_data_types=False,
            show_search=False,
            max_columns=None,
        )
        if _lineage
        else mo.callout("This definition carries no FIX lineage.")
    )
    _codes = metadata_records(_field, "codes")
    _codes_view = (
        mo.ui.table(
            _codes,
            selection=None,
            page_size=20,
            show_column_summaries=False,
            show_data_types=False,
            show_search=True,
            max_height=520,
            max_columns=None,
        )
        if _codes
        else mo.callout("This definition carries no FIX code set.")
    )
    _metadata_view = mo.ui.table(
        [{"key": key, "value": value} for key, value in _field.metadata.items()],
        selection=None,
        pagination=False,
        show_column_summaries=False,
        show_data_types=False,
        show_search=True,
        wrapped_columns=["value"],
        max_height=520,
    )
    _field_json = _field.into_json(indent=2)
    _json_view = mo.vstack(
        [
            mo.download(
                data=_field_json.encode("utf-8"),
                filename=f"fix-field-{_fix.tag}.json",
                mimetype="application/json",
                label="Download field JSON",
            ),
            mo.ui.code_editor(
                value=_field_json,
                language="json",
                disabled=True,
                min_height=520,
                show_copy_button=True,
                label="rekep Field JSON",
            ),
        ]
    )
    mo.vstack(
        [
            mo.md("## Definition"),
            mo.ui.tabs(
                {
                    "Overview": _overview,
                    f"Members ({len(_members):,})": _member_view,
                    f"Lineage ({len(_lineage):,})": _lineage_view,
                    f"Codes ({len(_codes):,})": _codes_view,
                    "Metadata": _metadata_view,
                    "Field JSON": _json_view,
                }
            ),
        ]
    )


if __name__ == "__main__":
    app.run()
