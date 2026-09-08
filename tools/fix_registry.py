import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")

with app.setup:
    import json

    import marimo as mo
    import pyarrow

    from rekep import Field
    from rekep.fix import FixRegistry, fix_registry, parse_arrow_reader

    def open_registry(location):
        """Open one registry location; blank selects rekep's bundled registry."""
        return fix_registry(location)

    def into_registry_rows(dictionary):
        """Small display rows over the native registry iterator."""
        rows = []
        for field in dictionary:
            fix = field.fix
            lineage = json.loads(fix.get("lineage", '{"entries":[]}'))["entries"]
            aliases = fix.aliases
            tags = fix.tags
            name = field.display or field.name
            description = fix.description or field.comment or ""
            rows.append(
                {
                    "_id": fix.id,
                    "_search": " ".join(
                        (
                            str(fix.tag or ""),
                            *(str(tag) for tag in tags),
                            name,
                            field.name,
                            *aliases,
                            description,
                        )
                    ).casefold(),
                    "tag": fix.tag,
                    "name": name,
                    "branch": fix.branch or "standard",
                    "shape": "group" if field.dtype.is_nested else "field",
                    "Arrow kind": field.dtype.kind,
                    "FIX type": lineage[-1].get("type", "") if lineage else "",
                    "since": lineage[0].get("since", "") if lineage else "",
                    "aliases": ", ".join(aliases),
                    "description": description,
                }
            )
        return rows

    def metadata_records(field, key, collection):
        """One validated FIX metadata document as table rows."""
        document = field.fix.get(key)
        return [] if document is None else json.loads(document).get(collection, [])

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
        """The registry's full parser schema without reading a row."""
        source = pyarrow.RecordBatchReader.from_batches(pyarrow.schema([]), [])
        parsed = parse_arrow_reader(source, registry=dictionary)
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
    registry_rows = into_registry_rows(dictionary)
    return dictionary, registry_error, registry_rows, registry_source


@app.cell(hide_code=True)
def _(registry_error, registry_rows, registry_source):
    if registry_error:
        _status = mo.callout(registry_error, kind="danger")
    else:
        _groups = sum(row["shape"] == "group" for row in registry_rows)
        _branches = len({row["branch"] for row in registry_rows})
        _typed = sum(bool(row["FIX type"]) for row in registry_rows)
        _status = mo.vstack(
            [
                mo.tree({"source": registry_source}),
                mo.md(
                    f"**{len(registry_rows):,} definitions** · "
                    f"{_groups:,} repeating groups · {_branches:,} branches · "
                    f"{_typed:,} typed definitions"
                ),
            ]
        )
    mo.vstack([_status])


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
    _branches = sorted({row["branch"] for row in registry_rows})
    query = mo.ui.text(
        placeholder="Tag, name, alias, or description",
        label="Search",
        debounce=250,
        full_width=True,
    )
    branch = mo.ui.dropdown(
        {"All branches": None, **{name: name for name in _branches}},
        value="All branches",
        label="Branch",
        full_width=True,
    )
    shape = mo.ui.dropdown(
        {"Fields and groups": None, "Fields": "field", "Repeating groups": "group"},
        value="Fields and groups",
        label="Shape",
        full_width=True,
    )
    return branch, query, shape


@app.cell(hide_code=True)
def _(branch, query, shape):
    mo.hstack([query, branch, shape], widths=[3, 1, 1], align="end")


@app.cell
def _(branch, query, registry_rows, shape):
    _terms = query.value.casefold().split()
    visible_registry_rows = [
        row
        for row in registry_rows
        if (branch.value is None or row["branch"] == branch.value)
        and (shape.value is None or row["shape"] == shape.value)
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
            "branch",
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
    _field = dictionary.field_by_id(_selected[0]["_id"])
    _fix = _field.fix
    _overview = mo.ui.table(
        [
            {
                "tag": _fix.tag,
                "identifier": _fix.id,
                "name": _field.display or _field.name,
                "storage name": _field.name,
                "branch": _fix.branch or "standard",
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
    _lineage = metadata_records(_field, "lineage", "entries")
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
    _codes = metadata_records(_field, "codes", "codes")
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
