# Yggdryl prompt: expose the native FIX schema

Start at `c9c84b24` on `codex/fix-arrow-python`, or its merged descendant.
Implement in Rust, then keep Python and JavaScript bindings thin. The only
FIX schema source of truth is `rust/src/fix/schema.rs::fix_schema`; do not add
a parser, projection, schema class, tag list, or binding-owned JSON model.

Expose the native constructor as Python
`fix_schema(registry: FixRegistry | None = None, name: str = "fix", *,
carried: FieldLike | pyarrow.Schema | None = None) -> Field` and JavaScript
`fix.schema(registry?: FixRegistry | null, name?: string,
carried?: Field | null): Field`. `None`/`null` uses the global registry. With
no `carried` value, return exactly Rust `fix_schema(registry, name)`. A carried
root must be a Struct; preserve every carried child, including nested metadata,
in source order, append the native FIX children, reject case-insensitive child
name collisions, and put the FIX root metadata on the named combined root.
Python may coerce a PyArrow schema through the existing Arrow-to-`Field`
boundary; JavaScript takes its native `Field`. Move the private merge in
`rust/src/fix/batch.rs` beside the schema owner and make
`FixBatchReader::from_column` call the same operation.

Return an ordinary native `Field`. Its existing `into_arrow_schema`,
`into_json`/`toJSON`, and metadata views must be the only renderers. Prove that
Python and JavaScript return structurally identical JSON for the same registry,
and that the Arrow round trip preserves numeric tag names, display and FIX
metadata, nested groups, nullability, derived crate columns, `entries`, and
`unmapped`. Add Rust tests comparing direct, carried, and batch-reader schemas;
cover empty/partial/full registries, root and child metadata, collision refusal,
and deterministic order. Add binding export, typing, explicit/global registry,
carried/no-carried, PyArrow-schema (Python), JSON-parity, and executable-doc
tests. Assert schema construction cannot pull or parse a row, then delete the
empty-reader schema workaround from examples and downstream UI code.

One adjacent deletion is allowed only if it removes yggfin's
`rekep.fields.replace_field`: add one native immutable
`Field.clone_with(*, name=None, dtype=None, nullable=None, metadata=None) -> Field`
and JavaScript `field.cloneWith({ name, dtype, nullable, metadata }): Field`.
Unspecified parts, dictionary flags, metadata, and other native field state
must survive; replacements use existing validation. Otherwise omit this API
rather than reconstructing a `Field` in a binding. Do not add any other field
builder or compatibility layer.
