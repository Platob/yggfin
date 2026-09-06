# Yggdryl prompt: strict Arrow cast plans

Implement opt-in strict Arrow casting in Yggdryl. Put the implementation in the
Rust core, then expose thin Python and JavaScript bindings with identical
semantics. Preserve current behavior unless strict mode is selected.

## Strict nullability

Add a nullability policy to Arrow cast options. In strict mode, recursively
reject a missing target non-null field or a null value at that field. Include
the full field path in the error. A missing nullable field remains all-null and
undeclared source fields remain dropped. Never replace a required value with a
datatype default.

Apply the policy to scalars, arrays, record batches, tables, and readers. Keep
conversion safety independent from nullability: strictness decides whether a
value may be absent; `safe` decides whether a present value may be converted.
Preserve target field and schema metadata through every cast.

## Compiled plans

Compile the schema-dependent work once from source schema, target non-null
struct field, and options. The immutable, `Send + Sync` plan owns name mapping,
child order, recursive type dispatch, target schema, and reusable kernels.
Expose compile/preflight/apply operations and reuse one plan for every batch in
a reader. Only masks, offsets, and dictionary reachability may vary per batch.

Exact casts are zero-copy and return the original Arrow object when ownership
allows it. Reader casts remain lazy: do not collect batches, run Python or
JavaScript row loops, or plan again per batch. Pull one source batch at a time,
surface its error at pull, release the source C stream on early close or drop,
and fuse after the first error.

Python exposes named `cast_arrow_table` and `cast_arrow_reader` methods; the
generic `cast_arrow` delegates to them. A table cast is explicitly eager and a
reader cast returns a lazy `RecordBatchReader`. JavaScript exposes equivalent
batch and stream methods using its existing Arrow ownership conventions. Both
bindings pass `safe` and strictness explicitly.

## Proof

Test missing required fields, required nulls, nullable omissions, nested
struct/list/map fields, dictionaries, extension metadata, ambiguous names,
exact no-op identity, reader errors on pull, early-close release, and one plan
compilation across many batches. Assert complete error paths.

Benchmark 1, 10, and 1,000 small batches against current per-batch planning
after verifying identical results. Report warmed medians and reject a
regression. Assert fixed materialization budgets per batch rather than across
the whole reader.

Document default and strict policies, eager versus lazy APIs, error timing, and
ownership. Do not add compatibility aliases or binding-side structural cast
implementations.
