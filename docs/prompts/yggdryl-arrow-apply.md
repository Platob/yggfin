# Yggdryl prompt: finish native Arrow apply

Start at merged main `179b34df`. Implement in Rust, then expose thin Python and
JavaScript bindings. Add no compatibility surface.

At `AppliedPlan::compile`, reject executable `partition:` or `digest:` metadata
below list, map, union, dictionary, or run-end containers until native protocol
walkers can materialize those locations. In strict mode, defer only the target
field's initial missing/null refusal: never replace source nulls with canonical
defaults before whole-column materialization decides to preserve a populated
column. Final verification must therefore reject remaining required nulls.

Fuse `AppliedReader` on its first source, protocol, or verification error and
drop its inner stream then. Add eager `Field.apply_arrow_table`, backed by one
compiled plan, to Rust and Python/JavaScript. During plan compilation, disable
partition and digest batch walks when the schema contains no corresponding
namespace, while still rejecting malformed partial declarations.

Let an apply plan consume dependency-only input columns named by partition and
digest sources while emitting exactly its target schema. This removes consumer
projection closures and covers explicit, default, and `*` digest selection.
Order generated fields by their dependency graph: a partition derived from a
digest holder must see the digest produced in the same apply. Reject cycles at
plan construction rather than leaving a generated null or depending on field
order.
Generalize a derived partition transform to accept typed literal arguments so
`truncate(timestamp, "hour")` can materialize a UTC hour boundary; keep unary
tokens canonical and compile the expression once per reader.

Prove complete nested error paths, partial generated nulls, populated generated
columns, malformed protocols, schema-only failure, lazy pull-time errors,
early release, and one-plan reuse. Benchmark 1, 10, and 1,000 small batches with
and without protocol metadata after asserting identical output; no-protocol
application must be indistinguishable from the strict cast path within noise.
