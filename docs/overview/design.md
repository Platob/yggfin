# Design

## Arrow is the boundary

One Arrow schema defines each record. Producers and consumers cast against the
same declaration, so width, order, nullability, metadata, and nested types do
not drift between task applications, Iceberg, Python, or a later Rust stage.

```python
from rekep import Book

shape = Book.into_field()
empty = shape.into_arrow_schema().empty_table()

print(len(shape.into_arrow_schema()), len(shape.into_iceberg_schema().fields))
print(shape.cast_arrow(empty).num_columns)
```

```text
55 55
55
```

Shape changes use Arrow kernels. Data-sized Python collections or row loops do
not belong in casts.

## Stream by default

Readers and writers exchange `RecordBatchReader`. Batch size bounds parsing
memory; commit size bounds write memory and metadata growth. Files are opened
one at a time in deterministic natural order. Filters and projections are
pushed to Iceberg before rows are read.

## Refuse ambiguity

Missing non-null fields, unsupported unions, unresolved locations, unordered
event streams, and unsafe merge keys raise at their boundary. Filling or
guessing would make bad data look valid.

## Keep identities portable

Value identities are signed `int64`. Lifecycle and reference identities are
direct XXH3-128 digests, while event `hash` composes epoch microseconds over a
value digest. All three wide identities use big-endian
`fixed_size_binary(16)` in Arrow and `fixed[16]` in Iceberg.
Composite keys use the exact
[binary frame](../contracts/identity.md), not Python formatting or
process-local hashes.
Enums persist integer codes with their member table in field metadata, so an
unknown future code is retained.

## Keep filtered values flat

Columns used for time, instrument, state, price, or quantity filters remain
top-level. Nested lists hold compact book levels and repeated protocol data,
where engines do not provide useful bounds anyway.

## Separate reusable logic from orchestration

Package classes parse, normalize, fold, cast, and store data. Task
applications choose sources, targets, time windows, and deployment policy.
`Task` describes an application config; it never hides the application
implementation.

## Documentation budget

A field description is one factual sentence. Keep units, source, derivation,
and null meaning; remove prose that repeats the field name or type. Put one
non-obvious constraint in a nearby comment and longer rationale only on the
guide page that owns it.

## Validation

Tests cover reusable internals and compare optimized paths with Arrow or
pyiceberg references. Long storage transactions are marked `integration`.
Focused component benchmarks remain; full-pipeline and million-row development
benchmarks do not.
