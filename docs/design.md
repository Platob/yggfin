# Design

## Arrow is the boundary

One Arrow schema defines each record. Producers and consumers cast against the
same declaration, so width, order, nullability, metadata, and nested types do
not drift between notebooks, Iceberg, Python, or a later Rust stage.

```python
shape = Quote.into_field()
shape.into_arrow_schema()
shape.cast_arrow(reader)
shape.into_iceberg_schema()
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

All persisted identifiers are signed `int64`. Composite keys use the exact
[binary frame](identity.md), not Python formatting or process-local hashes.
Enums persist integer codes with their member table in field metadata, so an
unknown future code is retained.

## Keep filtered values flat

Columns used for time, instrument, state, price, or quantity filters remain
top-level. Nested lists hold compact book levels and repeated protocol data,
where engines do not provide useful bounds anyway.

## Separate reusable logic from orchestration

Package classes parse, normalize, fold, cast, and store data. Notebook tasks
choose sources, targets, time windows, and deployment policy. `Task` describes
a notebook config; it never hides the notebook implementation.

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
