# Identity and binary conversions

`rekep-identity-v1` is the byte contract used for composite event and lifecycle
identifiers. It is intentionally small enough to implement without Python or
Arrow. Raw capture lines use the unframed rule below.

## Frame

For each ordered part:

1. Convert the part to its payload below.
2. Append its payload length as a signed 64-bit little-endian integer.
3. Append the payload. A null part uses length `-1` and no payload; an empty
   payload uses length `0`.

Hash the concatenated frame with XXH3-64, the default secret, and seed `0`.
XXH3 returns `u64`; store those same 64 bits as two's-complement `i64`. This is
a numeric conversion, not a digest-byte serialization. If a digest must be
sent as bytes, upstream xxHash defines its canonical byte form as big-endian.

[XXH3 became stable in xxHash 0.8.0](https://github.com/Cyan4973/xxHash/releases/tag/v0.8.0):
the same input and parameters produce the same value across systems and future
versions. The [algorithm specification](https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md)
defines its unsigned output and canonical representation. The framing and
signed database representation above are rekep's contract.

### Raw blobs

An indivisible raw line or blob is already its complete identity input:
`hash_bytes(raw)` applies the same XXH3-64 seed `0` and `u64`-to-`i64`
conversion directly to those exact bytes, without a length prefix. Composite
identities use `hash_of(*parts)` and the frame above. The two operations are
deliberately distinct and have pinned tests; an empty composite is refused.

## Stored column

A digest is computed as a signed `i64` and stored as sixteen big-endian
two's-complement bytes -- `fixed_size_binary(16)` in Arrow, `fixed[16]` in
Iceberg. One width covers both a content digest and the wider time-anchored
version hash [`rekep.txhash`](txhash.md) builds, and big-endian keeps the
column sorting as the values do. `hash_bytes_of` writes those bytes and
`hash_int_of` reads them back. `linkedhashes` stores related lifecycle digests
as signed `int64` values; the related event time comes from joining on `xhash`.

An identifier that is itself a part of another identity enters the frame as
those sixteen bytes, never as an integer -- which is why the integer payload
below still refuses anything outside `i64`.

## Scalar payloads

| Logical value | Payload |
| --- | --- |
| null | no payload; frame length is `-1` |
| UTF-8 text | exact UTF-8 bytes, without normalization |
| bytes, bytearray, memory view | exact bytes |
| boolean | one byte: `00` or `01` |
| signed integer | signed `i64.to_le_bytes()`; values outside `i64` are refused |
| floating point | IEEE-754 binary64 in little-endian order |
| UUID | its 16 RFC 4122/network-order bytes |

Floating inputs are converted to binary64 first. Positive and negative zero
remain distinct. Infinities keep their IEEE-754 bits. Every NaN payload and
sign is canonicalized to bits `0x7ff8000000000000`, whose little-endian payload
is `000000000000f87f`.

There is no type tag. Values with the same payload are deliberately equivalent:
UTF-8 text and identical raw bytes, `true` and raw byte `01`, and integer zero
and positive floating zero. A part's position must therefore keep one semantic
type. Length prefixes still distinguish `("AB", "C")` from `("A", "BC")`.

Dates, decimals, objects, maps, lists, and integers wider than `i64` are
refused instead of being stringified. A domain shape must project them into
an ordered sequence of the supported scalars, and that projection belongs to
its contract: reference data sorts map keys, records null-versus-empty
container state, and renders dates as ISO 8601 before framing.

## Arrow and Python

`hash_of(*parts)` implements the scalar composite contract and
`hash_arrow(*columns)` produces the same values row by row.
`hash_bytes_arrow(column)` is the vectorized unframed operation used for raw
message strings:

- dictionary arrays are decoded to values and extension arrays use storage;
- integers are safely widened to `int64` and floats to `float64`;
- text, binary, fixed binary, boolean, numeric, and null arrays are accepted;
- every other Arrow type is refused.

Arrow's zero-copy numeric path is enabled only on little-endian hosts. It
raises clearly on a big-endian host; scalar `hash_of`, which writes byte order
explicitly, remains portable there.

The machine-readable [golden vectors](../assets/identity-v1.json) pin conversion,
frame bytes, unsigned digest bits, and signed values. Python tests validate
both scalar and Arrow builders against every vector.

## Rust reference

The executable in `python/examples/identity-rust` reads the same golden corpus. Its
core operation is:

```rust
use xxhash_rust::xxh3::xxh3_64_with_seed;

let digest: u64 = xxh3_64_with_seed(&frame, 0);
let stored: i64 = digest as i64;
```

Every scalar uses Rust's explicit `to_le_bytes`; NaN uses the fixed bits above.
Run the complete reference with:

```console
cargo run --release --locked --manifest-path python/examples/identity-rust/Cargo.toml
# rekep-identity-v1: 3 raw + 16 framed vectors match
```

[`xxhash-rust::xxh3_64_with_seed`](https://docs.rs/xxhash-rust/0.8.15/xxhash_rust/xxh3/fn.xxh3_64_with_seed.html)
returns the unsigned 64-bit result used here. No Rust dependency is added to
the Python package.

Changing a conversion, frame byte, algorithm parameter, or signed storage rule
requires a new protocol version. Existing v1 identifiers must never be
reinterpreted under a revised rule.
