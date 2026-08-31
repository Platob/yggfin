# Identity and binary conversions

`rekep-identity-v1` is the byte contract used for composite value identities.
It is intentionally small enough to implement without Python or Arrow. Raw
capture lines and lifecycle codes use the unframed rules below.

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

A lifecycle code is already one complete UTF-8 value. `xhash` applies
XXH3-128 seed `0` directly to those bytes, without a length prefix or clock,
and stores xxHash's canonical sixteen-byte digest. An empty code produces the
all-zero sentinel rather than the digest of emptiness.

## Stored columns

```text
hash:            fixed_size_binary[16]
xhash:           fixed_size_binary[16]
instrumentxhash: fixed_size_binary[16]
prevhash:        fixed_size_binary[16]
linkhashes:      list<item: fixed_size_binary[16]>
parenthash:      list<item: fixed_size_binary[16]>
```

```python
import xxhash

from rekep import txhash
from rekep.market import Event

xhash = Event.xhash_of("ORD-1")

assert txhash.wide_bytes(xhash) == xxhash.xxh3_128_digest(b"ORD-1")
```

`CodeSource <30027>` records the reader-facing field that supplied `code`, such
as `OrderID`, `ExecID`, or `SymbolTicker`. It explains the lifecycle key but
does not add another identity part.

An event `hash` composes its epoch microseconds with its `vhash` without
hashing the payload again. It is stored as sixteen big-endian two's-complement
bytes -- `fixed_size_binary(16)` in Arrow and `fixed[16]` in Iceberg.
`prevhash` names the preceding exact event version and `parenthash` names the
exact event versions used to construct this one. `linkhashes`
(`LinkHashes <30013>`) lists related exact event `hash` values. The
[time-anchored hash contract](txhash.md) defines the reversible composition.

XXH3-64 composite digests remain signed `int64`. `vhash` is the clock-free
event value. `xhash` is the direct clock-free XXH3-128 digest of UTF-8 `code`.
`Instrument.xhash`, `Leg.xhash`, and `instrumentxhash` use the same operation
over `symbolticker`. A lifecycle or reference identity nested in another
identity frame enters as its stored sixteen bytes; Book value hashes keep
their native signed `int64` payloads.

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

`hash_of(*parts)` implements the scalar composite contract and returns a signed
`int64`. `hash_arrow(*columns)` produces the same `int64` values row by row.
`hash_bytes_arrow(column)` is the vectorized unframed XXH3-64 operation used
for raw message strings. `hash128_bytes_arrow(column)` returns canonical
sixteen-byte XXH3-128 digests for lifecycle codes:

- dictionary arrays are decoded to values and extension arrays use storage;
- integers are safely widened to `int64` and floats to `float64`;
- text, binary, fixed binary, boolean, numeric, and null arrays are accepted;
- every other Arrow type is refused.

Arrow's zero-copy numeric path is enabled only on little-endian hosts. It
raises clearly on a big-endian host; scalar `hash_of`, which writes byte order
explicitly, remains portable there.

The machine-readable [golden vectors](../assets/identity-v1.json) pin conversion,
frame bytes, lifecycle UTF-8 bytes, digest bits, and signed values. Python tests
validate both scalar and Arrow builders against every vector.

Changing a conversion, frame byte, algorithm parameter, or signed storage rule
requires a new protocol version. Existing v1 identifiers must never be
reinterpreted under a revised rule.
