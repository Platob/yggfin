# Time-anchored hashes

```python
from rekep.txhash import couple128, micros_of, vhash_of, wide_bytes

vhash = -4_872_843_452_109_876_543
value = couple128(1_700_000_000_000_000, vhash)

assert micros_of(value) == 1_700_000_000_000_000
assert vhash_of(value) == vhash
assert len(wide_bytes(value)) == 16
```

An event `hash` is the exact composition
`(unix // 1_000 << 64) | (vhash & ((1 << 64) - 1))`. The high signed
64 bits are epoch microseconds and the low 64 bits preserve the signed
`vhash` bit-for-bit. Composition does not hash the value again.

The result is stored as sixteen big-endian two's-complement bytes:
`fixed_size_binary(16)` in Arrow and `fixed[16]` in Iceberg. Nonnegative epoch
times sort chronologically in this representation, and the composition is
reversible through `micros_of` and `vhash_of`. Both inputs must fit signed
`int64`; null on either Arrow input produces null.

`couple128_arrow` performs the same composition over `int64` columns. It emits
the stored fixed-width bytes directly and produces the same value as
`couple128` row by row.

## Narrow hashes

```python
from rekep.txhash import digest_of, h64, seconds_of, xxh32_of

value = h64(1_700_000_000, b"payload")
assert seconds_of(value) == 1_700_000_000
assert digest_of(value) == xxh32_of(b"payload")
```

`h64` remains the separate seconds-over-XXH32 contract for callers that need
one `int64`. Its high signed `int32` stores epoch seconds and its low 32 bits
store the payload digest. `h64_arrow`, `seconds_arrow`, and `digest_arrow`
provide the matching Arrow operations.
