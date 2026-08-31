# Time-anchored hashes

```python
from rekep.txhash import couple128, micros_of, vhash_of, wide_bytes

vhash = -4_872_843_452_109_876_543
value = couple128(1_700_000_000_000_000, vhash)

assert micros_of(value) == 1_700_000_000_000_000
assert vhash_of(value) == vhash
assert len(wide_bytes(value)) == 16
```

Both wide identities use the same composition:

```text
hash  = couple128(unix     // 1_000, vhash)
xhash = couple128(creaunix // 1_000, hash_of(code))
```

The high signed 64 bits are epoch microseconds and the low 64 bits preserve
the signed digest bit-for-bit. `hash_of(code)` is the framed
`rekep-identity-v1` XXH3-64 digest. `CodeSource` says which field supplied the
code; it is not part of the composition.

The result is stored as sixteen big-endian two's-complement bytes:
`fixed_size_binary(16)` in Arrow and `fixed[16]` in Iceberg. Nonnegative epoch
times sort chronologically in this representation, and the composition is
reversible through `micros_of` and `vhash_of`. Both inputs must fit signed
`int64`; null on either Arrow input produces null.

`couple128_arrow` performs the same composition over `int64` columns. It emits
the stored fixed-width bytes directly and produces the same value as
`couple128` row by row.
