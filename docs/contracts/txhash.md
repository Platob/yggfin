# Time-anchored hashes

```python
from rekep.txhash import couple128, micros_of, vhash_of, wide_bytes

vhash = -4_872_843_452_109_876_543
value = couple128(1_700_000_000_000_000, vhash)

print(micros_of(value) == 1_700_000_000_000_000)
print(vhash_of(value) == vhash)
print(len(wide_bytes(value)))
```

```text
True
True
16
```

Event hashes use `hash = couple128(unix // 1_000, vhash)`.

The high signed 64 bits are epoch microseconds and the low 64 bits preserve
the signed digest bit-for-bit.

The result is stored as sixteen big-endian two's-complement bytes:
`fixed_size_binary(16)` in Arrow and `fixed[16]` in Iceberg. Nonnegative epoch
times sort chronologically in this representation, and the composition is
reversible through `micros_of` and `vhash_of`. Both inputs must fit signed
`int64`; null on either Arrow input produces null.

`couple128_arrow` performs the same composition over `int64` columns. It emits
the stored fixed-width bytes directly and produces the same value as
`couple128` row by row.
