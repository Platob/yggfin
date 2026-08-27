# Time-anchored hashes

```python
from rekep.txhash import digest_of, h64, seconds_of, xxh32_of

value = h64(1_700_000_000, b"payload")
assert seconds_of(value) == 1_700_000_000
assert digest_of(value) == xxh32_of(b"payload")
```

`rekep.txhash` couples a row's epoch seconds with the XXH32 of its payload
into one signed `int64`: the four high bytes are the seconds as a signed
`int32`, the four low bytes the digest. Comparing two txhashes compares their
times first, so a column sorts by time while still spreading rows hashed
within one second -- one value that is both an identity and a sort key.

The couple is exact and reversible: `seconds_of` and `digest_of` read the
halves back. A null on either input is a null txhash; a clock outside `int32`
refuses loudly rather than wrapping.

The Arrow kernels -- `h64_arrow` over a seconds column and a payload column,
`seconds_arrow` and `digest_arrow` for the halves -- produce bit-identical
values to the Python builders row for row, walking the payload buffer once
without framing or copying. The wide kernels below mirror them.

## One hundred and twenty-eight bits

`h128` couples epoch **microseconds** over an XXH64 digest: the clock keeps
the high sixty-four bits, so a column still orders by time, with microsecond
resolution and a digest wide enough that a collision inside one microsecond
is not something a feed meets.

```python
from rekep.txhash import digest64_of, h128, micros_of

wide = h128(1_700_000_000_000_000, b"payload")
assert micros_of(wide) == 1_700_000_000_000_000
assert digest64_of(wide) < 1 << 64
```

A column stores it as its sixteen big-endian bytes, `fixed_size_binary(16)`,
which is what every identifier in the package is stored as -- so it still
sorts by time. `micros_of` and `digest64_of` read the halves back, and
`micros_arrow` and `digest64_arrow` do the same for a column.

Every event identity is this hash: `Event.txhash_of` frames a version's parts
and anchors them, and `txhash_framed` anchors a frame a shape already has --
which is how a book, whose live sides are cached frames, gets the same value
without rebuilding them.

The wide builders mirror the narrow ones over the same framing:
`h128_arrow_arrays` over several columns and a clock, `h128_arrow_batch`
with selectors, and `h128_dataclass` for one row.
