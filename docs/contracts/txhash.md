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
without framing or copying.
