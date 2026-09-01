# Plugin

[`Ascii128`](ascii-codes.md){ .enum-base } — up to sixteen bytes of printable
ASCII stored as `fixed_size_binary[16]`.

```python
from rekep.enums import Plugin

xmlapi = Plugin.from_str("XmlApi")
assert str(xmlapi) == "XMLAPI"
assert Plugin.from_stored(xmlapi.into_stored()) is xmlapi
```

`Plugin` is an open vocabulary for bounded recording-source codes. A missing
or overwide source is stored as `UNKNOWN`; classification still sees the raw
header value before that boundary.
