# Plugin

[`Ascii64`](ascii-codes.md){ .enum-base } — up to eight bytes of printable
ASCII packed left-justified into one `int64`.

```python
from rekep.enums import Plugin

xmlapi = Plugin.from_str("XmlApi")
assert str(xmlapi) == "XMLAPI"
assert Plugin.from_int(int(xmlapi)) is xmlapi
```

`Plugin` is an open vocabulary for deployments that assign bounded plugin
codes. Raw `Message.plugin` provenance remains a string because captured
logger names such as `OMSSales_Enrichment` are not bounded to eight bytes.
