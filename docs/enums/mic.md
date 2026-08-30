# MIC

[`Ascii32`](ascii-codes.md){ .enum-base } — four bytes of printable ASCII packed left-justified into one `int32`, an open vocabulary, so a code it meets and can round-trip registers itself.

```python
from rekep.enums import MIC

venue = MIC.from_str("XPAR")
assert venue.code == "XPAR"
assert MIC.from_int(int(venue)) is venue
```

MIC accepts any four-character uppercase ISO 10383 spelling matching
`[A-Z0-9]{4}`. The spelling fills all four big-endian ASCII bytes of one
`int32`, so it needs no padding.

The table is the compiled set: the two special values, and the operating MICs a
capture keeps meeting. Compiling one is what makes it survive the registry's
eviction, render in `MIC.into_arrow_array`, and reach the `enum:values` every
contract carrying a venue publishes. A venue missing here still parses -- it
registers on first sight, like any code an open vocabulary meets.

| Key | Code | Stored value | Meaning |
| --- | --- | ---: | --- |
| `UNKNOWN` |  | 0 | No valid market identifier was present. |
| `XOFF` | `XOFF` | 1,481,590,342 | Off-market transaction. |
| `XXXX` | `XXXX` | 1,482,184,792 | No market, including an unlisted instrument. |
| `XPAR` | `XPAR` | 1,481,654,610 | Euronext Paris. |
| `XAMS` | `XAMS` | 1,480,674,643 | Euronext Amsterdam. |
| `XBRU` | `XBRU` | 1,480,741,461 | Euronext Brussels. |
| `XLIS` | `XLIS` | 1,481,394,515 | Euronext Lisbon. |
| `XMIL` | `XMIL` | 1,481,460,044 | Euronext Milan. |
| `XDUB` | `XDUB` | 1,480,873,282 | Euronext Dublin. |
| `XOSL` | `XOSL` | 1,481,593,676 | Euronext Oslo Bors. |
| `XETR` | `XETR` | 1,480,938,578 | Deutsche Boerse Xetra. |
| `XFRA` | `XFRA` | 1,481,003,585 | Frankfurt Stock Exchange. |
| `XLON` | `XLON` | 1,481,396,046 | London Stock Exchange. |
| `XSWX` | `XSWX` | 1,481,856,856 | SIX Swiss Exchange. |
| `XMAD` | `XMAD` | 1,481,457,988 | Bolsa de Madrid. |
| `XSTO` | `XSTO` | 1,481,856,079 | Nasdaq Stockholm. |
| `XCSE` | `XCSE` | 1,480,807,237 | Nasdaq Copenhagen. |
| `XHEL` | `XHEL` | 1,481,131,340 | Nasdaq Helsinki. |
| `XWBO` | `XWBO` | 1,482,113,615 | Wiener Boerse. |
| `XEUR` | `XEUR` | 1,480,938,834 | Eurex. |
| `XLME` | `XLME` | 1,481,395,525 | London Metal Exchange. |
| `IFEU` | `IFEU` | 1,229,342,037 | ICE Futures Europe. |
| `XNYS` | `XNYS` | 1,481,529,683 | New York Stock Exchange. |
| `XNAS` | `XNAS` | 1,481,523,539 | Nasdaq. |
| `ARCX` | `ARCX` | 1,095,910,232 | NYSE Arca. |
| `BATS` | `BATS` | 1,111,577,683 | Cboe BZX. |
| `XCBO` | `XCBO` | 1,480,802,895 | Cboe Options Exchange. |
| `IEXG` | `IEXG` | 1,229,281,351 | Investors Exchange. |
| `XCME` | `XCME` | 1,480,805,701 | Chicago Mercantile Exchange. |
| `XCBT` | `XCBT` | 1,480,802,900 | Chicago Board of Trade. |
| `XNYM` | `XNYM` | 1,481,529,677 | New York Mercantile Exchange. |
| `XCEC` | `XCEC` | 1,480,803,651 | Commodity Exchange. |
| `IFUS` | `IFUS` | 1,229,346,131 | ICE Futures U.S. |
| `XTSE` | `XTSE` | 1,481,921,349 | Toronto Stock Exchange. |
| `XTKS` | `XTKS` | 1,481,919,315 | Tokyo Stock Exchange. |
| `XHKG` | `XHKG` | 1,481,132,871 | Hong Kong Exchanges. |
| `XSES` | `XSES` | 1,481,852,243 | Singapore Exchange. |
| `XASX` | `XASX` | 1,480,676,184 | Australian Securities Exchange. |
| `XSHG` | `XSHG` | 1,481,852,999 | Shanghai Stock Exchange. |
| `XSHE` | `XSHE` | 1,481,852,997 | Shenzhen Stock Exchange. |
| `XKRX` | `XKRX` | 1,481,331,288 | Korea Exchange. |
| `XNSE` | `XNSE` | 1,481,528,133 | National Stock Exchange of India. |
| `XBOM` | `XBOM` | 1,480,740,685 | BSE India. |
| `XJSE` | `XJSE` | 1,481,265,989 | Johannesburg Stock Exchange. |
