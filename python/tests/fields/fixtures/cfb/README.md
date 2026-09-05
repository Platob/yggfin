# Synthetic Ullink `.cfb` captures

Hand-written, because no real bridge configuration is checked in. Each file
exercises one thing `Field.from_cfb` has to get right, and the tests derive
their numbers from these files rather than restating them.

| file | what it is for |
| --- | --- |
| `FX_Quoting_SellSide.cfb` | Every construct the parser reads: a vocabulary of all eight Ullink datatypes with one entry lacking `alt`; three bindings whose `type` folds to two message types (`D`, `j`); a tag used in three bindings with three different value sets; anchored alternations (one with an escaped `\?`), character classes with and without ranges, a space-separated repeat, and formats that must stay skipped (`.*`, `.-.`, `^[0-9]{4}...$`); a `condition-expression`; a constraint on a tag the vocabulary does not declare (`9999`); two vocabulary tags no grammar places (`7777`, `7778`); repeating groups nested two deep with the counter first; and the sections the parser must ignore. |
| `Rates_BuySide.cfb` | A second namespace: `SACHA` at tag 11 where the first file says `ClOrdID`, and `MaturityDate` at 5001 where the first says `GLMXTradeType`. |
| `Damaged_Capture.cfb` | Truncated mid-element. `ElementTree.ParseError` yields nothing, so one damaged file in a directory scan does not lose the others. |
| `Not_A_Capture.cfb` | Well-formed XML whose root is not `cplugin-configuration`; refused. |

Namespaces come from the file stem, normalised: `FX_Quoting_SellSide` is
`fx-quoting-sellside`.
