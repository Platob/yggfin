# Clear Street FIX fixture

| Name | FIX Tag | Allowable Values | Type | Length | Required? | Description |
| - | - | - | - | - | - | - |
| `TradeType` | `9001` | `A` `B` `E` `T` `W` | `String` | `1` | `R` | `A-Allocation` `B-Bilateral` `E-Exchange` `T-Transfer` `W-Away` |
| `RegisteredRep` | `9002` | | `String` | | `O` | Registered rep |
| `BranchOffice` | `9003` | | `String` | | `O` | Branch office |
| `ContraSideQualifier` | `9004` | `5` `6` | `Integer` | | `CR` | `5-Sell Short` `6-Sell Exempt` |
| `OmitSECFee` | `9005` | `F` `T` | `String` | `1` | `O` | SEC fee flag |
| `OmitTAFFee` | `9006` | `F` `T` | `String` | `1` | `O` | TAF fee flag |
| `LocateID` | `9007` | | `String` | | `CR` | Locate identifier |
| `LocateSource` | `9008` | | `String` | | `CR` | Locate source |
| `CancelTradeID` | `9009` | | `String` | | `CR` | Trade to cancel |
| `NSCCClearing` | `9010` | `agu` `contra` `corr` `corr_fees` `qsr` | `String` | | `O` | Clearing mode |
| `Response` | `9011` | | `String` | | `O` | ACK or NACK with reason |
