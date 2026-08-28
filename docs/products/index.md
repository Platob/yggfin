# Products

Six persisted contracts. A row is text until the FIX registry transcribes it,
and a market product until a fold gives it state.

```mermaid
flowchart LR
  T[log text] --> M[Message] --> F[FixMsg]
  F --> I[Instrument]
  F --> O[Order]
  F --> E[Execution]
  O --> B[Book]
```

| product | one row is | built by |
| --- | --- | --- |
| [Message](message.md) | one log line, tokenized | `Message.from_text` |
| [FixMsg](fixmsg.md) | one line transcribed under the registry | `FixMsg.from_message_batch` |
| [Instrument](instrument.md) | one version of an instrument's facts | `Instrument.from_fixmsgs` |
| [Order](order.md) | one version of one order | `FixMsg.into_market_events` |
| [Execution](execution.md) | one fill, correction or cancellation | `FixMsg.into_market_events` |
| [Book](book.md) | both sides of one book, flat | `BookIterator.from_events` |

Every one of them is keyed `(unix, hash)`, sorted by `unix` and partitioned on
`unix_partition` alone:

```bash
rekep fields load --target schemas/rekep/order.yaml | tail -2
```

```text
  primary keys: ['unix', 'hash']
  partition keys: {'unix_partition': 'identity'}
```

Declarations are dumped from the classes, so a document is never the contract:

```bash
rekep fields dump --pyclass rekep.market.orders:Order --target schemas/rekep/order.yaml
```

See [schema contracts](../contracts/index.md) for the document format,
[types](../contracts/types.md) for the Arrow types it may name, and
[binary identities](../contracts/identity.md) for how `hash` and `xhash` are framed.
