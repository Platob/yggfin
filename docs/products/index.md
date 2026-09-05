# Products

Six persisted contracts. One physical line remains raw until the FIX registry
transcribes it, and a market event remains flat until a fold gives it state.

```mermaid
flowchart LR
  T[log text] --> M[Message] --> F[FixMsg]
  F --> I[InstUpdate]
  F --> O[Order]
  F --> E[Execution]
  O --> B[Book]
```

| product | one row is | built by |
| --- | --- | --- |
| [Message](message.md) | one physical source line, uninterpreted | `Message.from_text` |
| [FixMsg](fixmsg.md) | one line transcribed under the registry | `FixMsg.from_message_batch` |
| [Instrument update](instrument.md) | one current reference-data event | `InstUpdate.from_fixmsgs` |
| [Order](order.md) | one version of one order | `FixMsg.into_market_events` |
| [Execution](execution.md) | one fill, correction or cancellation | `FixMsg.into_market_events` |
| [Book](book.md) | both sides of one book, flat | `BookIterator.from_events` |

Raw `Message` is keyed by `(sourceurl, sourcerownum)` and is unpartitioned.
`FixMsg`, Book, Order, and Execution are keyed by `(unix, hash)`; the current
`InstUpdate` row is keyed by its sixteen-byte `xhash`. `vhash` is the
clock-free `int64` value identity, while `hash` and `xhash` are sixteen-byte
identities. The five parsed products partition on `unixpartition`. None
declares a physical sort order; ordered readers request `(unix, hash)` or the
protocol sequence explicitly:

```bash
rekep fields load --target schemas/rekep/order.yaml | tail -2
```

```text
  primary keys: ['unix', 'hash']
  partition keys: {'unixpartition': 'identity'}
```

Declarations are dumped from the classes, so a document is never the contract:

```bash
rekep fields dump --pyclass rekep.market.orders:Order --target schemas/rekep/order.yaml
```

See [schema contracts](../contracts/index.md) for the document format,
[types](../contracts/types.md) for the Arrow types it may name, and
[binary identities](../contracts/identity.md) for the lifecycle and event hash contracts.
