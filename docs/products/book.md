# Book

Both sides of one book, flat, plus the prices that only exist across them:
`spread`, `px` (the midpoint), `vwap` and `imbalance`.

```python
from rekep import FixMsg
from rekep.market import BookIterator

lines = [
    "8=FIX.4.4|35=8|34=1|52=20260101-10:00:00.000|11=B1|37=OB1|17=E1|55=BTC-USD|54=1"
    "|44=99.50|38=5|151=5|14=0|39=0|150=0|60=20260101-10:00:00.000|10=000",
    "8=FIX.4.4|35=8|34=2|52=20260101-10:00:01.000|11=S1|37=OS1|17=E2|55=BTC-USD|54=2"
    "|44=100.50|38=4|151=4|14=0|39=0|150=0|60=20260101-10:00:01.000|10=000",
]
events = [
    event
    for line in lines
    for event in FixMsg.from_text(line).into_market_events(fix_version="4.4")
]
for book in BookIterator.from_events(events):
    print(book.bidpx, book.bidqty, book.askpx, book.askqty, book.spread, book.px)
```

```text
99.5 5.0 None None None None
99.5 5.0 100.5 4.0 1.0 100.0
```

`BookIterator` is deliberately single-threaded: order state is sequential.
Three settings bound what stays alive, and they answer different questions.

| setting | question |
| --- | --- |
| `max_order_age_ns` | how long may an order nothing has touched stay? |
| `max_side_alive` | how many per side, by price-time priority? |
| `purge_alive` | what happens to what is still resting when the *stream* ends? |

`purge_alive=True` ends each resting order as its own `INTERNAL_EXPIRED`
version, linked to the book that closed it -- a reader of the last book cannot
otherwise tell a resting order from one nobody cancelled. It is off by default,
which is what a run resumed from its snapshots wants.

Deltas and snapshots are told apart by `sunix`, not by a nullable list:

| row | `deltas`, `executions` | level lists | `bidalive`, `askalive` |
| --- | --- | --- | --- |
| delta | what changed | changed levels only | empty |
| snapshot | empty | complete living state | complete living state |

An empty delta list means nothing changed; an empty snapshot list means the
side is empty.

## Lineage

<div data-product-lineage data-product="book"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Almost every Book column is derived rather than read: a book is a fold over
[orders](order.md), not a projection of one message.
