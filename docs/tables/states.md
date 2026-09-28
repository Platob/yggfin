# States

`state` is stored as the `int32` code of a lifecycle-sorted enum on every
table that carries it. The hundreds of a code are its rank, so the codes
order from the first state to the terminal ones and a band of ranks is one
question: below `8000` a thing can still change, `8000`-`8999` it ended having
done what was asked, `9000`-`9499` someone stopped it, `9500`-`9999` it could
not be done. A state added to a rank takes the next free number of it, so no
stored code moves.

| code | name | rank | meaning |
| ---: | --- | ---: | --- |
| 0 | `UNKNOWN` | 0 | Stated, but not a state anything reached; every other state is further along than it. |
| 1000 | `PENDING` | 10 | Asked for, with nothing more said. |
| 1001 | `PENDING_NEW` | 10 | A new order asked for and not yet acknowledged: FIX `PendingNew`, and what a `NewOrderSingle` asks for. |
| 1002 | `QUEUED` | 10 | Waiting its turn. |
| 1003 | `RECEIVED` | 10 | Received, not yet processed: an execution, allocation, confirmation or affirmation the counterparty has and has not yet answered. |
| 1004 | `PENDING_VERIFICATION` | 10 | A trade report waiting on its verification. |
| 1005 | `PENDING_ALLOCATION` | 10 | An allocation asked for and not yet made. |
| 1006 | `PENDING_APPROVAL` | 10 | A give-up or take-up waiting on its approval. |
| 2000 | `ACCEPTED` | 20 | Accepted, not yet working: FIX `AcceptedForBidding`, an accepted trade report, quote or mass action. |
| 2001 | `NEW` | 20 | Acknowledged by the venue as a new order. |
| 2002 | `STARTING` | 20 | About to start. |
| 2003 | `SUBMITTED` | 20 | Handed over to whoever does it. |
| 2004 | `ACKNOWLEDGED` | 20 | Acknowledged by the counterparty: an execution it accepted. |
| 3000 | `RUNNING` | 30 | Working. |
| 3001 | `STATUS` | 30 | A status report: working, with nothing new to say. |
| 3002 | `TRIGGERED` | 30 | Triggered or activated by the system. |
| 3003 | `ACTIVE` | 30 | A quote standing in the market. |
| 3004 | `UPDATED` | 30 | Stated anew over a live predecessor, and carrying on. |
| 4000 | `IN_PROGRESS` | 40 | Working, with some of it done. |
| 4001 | `PARTIALLY_FILLED` | 40 | Some of the order filled. |
| 4002 | `TRADE` | 40 | A report of one trade. |
| 4003 | `TRADE_CORRECT` | 40 | A correction of a trade reported before. |
| 4004 | `TRADE_CANCEL` | 40 | A cancellation of a trade reported before. |
| 4005 | `TRADE_IN_CLEARING_HOLD` | 40 | A trade held before clearing. |
| 5000 | `PAUSED` | 50 | Halted by its owner. |
| 5001 | `STOPPED` | 50 | Stopped, and able to resume. |
| 5002 | `SUSPENDED` | 50 | Suspended by the venue. |
| 5003 | `LOCKED` | 50 | Locked by the venue: FIX `ExecType` `Locked`. |
| 5004 | `DISPUTED` | 50 | A trade report its counterparty disputes. |
| 5005 | `INCOMPLETE` | 50 | An allocation missing some of what it needs. |
| 6000 | `PENDING_CANCEL` | 60 | A cancel asked for and not yet answered. |
| 6001 | `PENDING_REPLACE` | 60 | A replace asked for and not yet answered. |
| 6002 | `PENDING_REVERSAL` | 60 | A reversal asked for and not yet made. |
| 7000 | `REPLACED` | 70 | Replaced, and the replacement carries on. |
| 7001 | `RESTATED` | 70 | Restated by the venue, and carrying on. |
| 7002 | `AMENDED` | 70 | Amended, and carrying on. |
| 7003 | `RELEASED` | 70 | Released from a lock: FIX `ExecType` `Released`. |
| 8000 | `CALCULATED` | 80 | Its value calculated. |
| 8001 | `COMPLETE` | 80 | Complete. |
| 8002 | `DONE_FOR_DAY` | 80 | Done for the day. |
| 8003 | `FILLED` | 80 | All of the order filled. |
| 8004 | `SUCCEEDED` | 80 | Succeeded. |
| 8005 | `TRADE_RELEASED_TO_CLEARING` | 80 | A trade released to clearing. |
| 8006 | `ALLOCATED` | 80 | An allocation accepted and made. |
| 8007 | `CONFIRMED` | 80 | A trade confirmed. |
| 8008 | `AFFIRMED` | 80 | A confirmation affirmed. |
| 8009 | `VERIFIED` | 80 | A trade report verified, or deemed verified. |
| 8010 | `CLEARED` | 80 | Cleared. |
| 8011 | `SETTLED` | 80 | Settled. |
| 8012 | `CLAIMED` | 80 | An allocation claimed. |
| 9000 | `CANCELED` | 90 | Cancelled by someone. |
| 9001 | `REVERSED` | 90 | An allocation reversed. |
| 9002 | `REMOVED` | 90 | A quote removed from the market. |
| 9003 | `TERMINATED` | 90 | A trade report terminated, or a contract terminated under a quote. |
| 9500 | `EXPIRED` | 95 | Its time ran out. |
| 9501 | `FAILED` | 95 | Failed. |
| 9502 | `REJECTED` | 95 | Refused. |
| 9503 | `TIMED_OUT` | 95 | Took too long. |
| 9504 | `DONT_KNOW` | 95 | An execution the counterparty does not know: FIX's DK. |
| 9505 | `MISMATCHED` | 95 | A confirmation whose account or settlement instructions do not match. |
| 9506 | `NOT_FOUND` | 95 | A quote the venue does not know. |
