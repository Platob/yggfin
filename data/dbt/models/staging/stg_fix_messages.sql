-- One FIX message, narrowed to what the order and execution products read.
--
-- Read off `fix.silver` and never `fix.bronze`: a product needs the chain --
-- the step an event follows, the state its chain reached -- and only the
-- walked rows carry one. A row here is an event and not a line: the parse
-- folds every hop that logged one message onto one `curruuid`. Native
-- `srcuuids` retains the raw-line identities for a later `logs.messages` lookup.
--
-- The native lifecycle owns event time, including expiry: using an inherited
-- TransactTime would move an expired event back to the transaction it closed.
-- Normalized instrument codes and null filtering also come from the native
-- row. Product price and quantity retain their documented fallback order.

select
    curruuid,
    srcuuids,
    crossuuid,
    crosscode,
    prevuuid,
    seqnum,
    currunix as eventtime,
    msgsessionid as sessionid,
    msgtype,
    msgdirection,
    account,
    clordid,
    origclordid,
    orderid,
    symbol,
    symbol as symbolticker,
    isincode,
    miccode,
    side,
    ordtype,
    currency,
    coalesce(price, lastpx, avgpx) as px,
    coalesce(orderqty, lastqty, cumqty, leavesqty) as qty,
    cumqty,
    leavesqty,
    avgpx,
    lastqty,
    lastpx,
    execid,
    tradeid,
    ordstatus,
    exectype,
    state,
    ordrejreason,
    "text"
from {{ source('fix', 'silver') }}
