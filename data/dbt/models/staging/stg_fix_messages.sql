-- One FIX message, narrowed to what the order and execution products read.
--
-- Read off `fix.silver` and never `fix.bronze`: a product needs the chain --
-- the step an event follows, the state its chain reached -- and only the
-- walked rows carry one. A row here is an event and not a line: the parse
-- folds every hop that logged one message onto one `curruuid`, so a source
-- position no longer names a row and the identity does.
--
-- Three readings are settled here so both products state the same thing. An
-- event is dated by the transaction time the message carried, else by the
-- instant the walk settled on, which is never null. A session is the bridge
-- session instance the capture recorded the line on. A market fact is FIX's
-- own field, and the products' reading of one is restated here off those
-- fields: the price an event is about is what it stated, else what it last
-- traded, else what it averaged; the quantity is what it ordered, else what
-- it last traded, else what is done, else what is left. The instrument is
-- the symbol the message stated, the ISIN it stated under that source, and
-- the market it named first: its exchange, else its destination, else where
-- it last traded.

select
    sourceurl,
    rownum,
    curruuid,
    crossuuid,
    crosscode,
    prevuuid,
    seqnum,
    coalesce(transacttime, currunix) as eventtime,
    msgsessionid as sessionid,
    msgtype,
    msgdirection,
    account,
    clordid,
    origclordid,
    orderid,
    symbol,
    nullif(symbol, '[N/A]') as symbolticker,
    case
        when lower(securityidsource) in ('4', 'isin') then securityid
    end as isincode,
    coalesce(securityexchange, exdestination, lastmkt) as miccode,
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
