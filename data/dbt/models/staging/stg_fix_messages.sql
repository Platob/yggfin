-- One FIX message, narrowed to what the order and execution products read.
--
-- Two readings are settled here so both products state the same thing. An
-- event is dated by the transaction time the message carried, else by the
-- settled instant the parser gave it, which is never null. A session is the
-- counterparty session a message names, else the bridge session instance the
-- capture recorded it on.
--
-- `eventindex` counts a message inside its own line: a line carrying two
-- frames answers two rows, so a source position alone does not name one.

select
    sourceurl,
    rownum,
    cast(
        row_number() over (partition by sourceurl, rownum order by msghash) - 1 as integer
    ) as eventindex,
    msghash,
    msgphash,
    code,
    coalesce(transacttime, sendingtime) as eventtime,
    coalesce(sendersessionid, targetsessionid, bridgesessionid) as sessionid,
    msgtype,
    msgdirection,
    account,
    clordid,
    origclordid,
    orderid,
    parentclordid,
    parentorderid,
    symbolticker,
    isincode,
    miccode,
    side,
    ordtype,
    currency,
    price,
    orderqty,
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
from {{ source('fix', 'messages') }}
