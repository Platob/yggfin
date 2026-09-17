-- One FIX message, narrowed to what the order and execution products read.
--
-- Two readings are settled here so both products state the same thing. An
-- event is dated by the transaction time the message carried, else by the
-- instant the parse settled on, which is never null. A session is the bridge
-- session instance the capture recorded the line on.
--
-- A row here is an event and not a line: the parse folds every hop that
-- logged one message onto one `curruuid`, so a source position no longer
-- names a row and the identity does.

select
    sourceurl,
    rownum,
    curruuid,
    crossuuid,
    crosscode,
    prevuuid,
    seqnum,
    coalesce(transacttime, unix) as eventtime,
    msgsessionid as sessionid,
    msgtype,
    msgdirection,
    account,
    clordid,
    origclordid,
    orderid,
    symbol,
    symbolticker,
    isincode,
    miccode,
    side,
    ordtype,
    currency,
    px,
    qty,
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
