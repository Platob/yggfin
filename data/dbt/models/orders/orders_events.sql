{{
    config(
        table='orders.events',
        mode='overwrite',
        primary_key=['eventkey'],
        not_null=[
            'orderkey',
            'eventkey',
            'eventtime',
            'timepartition',
        ],
        partition_by={'timepartition': 'day'},
        sort_by=['orderkey', 'eventtime'],
        arrow_types={
            'eventkey': 'fixed_size_binary[16]',
            'orderkey': 'fixed_size_binary[16]',
        },
    )
}}

-- One normalized order event.
--
-- A message emits one event when it carries an order identity -- a client or a
-- venue order id -- and an order lifecycle fact, which is a stated status or a
-- stated execution type. A message carrying one and not the other stays in
-- `fix.silver` rather than being assigned a guessed order.
--
-- `orderkey` is the chain the parser already named: `crossuuid` is the chain's
-- identity over `crosscode`, and `crosscode` is the first identifier the
-- message states. A message the walk could not place carries an empty
-- `crosscode`, and an unknown chain is not an order, so those are left behind.
--
-- `eventkey` is the event's own identity, which the parse settled: one message
-- logged at three hops is one event, so the key is `curruuid` itself rather
-- than a digest of where a copy of it was read from.
--
-- The event is immutable: a replace, a cancel and a reject are each an event,
-- and `orders.current` is the fold over them rather than a row rewritten here.
-- It is committed on its key, so a rebuild lands each event once however many
-- times the capture is read.

select
    crossuuid as orderkey,
    curruuid as eventkey,
    prevuuid,
    seqnum,
    srcuuids,
    eventtime,
    eventtime as timepartition,
    sessionid,
    account,
    clordid,
    origclordid,
    orderid,
    symbolticker,
    side,
    ordtype,
    px,
    qty,
    cumqty,
    leavesqty,
    avgpx,
    state,
    exectype,
    ordrejreason,
    "text"
from {{ ref('stg_fix_messages') }}
where crosscode <> ''
  and (clordid is not null or orderid is not null)
  and (ordstatus is not null or exectype is not null)
