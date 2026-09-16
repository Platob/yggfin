{{
    config(
        table='orders.events',
        mode='overwrite',
        primary_key=['eventkey'],
        not_null=[
            'orderkey',
            'sourceurl',
            'rownum',
            'eventindex',
            'msghash',
            'eventtime',
            'timepartition',
        ],
        partition_by={'timepartition': 'day'},
        sort_by=['orderkey', 'eventtime'],
        arrow_types={
            'eventkey': 'fixed_size_binary[16]',
            'orderkey': 'fixed_size_binary[16]',
            'msghash': 'fixed_size_binary[16]',
        },
    )
}}

-- One normalized order event.
--
-- A message emits one event when it carries an order identity -- a client or a
-- venue order id -- and an order lifecycle fact, which is a stated status or a
-- stated execution type. A message carrying one and not the other stays in
-- `fix.messages` rather than being assigned a guessed order.
--
-- `orderkey` is the chain the parser already named: `msgphash` is sixteen bytes
-- over `code`, and `code` scopes a client order id by the session pair it was
-- seen on. A message the lifecycle could not place carries an empty `code`, and
-- an unknown chain is not an order, so those are left behind too.
--
-- The event is immutable: a replace, a cancel and a reject are each an event,
-- and `orders.current` is the fold over them rather than a row rewritten here.
-- It is committed on its key, so a rebuild lands each event once however many
-- times the capture is read.

select
    msgphash as orderkey,
    {{ rekep_digest(['sourceurl', 'rownum', 'eventindex']) }} as eventkey,
    sourceurl,
    rownum,
    eventindex,
    msghash,
    eventtime,
    eventtime as timepartition,
    sessionid,
    account,
    clordid,
    origclordid,
    orderid,
    parentclordid,
    parentorderid,
    symbolticker,
    side,
    ordtype,
    price,
    orderqty,
    cumqty,
    leavesqty,
    avgpx,
    state,
    exectype,
    ordrejreason,
    "text"
from {{ ref('stg_fix_messages') }}
where code <> ''
  and (clordid is not null or orderid is not null)
  and (ordstatus is not null or exectype is not null)
