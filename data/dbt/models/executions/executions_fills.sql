{{
    config(
        table='executions.fills',
        mode='overwrite',
        primary_key=['executionkey'],
        not_null=[
            'eventkey',
            'orderkey',
            'sourceurl',
            'rownum',
            'eventindex',
            'msghash',
            'executiontime',
            'timepartition',
            'lastqty',
            'lastpx',
        ],
        partition_by={'timepartition': 'day'},
        sort_by=['orderkey', 'executiontime'],
        arrow_types={
            'executionkey': 'fixed_size_binary[16]',
            'eventkey': 'fixed_size_binary[16]',
            'orderkey': 'fixed_size_binary[16]',
            'msghash': 'fixed_size_binary[16]',
        },
    )
}}

-- One economic execution occurrence.
--
-- A message becomes a row when it states a quantity and a price this
-- occurrence executed at. A status-only report states neither and does not
-- become a zero fill; a stated quantity with no price is not an occurrence
-- this product can price, and both stay in `fix.messages`.
--
-- `executionkey` scopes the venue's execution id by the chain it arrived in.
-- A bridge relays one execution into every chain it belongs to and logs the
-- copy it received beside the copy it sent, so an execution id alone names
-- several rows; scoped by the chain, each order sees its own occurrence once.
-- The earliest source position wins, so which copy is kept does not depend on
-- the order the rows were read in, and the row is committed on its key so a
-- rebuild lands each occurrence once.
--
-- `exectype` says whether the occurrence is a trade, a correction or a cancel.
-- Corrections and cancels are rows of their own, and the settled quantity is
-- the chain's, not this row's: `cumqty`, `leavesqty` and `avgpx` are what the
-- message stated and are audit checks rather than the occurrence.

select
    case
        when execid is not null then {{ rekep_digest(['code', 'execid']) }}
        else {{ rekep_digest(['sourceurl', 'rownum', 'eventindex']) }}
    end as executionkey,
    {{ rekep_digest(['sourceurl', 'rownum', 'eventindex']) }} as eventkey,
    msgphash as orderkey,
    sourceurl,
    rownum,
    eventindex,
    msghash,
    eventtime as executiontime,
    eventtime as timepartition,
    sessionid,
    execid,
    tradeid,
    clordid,
    orderid,
    symbolticker,
    isincode,
    miccode,
    side,
    lastqty,
    lastpx,
    currency,
    cumqty,
    leavesqty,
    avgpx,
    exectype,
    state
from {{ ref('stg_fix_messages') }}
where code <> ''
  and lastqty is not null
  and lastqty <> 0
  and lastpx is not null
qualify row_number() over (
    partition by executionkey
    order by eventtime, sourceurl, rownum, eventindex
) = 1
