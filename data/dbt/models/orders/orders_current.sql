{{
    config(
        table='orders.current',
        mode='overwrite',
        primary_key=['orderkey'],
        not_null=['last_eventkey', 'updatedat', 'eventcount'],
        sort_by=['orderkey'],
        arrow_types={
            'orderkey': 'fixed_size_binary[16]',
            'last_eventkey': 'fixed_size_binary[16]',
        },
    )
}}

-- The latest settled state of one logical order.
--
-- Built from `orders.events` alone and never from FIX: a late event is
-- appended there and changes this row only through the ordering below, which
-- is event time, then lifecycle sequence and native event identity. That
-- ordering is total, so the same events fold to the same row.
--
-- The row is overwritten on its key rather than appended, so this table holds
-- one row per order however many times it is rebuilt.
--
-- It is not partitioned: one row per order is the whole table, and a partition
-- on a column that moves would rewrite a file every time an order ticks.

with folded as (

    select
        orderkey,
        count(*) as eventcount,
        min(eventtime) as firstat,
        min(eventtime) filter (
            where state in {{ rekep_opening_states() }}
        ) as acceptedat,
        max(eventtime) filter (
            where state in {{ rekep_terminal_states() }}
        ) as closedat
    from {{ ref('orders_events') }}
    group by orderkey

),

latest as (

    select *
    from {{ ref('orders_events') }}
    qualify row_number() over (
        partition by orderkey
        order by eventtime desc, seqnum desc, eventkey desc
    ) = 1

)

select
    latest.orderkey,
    latest.eventkey as last_eventkey,
    coalesce(folded.acceptedat, folded.firstat) as openedat,
    latest.eventtime as updatedat,
    folded.closedat,
    latest.sessionid,
    latest.account,
    latest.clordid,
    latest.origclordid,
    latest.orderid,
    latest.symbolticker,
    latest.side,
    latest.ordtype,
    latest.px,
    latest.qty,
    latest.cumqty,
    latest.leavesqty,
    latest.avgpx,
    latest.state,
    latest.exectype,
    folded.eventcount
from latest
inner join folded on latest.orderkey = folded.orderkey
