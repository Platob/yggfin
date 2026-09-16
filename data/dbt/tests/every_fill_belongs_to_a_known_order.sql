{{ config(severity='warn') }}

-- A fill whose chain produced no order event.
--
-- This is the roadmap's unlinked-order report, and it warns rather than fails:
-- a capture that starts mid-stream holds executions whose order was accepted
-- before the first line, and that is the capture's shape and not the build's
-- mistake. A run says how many there are; it does not stop for them.

select
    fills.orderkey,
    count(*) as occurrences,
    min(fills.executiontime) as firstat,
    max(fills.executiontime) as lastat
from {{ ref('executions_fills') }} as fills
left join {{ ref('orders_current') }} as orders on fills.orderkey = orders.orderkey
where orders.orderkey is null
group by fills.orderkey
