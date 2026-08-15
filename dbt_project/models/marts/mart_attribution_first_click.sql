-- mart_attribution_first_click
-- Credits each conversion to the EARLIEST touchpoint for that user
-- within the lookback window (var: lookback_days, default 30 — see
-- docs/assumptions.md).
--
-- Tie-breaker: if two touchpoints share the exact same timestamp
-- (possible with truncated/batched timestamps), the lower ga_session_id
-- wins, i.e. the session that was created first. This is deterministic
-- and reproducible on re-run, which matters for idempotency.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> mart_attribution_first_click

with eligible_touchpoints as (

    select
        c.conversion_id,
        c.conversion_date,
        c.conversion_ts,
        c.purchase_revenue,
        t.channel,
        t.touchpoint_ts,
        t.ga_session_id,
        row_number() over (
            partition by c.conversion_id
            order by t.touchpoint_ts asc, t.ga_session_id asc
        ) as rn
    from {{ ref('int_conversions') }} c
    join {{ ref('int_touchpoints') }} t
      on t.user_pseudo_id = c.user_pseudo_id
     and t.touchpoint_ts <= c.conversion_ts
     and t.touchpoint_ts >= timestamp_sub(c.conversion_ts, interval {{ var('lookback_days') }} day)

)

select
    conversion_id,
    conversion_date,
    purchase_revenue,
    channel as attributed_channel,
    'first_click' as attribution_model
from eligible_touchpoints
where rn = 1
