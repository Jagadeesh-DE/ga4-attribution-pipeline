-- mart_attribution_last_click
-- Credits each conversion to the LATEST touchpoint at or before the
-- conversion, within the lookback window. Mirrors mart_attribution_first_click
-- but with sort order reversed.
--
-- Tie-breaker: highest ga_session_id wins on an exact timestamp tie
-- (the most recently created session).
--
-- NOTE (assumption, see docs/assumptions.md): we do NOT exclude
-- '(direct) / (none)' from being a valid last-click channel, unlike
-- some GA4 UI reports which fall back to the last *non-direct* click.
-- We chose the simpler, more transparent rule for this exercise and
-- documented the trade-off explicitly.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> mart_attribution_last_click

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
            order by t.touchpoint_ts desc, t.ga_session_id desc
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
    'last_click' as attribution_model
from eligible_touchpoints
where rn = 1
