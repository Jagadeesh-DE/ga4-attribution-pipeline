-- mart_channel_daily
-- Aggregates both attribution models by day + channel. This is the
-- single table the dashboard reads for: First vs Last totals,
-- the 14-day time series, and the channel breakdown.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> mart_channel_daily

with unioned as (
    select * from {{ ref('mart_attribution_first_click') }}
    union all
    select * from {{ ref('mart_attribution_last_click') }}
)

select
    attribution_model,
    conversion_date,
    attributed_channel,
    count(*) as conversions,
    round(sum(purchase_revenue), 2) as revenue
from unioned
group by 1, 2, 3
