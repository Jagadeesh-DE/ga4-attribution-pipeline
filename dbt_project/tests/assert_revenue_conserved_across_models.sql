-- Custom singular dbt test.
-- Every conversion must be attributed to exactly one channel under each
-- model, so total revenue summed across all channels must be IDENTICAL
-- between first_click and last_click (only the channel split differs).
-- If this test returns any rows, the join in one of the mart models is
-- fanning out or dropping conversions.

with totals as (
    select attribution_model, round(sum(purchase_revenue), 2) as total_revenue
    from {{ ref('mart_channel_daily') }}
    group by 1
)

select *
from (
    select
        (select total_revenue from totals where attribution_model = 'first_click') as first_click_total,
        (select total_revenue from totals where attribution_model = 'last_click')  as last_click_total
)
where first_click_total != last_click_total
