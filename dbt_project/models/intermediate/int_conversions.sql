-- int_conversions
-- One row per purchase (conversion) event. conversion_id is a synthetic
-- key so downstream models can dedupe/join cleanly even if GA4's
-- ecommerce.transaction_id is occasionally null in the public sample.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> int_conversions

select
    user_pseudo_id || '-' || cast(event_timestamp as string) as conversion_id,
    user_pseudo_id,
    event_date  as conversion_date,
    event_ts    as conversion_ts,
    purchase_revenue
from {{ ref('stg_events') }}
where event_name = 'purchase'
