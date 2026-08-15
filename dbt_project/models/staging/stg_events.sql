-- stg_events
-- Flattens the raw nested GA4 export (event_params is a REPEATED STRUCT)
-- into one clean row per event, pulling out only the fields attribution
-- needs. Unions the daily batch export with the streaming demo table so
-- downstream models see one continuous event stream.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> stg_events
-- (that version works off a pre-flattened CSV since SQLite has no UNNEST)

with batch_events as (

    select
        event_date,
        event_timestamp,                                   -- microseconds since epoch
        timestamp_micros(event_timestamp) as event_ts,
        event_name,
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')  as ga_session_id,
        -- GA4's public sample dataset carries acquisition info on traffic_source
        -- (session-level) rather than per-event_params in most rows; we fall back
        -- to event_params 'source'/'medium'/'campaign' when present (e.g. UTM-tagged
        -- landing events), since those are more precise for multi-touch journeys.
        coalesce(
            (select value.string_value from unnest(event_params) where key = 'source'),
            traffic_source.source
        ) as source,
        coalesce(
            (select value.string_value from unnest(event_params) where key = 'medium'),
            traffic_source.medium
        ) as medium,
        coalesce(
            (select value.string_value from unnest(event_params) where key = 'campaign'),
            traffic_source.name
        ) as campaign,
        ecommerce.purchase_revenue as purchase_revenue,
        'batch' as _source

    from {{ source('ga4_public', 'events_*') }}
    -- ASSUMPTION: process trailing 60 days of batch export; adjust via
    -- _table_suffix filter for full backfills.
    where _table_suffix between
        format_date('%Y%m%d', date_sub(current_date(), interval 60 day))
        and format_date('%Y%m%d', current_date())

),

streaming_events as (

    select
        event_date,
        event_timestamp,
        timestamp_micros(event_timestamp) as event_ts,
        event_name,
        user_pseudo_id,
        ga_session_id,
        source,
        medium,
        campaign,
        purchase_revenue,
        'streaming' as _source
    from {{ source('ga4_public', 'events_streaming') }}

)

select * from batch_events
union all
select * from streaming_events
