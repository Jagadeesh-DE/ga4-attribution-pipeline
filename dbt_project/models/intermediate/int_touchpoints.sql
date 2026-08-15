-- int_touchpoints
-- A "touchpoint" = one session that carries acquisition info, taken from
-- the session_start event. This is the unit attribution models join
-- against. We use session_start (not every page_view) because GA4
-- doesn't re-fire source/medium on every event within a session, and
-- crediting the session once avoids over-counting a single visit as
-- multiple touchpoints.
--
-- Local-demo equivalent: local_demo/run_local_pipeline.py -> int_touchpoints

with sessionized as (

    select
        user_pseudo_id,
        ga_session_id,
        event_ts as touchpoint_ts,
        coalesce(source, '(direct)') as source,
        coalesce(medium, '(none)')   as medium,
        campaign,
        coalesce(source, '(direct)') || ' / ' || coalesce(medium, '(none)') as channel
    from {{ ref('stg_events') }}
    where event_name = 'session_start'

)

select * from sessionized
