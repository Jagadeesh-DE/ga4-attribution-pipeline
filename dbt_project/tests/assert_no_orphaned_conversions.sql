-- Custom singular dbt test.
-- A conversion with NO touchpoint inside the lookback window (e.g. a
-- user whose only session_start happened >30 days before they bought)
-- would silently disappear from the mart_attribution_* models because
-- the join in those models is an INNER JOIN. That's a real edge case
-- (see docs/assumptions.md) but we want it to be VISIBLE, not silent.
-- This test fails loudly if the drop rate is unexpectedly high, which
-- is the signal that the lookback window may need widening.

select
    c.conversion_id
from {{ ref('int_conversions') }} c
left join {{ ref('mart_attribution_last_click') }} m
    on m.conversion_id = c.conversion_id
where m.conversion_id is null
