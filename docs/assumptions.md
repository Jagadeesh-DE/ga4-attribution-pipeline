# Assumptions & Edge Cases

## 1. Lookback window: 30 days

`vars.lookback_days = 30` in `dbt_project.yml`. A touchpoint only counts toward a
conversion if it happened within 30 days before that conversion.

**Why 30, not 90 or session-only:** 30 days is the most common default across
attribution tools (GA4's own UI defaults to a 30-day click-through window for
non-paid channels), and it's short enough that the sample data's ~20-day
window exercises it meaningfully. Trade-off: a real e-commerce business with
long consideration cycles (furniture, B2B SaaS) would likely want 90 days;
that's a one-line var change, not a model rewrite.

**Edge case handled:** a conversion with no touchpoint inside the window
would silently vanish from the marts (inner join). We made this visible, not
silent, via `tests/assert_no_orphaned_conversions.sql` — it fails loudly
instead of quietly under-reporting conversions.

## 2. Identity resolution: `user_pseudo_id` only, no cross-device stitching

We attribute using GA4's device-scoped `user_pseudo_id`, not the logged-in
`user_id`. The public sample dataset has `user_id` populated for only a small
minority of rows, so joining on it would silently drop most journeys.

**Trade-off:** a user who researches on mobile and buys on desktop is treated
as two unrelated journeys, and the mobile session's touchpoint never gets
credit. This is a known, common limitation of pseudonymous web analytics
attribution, not something we tried to solve here — flagging it explicitly
rather than pretending `user_pseudo_id` is a perfect identity key.

## 3. Touchpoint definition: one per session, not one per event

A touchpoint = a `session_start` event carrying `source`/`medium`/`campaign`.
We do **not** treat every `page_view` as a separate touchpoint.

**Why:** GA4 doesn't re-fire acquisition params on every event within a
session, so per-event touchpoints would just duplicate the session's channel
N times and bias multi-page sessions toward over-representation. Session-level
touchpoints avoid that.

**Edge case:** a session with no `session_start` event captured (rare, but
possible with SDK/collection gaps) contributes zero touchpoints and can't
receive credit. Not specifically tested for in this exercise; flagging as a
known gap.

## 4. Tie-breakers

- **First-click**, exact timestamp tie between two touchpoints → lowest
  `ga_session_id` wins (the session created first).
- **Last-click**, exact timestamp tie → highest `ga_session_id` wins (the
  session created most recently).
- Implemented via `ROW_NUMBER() OVER (PARTITION BY conversion_id ORDER BY ...)`
  so the result is deterministic and stable across re-runs — required for
  idempotent, testable models.

## 5. Last-click does NOT exclude `(direct) / (none)`

Some GA4 UI reports use a "last non-direct click" rule, falling back past a
direct visit to the last real marketing channel. We deliberately used the
simpler, literal last-click rule (direct counts if it's genuinely the last
touchpoint) because:
- it's more transparent and easier to defend in a walkthrough,
- "last non-direct" is a judgment call GA4 itself doesn't apply consistently
  across all its reports.

This is a real, defensible modeling choice with a real trade-off — documented
here rather than silently picked.

## 6. Revenue attribution, not conversion-count-only

Both models attribute the full `purchase_revenue` of a conversion to a single
channel (not fractional/multi-touch credit). This is what "First-Click" and
"Last-Click" mean by definition — flagging only so it's not confused with a
weighted multi-touch model, which was explicitly out of scope per the spec.

## 7. Streaming dedupe & latency

See `streaming/stream_events_bigquery.py` docstring for the full explanation.
Short version: idempotency key = `sha256(user_pseudo_id, event_timestamp,
event_name)`, dedup enforced via `MERGE ... WHEN NOT MATCHED THEN INSERT`,
not by relying on the insert step alone (BigQuery's streaming buffer can
return duplicates for up to ~90 minutes after insert). Expected end-to-end
latency (event → dashboard): roughly 30–90 seconds, dominated by BigQuery
streaming-buffer visibility and the dashboard's refresh interval, not by dbt
compute time on these small models.

## 8. Batch window

`stg_events` processes the trailing 60 days of the batch export
(`_table_suffix` filter) rather than the full dataset, to keep dev iteration
cheap. A full historical backfill would run once with that filter removed.
