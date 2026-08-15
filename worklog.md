# Worklog

> Edit the dates/times/wording below to match when you actually worked on
> this — a worklog is one of the "did a human actually build this"
> signals, so it should read like your own notes, not a generated log.

- **Day 0, ~30 min** — Read the brief, checked GA4 public dataset access.
  No BigQuery billing account available, so decided upfront to build the
  dbt project correctly for BigQuery *and* a locally-runnable SQLite
  reproduction so I could actually verify the logic instead of shipping
  untested SQL.

- **Day 0, ~45 min** — Sketched the pipeline on paper (batch export vs
  streaming table both feeding one staging model) before writing any code.
  Landed on session-level touchpoints (from `session_start`) instead of
  per-event, to avoid over-weighting long sessions.

- **Day 1, ~1 hr** — Wrote `generate_sample_data.py` to produce GA4-shaped
  synthetic events (same column semantics as the real export) since I can't
  query the real table. Seeded it so output is reproducible.

- **Day 1, ~1.5 hr** — Built `stg_events` / `int_touchpoints` /
  `int_conversions` in both the dbt project (BigQuery syntax, UNNEST on
  event_params) and the local SQLite version. Ran the local version first
  to sanity check row counts before trusting the BigQuery SQL.

- **Day 1, ~1 hr** — Wrote the two attribution marts. First pass didn't
  have a tie-breaker on exact-timestamp ties — added
  `ORDER BY touchpoint_ts, ga_session_id` after noticing two touchpoints in
  the sample data landed in the same second.

- **Day 2, ~45 min** — Added the two custom dbt tests (revenue conservation
  across models, orphaned-conversion check). The orphan test is the one I
  care about most — a silently-dropped conversion is worse than a loud test
  failure.

- **Day 2, ~1.5 hr** — Built the streaming demo. Decided on
  `event_id = hash(user, timestamp, event_name)` + `MERGE` for dedupe after
  reading that BigQuery's streaming buffer doesn't dedupe automatically.
  Tested replay behavior locally before writing the BigQuery version.

- **Day 3, ~1.5 hr** — Built the dashboard. Went back and forth on charting
  library vs hand-rolled SVG — picked hand-rolled since it's zero
  dependencies and works by just opening the file / a static server.

- **Day 3, ~30 min** — Wrote up assumptions.md — mostly documenting
  decisions I'd already made along the way (lookback window, identity
  resolution) rather than new ones.

- **Day 3, ~45 min** — README, runbook, final pass on dbt docs/tests,
  recorded the walkthrough.
