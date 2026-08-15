# Before you submit — what you still need to do yourself

I can generate the code and docs, but a few required deliverables are
specifically designed to prove *you* did the work interactively. Faking
these defeats their purpose, so here's what's left and how to do it
genuinely and quickly.

## 1. Git commit history (incremental, not one big commit)

Don't `git add . && git commit -m "initial commit"`. Commit in the order
you actually build/review things — e.g.:

```bash
git init
git add local_demo/generate_sample_data.py
git commit -m "Add synthetic GA4-shaped sample data generator"

git add local_demo/run_local_pipeline.py
git commit -m "Local SQLite pipeline: staging + intermediate models"

git add dbt_project/models/staging dbt_project/models/intermediate
git commit -m "dbt: staging + intermediate models (BigQuery)"

git add dbt_project/models/marts dbt_project/tests
git commit -m "Attribution marts: first-click, last-click, custom tests"

git add streaming/
git commit -m "Streaming demo with idempotent MERGE dedupe"

git add dashboard/
git commit -m "Dashboard: totals, 14-day trend, channel breakdown, live panel"

git add docs/ README.md worklog.md
git commit -m "Docs: architecture, assumptions, README, runbook"
```

Spread these across actual working sessions (spread over your real Day
0–3) rather than firing them all in one sitting — the timestamps are part
of what an evaluator checks.

## 2. Two sketches/notes (photos OK)

Hand-draw and photograph, e.g.:
- The architecture diagram (batch export + streaming table → staging →
  intermediate → marts → dashboard) — a rougher version of
  `docs/architecture.md`'s diagram, in your own handwriting/boxes.
- The first-click/last-click tie-breaker logic, or the touchpoint
  definition decision (session-level vs per-event) — whatever you actually
  reasoned through on paper.

These are meant to look like real working notes, not a polished repro of
the typed docs — messy is fine and expected.

## 3. Practice the live 15-minute SQL edit

Know `mart_attribution_last_click.sql` and `mart_attribution_first_click.sql`
well enough to make a live change under pressure — e.g. be ready to:
- change the lookback window on the fly (`var('lookback_days')`)
- add a channel-exclusion rule (e.g. exclude `(direct)/(none)` from
  last-click, discussed as a rejected option in `docs/assumptions.md`)
- change the tie-breaker order

## 4. Demo script (5–8 min) — suggested beats

1. Architecture diagram — 30s narration of batch vs streaming paths.
2. `dbt run` + `dbt test` passing (or the local equivalent) — 1 min.
3. Open `mart_attribution_first_click.sql` vs `_last_click.sql` side by
   side, point at the one line that differs (sort direction) — 1 min.
4. Run `streaming/stream_events_local_demo.py --replay_last` live, showing
   the dedupe count — 1–2 min.
5. Dashboard: point out the paired bars where first-click and last-click
   disagree on channel credit — 1–2 min.
6. One assumption you'd revisit with more time (e.g. identity resolution) — 30s.
