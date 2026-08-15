# GA4 Near-Real-Time Attribution Pipeline

First-click and last-click attribution on top of the GA4 public sample
dataset, built with BigQuery + dbt, with a streaming demo and a live
dashboard prototype.

**No BigQuery billing account was available while building this**, so
everything here is provided two ways:
1. **Real dbt project** in `dbt_project/`, written correctly against
   `bigquery-public-data.ga4_obfuscated_sample_ecommerce` — ready to run
   as-is once pointed at a billing-enabled GCP project.
2. **A fully local, actually-executed reproduction** in `local_demo/` +
   `streaming/` + `dashboard/`, using Python's built-in `sqlite3` (zero
   install required) so every number in this repo is real, verified output —
   not just untested SQL.

See `docs/architecture.md` for the diagram and `docs/assumptions.md` for
every modeling decision (lookback window, identity resolution, tie-breakers).

---

## Quick start — local (no BigQuery needed, works anywhere)

```bash
# 1. Generate synthetic GA4-shaped sample data (40 users, 20 days)
cd local_demo
python3 generate_sample_data.py        # writes events.csv

# 2. Run the full pipeline (stg -> int -> mart), same logic as dbt_project/
python3 run_local_pipeline.py          # writes attribution.db, prints validation output

# 3. Stream 5-20 "live" events in, with idempotent dedupe
cd ../streaming
python3 stream_events_local_demo.py --n_events 12 --replay_last
#  --replay_last re-sends the same batch to prove duplicates are dropped

# 4. Export data for the dashboard and open it
cd ../dashboard
python3 export_dashboard_data.py       # writes data.json
python3 -m http.server 8000            # serve locally (fetch() needs http, not file://)
# open http://localhost:8000 in a browser
```

Re-run step 3 + `export_dashboard_data.py` while the dashboard tab is open —
it polls `data.json` every 5 seconds, so the "Live Stream Panel" updates
without a page refresh.

## Quick start — real BigQuery (once you have a billing-enabled project)

```bash
pip install dbt-bigquery google-cloud-bigquery
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml   # edit YOUR project id
gcloud auth application-default login

cd dbt_project
dbt deps          # if packages.yml is added later
dbt run
dbt test
dbt docs generate && dbt docs serve

# stream some live events in
cd ../streaming
python3 stream_events_bigquery.py --project YOUR_PROJECT --n_events 12
cd ../dbt_project && dbt run --select stg_events+   # pick up the new streamed rows
```

Point a BI tool (Looker Studio / Metabase) at `mart_channel_daily` and
`events_streaming` for the hosted version of the dashboard, or adapt
`dashboard/index.html` to query BigQuery via a small API instead of
`data.json`.

---

## Repo layout

```
docs/                    architecture.md, assumptions.md
dbt_project/              real dbt project, targets BigQuery
  models/staging/          stg_events (unnest + union batch/streaming)
  models/intermediate/     int_touchpoints, int_conversions
  models/marts/            mart_attribution_first_click / _last_click / _channel_daily
  tests/                   2 custom singular tests (revenue conservation, orphan check)
local_demo/               fully offline, actually-executed reproduction (SQLite)
streaming/                streaming demo: BigQuery version + local-runnable version
dashboard/                self-contained HTML/JS dashboard + data export script
worklog.md                incremental build log
```

---

## Failure handling

| Failure | Handling |
|---|---|
| Streaming insert fails / retried | Idempotency key (`event_id` = hash of user+timestamp+event) + `MERGE` means retries are safe no-ops. Never double-counts. |
| Conversion has no touchpoint in lookback window | Would silently disappear via inner join — caught explicitly by `tests/assert_no_orphaned_conversions.sql`, which fails the `dbt test` run instead of failing silently. |
| First-click / last-click revenue totals diverge | Should be mathematically impossible (every conversion attributed exactly once per model) — caught by `tests/assert_revenue_conserved_across_models.sql`. |
| Batch source late / not yet landed for today | `stg_events` windows over `_table_suffix` for the trailing 60 days; a missing latest-day shard just means today's conversions aren't attributed yet, not a pipeline error. Source freshness check (`freshness:` block in `_ga4_sources.yml`) would alert if the batch export itself goes stale (>26h). |
| Dashboard can't reach `data.json` | Shown inline ("fetch failed — serve via `python3 -m http.server`") rather than a blank page — `file://` URLs block `fetch()` in most browsers. |

## Monitoring suggestions (for the real BigQuery deployment)

- **dbt source freshness**: `dbt source freshness` on `ga4_public.events_*`, scheduled hourly; alert if the batch export is >26h stale.
- **Test failures as monitors**: schedule `dbt test` after every `dbt run` (e.g. Cloud Composer / GitHub Actions cron); route failures of the two custom tests above to a Slack webhook — they're specifically designed to catch silent data-quality regressions, not just schema drift.
- **Streaming lag**: track `TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ingested_at, SECOND)` on the newest row in `events_streaming`; alert if it exceeds a few minutes (would indicate the streaming source stopped sending).
- **Row-count sanity**: a simple `dbt_utils.recency` or row-count-vs-7-day-average test on `mart_channel_daily` catches a source going silent even if freshness checks miss it.

## Cost notes

- **Storage**: the public dataset itself costs nothing extra beyond your own derived tables; `mart_*` tables here are tiny (day × channel granularity).
- **Query cost**: `stg_events` scans the batch export filtered to trailing 60 days via `_table_suffix` pruning — BigQuery only bills for the partitions/shards actually scanned, so this stays cheap even though the source dataset is large.
- **Streaming inserts**: `tabledata.insertAll` is billed per row inserted, not per byte scanned — 5–20 rows per demo run is effectively free. At real GA4 traffic volumes, this is the line item worth watching.
- **Recommendation**: materialize `stg_events` and `int_*` as **views** (as configured in `dbt_project.yml`) rather than tables — they're cheap to query and avoid paying storage + recompute for intermediate data; only the small `mart_*` layer is materialized as tables.
