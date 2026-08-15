# Architecture — GA4 Attribution Pipeline

## Diagram

```
                    BATCH (daily)                          STREAMING (near-real-time)
                    ───────────────                        ───────────────────────────
  GA4 Public Dataset                                        stream_events_bigquery.py
  bigquery-public-data.                                     (simulates 5-20 live events)
  ga4_obfuscated_sample_ecommerce                                     │
  .events_YYYYMMDD  (sharded)                                         │ streaming insert
              │                                                       ▼
              │                                        <project>.ga4_attribution_dev
              │                                        .events_streaming
              │                                        (MERGE-deduped on event_id)
              │                                                       │
              └───────────────────────┬───────────────────────────────┘
                                       ▼
                          dbt model: stg_events
                          (UNNESTs event_params, unions batch + streaming)
                                       │
                          ┌────────────┴────────────┐
                          ▼                          ▼
              int_touchpoints              int_conversions
              (session-level channel)       (purchase events)
                          │                          │
                          └────────────┬─────────────┘
                                        ▼
                    ┌───────────────────┴───────────────────┐
                    ▼                                        ▼
        mart_attribution_first_click            mart_attribution_last_click
                    └───────────────────┬───────────────────┘
                                        ▼
                              mart_channel_daily
                          (day × channel × model, feeds dashboard)
                                        │
                                        ▼
                          dashboard/index.html
                (First vs Last totals, 14-day trend,
                 channel breakdown, live stream panel — polls every 5s)
```

## Tools

| Layer | Tool | Why |
|---|---|---|
| Warehouse | BigQuery | GA4's native export target; public sample dataset lives here |
| Transformation | dbt-bigquery | Testable, documented, incremental-friendly SQL modeling |
| Streaming demo | Python (`google-cloud-bigquery` streaming inserts + MERGE) | Mirrors GA4's own batch-vs-streaming export split without needing a live GA4 property |
| Dashboard | Static HTML/SVG/JS (no server framework) | Zero external dependencies, opens anywhere, easy to host or screen-record |
| Local fallback | Python `sqlite3` (stdlib only) | Lets the entire pipeline run and be verified with no BigQuery billing account |

## Exact dataset / table names

- **Source (real):** `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
- **Target project (yours):** `<your-gcp-project-id>.ga4_attribution_dev` (dev) / `.ga4_attribution` (prod) — set in `dbt_project/profiles.yml.example`
- **Streaming table:** `<your-project>.ga4_attribution_dev.events_streaming`
- **dbt schemas:** `staging` (views), `intermediate` (views), `marts` (tables)
- **Local fallback DB:** `local_demo/attribution.db` (SQLite), same model names as table names above

## Data flow summary

1. **Batch**: GA4 daily export lands in `events_YYYYMMDD` shards (~hours of lag, GA4-native).
2. **Streaming**: `stream_events_bigquery.py` simulates near-real-time events landing in `events_streaming`, MERGE-deduped on a deterministic `event_id`.
3. **dbt**: `stg_events` unions both sources → `int_touchpoints` / `int_conversions` → `mart_attribution_first_click` / `mart_attribution_last_click` → `mart_channel_daily`.
4. **Dashboard**: reads `mart_channel_daily` (or, locally, `dashboard/data.json` exported from SQLite) and polls every 5 seconds for near-real-time refresh.

See `docs/assumptions.md` for lookback window, identity resolution, and tie-breaker decisions.
