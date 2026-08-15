"""
export_dashboard_data.py
--------------------------
Reads local_demo/attribution.db (built by run_local_pipeline.py, updated
by streaming/stream_events_local_demo.py) and writes dashboard/data.json,
which dashboard/index.html loads directly (no server required).

In the real BigQuery version, this step is replaced by the dashboard
tool (Looker Studio / a small Cloud Run API) querying mart_channel_daily
and a `SELECT * FROM events_streaming ORDER BY ingested_at DESC LIMIT 20`
live panel query directly - see docs/README.md.

Usage: python3 export_dashboard_data.py
"""
import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "local_demo", "attribution.db")
OUT_PATH = os.path.join(os.path.dirname(__file__), "data.json")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

channel_daily = [dict(r) for r in conn.execute("SELECT * FROM mart_channel_daily")]

totals = {}
for row in conn.execute("""
    SELECT model, COUNT(*) as conversions, ROUND(SUM(purchase_revenue),2) as revenue
    FROM (SELECT * FROM mart_attribution_first_click UNION ALL SELECT * FROM mart_attribution_last_click)
    GROUP BY model
"""):
    totals[row["model"]] = {"conversions": row["conversions"], "revenue": row["revenue"]}

channel_breakdown = {"first_click": [], "last_click": []}
for model, tbl in [("first_click", "mart_attribution_first_click"), ("last_click", "mart_attribution_last_click")]:
    for row in conn.execute(f"""
        SELECT attributed_channel, COUNT(*) as conversions, ROUND(SUM(purchase_revenue),2) as revenue
        FROM {tbl} GROUP BY attributed_channel ORDER BY revenue DESC
    """):
        channel_breakdown[model].append(dict(row))

# live streaming panel: most recent events, if the streaming table exists
live_events = []
try:
    for row in conn.execute("""
        SELECT event_name, user_pseudo_id, source, medium, purchase_revenue, ingested_at
        FROM events_streaming ORDER BY ingested_at DESC LIMIT 20
    """):
        live_events.append(dict(row))
except sqlite3.OperationalError:
    pass  # no streamed events yet - fine, panel just shows empty state

payload = {
    "generated_at": __import__("datetime").datetime.now().isoformat(),
    "totals": totals,
    "channel_daily": channel_daily,
    "channel_breakdown": channel_breakdown,
    "live_events": live_events,
}

with open(OUT_PATH, "w") as f:
    json.dump(payload, f, indent=2, default=str)

print(f"Wrote {OUT_PATH}")
print(f"  totals: {totals}")
print(f"  channel_daily rows: {len(channel_daily)}")
print(f"  live_events: {len(live_events)}")
