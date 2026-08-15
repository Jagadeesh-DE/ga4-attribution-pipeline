"""
run_local_pipeline.py
----------------------
Runs the SAME transformation logic as the dbt project
(dbt_project/models/...) against a local SQLite database loaded from
events.csv. This is the "runnable SQL + sample data" fallback required
when BigQuery billing isn't available.

Every SQL step here has a 1:1 counterpart .sql file in dbt_project/models/.
Where SQLite syntax differs from BigQuery (e.g. UNNEST on event_params
arrays, DATETIME functions), a comment marks the difference.

Usage: python3 run_local_pipeline.py
Produces: attribution.db (SQLite) + prints validation output to stdout
"""
import sqlite3
import csv

LOOKBACK_DAYS = 30  # ASSUMPTION: see docs/assumptions.md

conn = sqlite3.connect("attribution.db")
conn.executescript("DROP TABLE IF EXISTS raw_events;")
conn.execute("""
CREATE TABLE raw_events (
    event_date TEXT,
    event_timestamp INTEGER,
    event_name TEXT,
    user_pseudo_id TEXT,
    ga_session_id INTEGER,
    source TEXT,
    medium TEXT,
    campaign TEXT,
    purchase_revenue TEXT
)
""")

with open("events.csv") as f:
    reader = csv.DictReader(f)
    rows = [(r["event_date"], int(r["event_timestamp"]), r["event_name"],
              r["user_pseudo_id"], int(r["ga_session_id"]), r["source"],
              r["medium"], r["campaign"], r["purchase_revenue"])
             for r in reader]
conn.executemany("INSERT INTO raw_events VALUES (?,?,?,?,?,?,?,?,?)", rows)
conn.commit()
print(f"Loaded {len(rows)} raw events into raw_events")

# ---------------------------------------------------------------
# stg_events  (mirrors dbt_project/models/staging/stg_events.sql)
# ---------------------------------------------------------------
conn.executescript("""
DROP TABLE IF EXISTS stg_events;
CREATE TABLE stg_events AS
SELECT
    event_date,
    event_timestamp,                                  -- microseconds
    event_timestamp / 1000000.0 AS event_ts_seconds,   -- BQ: TIMESTAMP_MICROS(event_timestamp)
    event_name,
    user_pseudo_id,
    ga_session_id,
    NULLIF(source, '')   AS source,
    NULLIF(medium, '')   AS medium,
    NULLIF(campaign, '') AS campaign,
    CASE WHEN purchase_revenue = '' THEN NULL ELSE CAST(purchase_revenue AS REAL) END AS purchase_revenue
FROM raw_events;
""")

# ---------------------------------------------------------------
# int_touchpoints (mirrors models/intermediate/int_touchpoints.sql)
# One row per session that carries channel info (session_start events).
# Channel label = source / medium, falling back to '(direct)/(none)'.
# ---------------------------------------------------------------
conn.executescript("""
DROP TABLE IF EXISTS int_touchpoints;
CREATE TABLE int_touchpoints AS
SELECT
    user_pseudo_id,
    ga_session_id,
    event_ts_seconds AS touchpoint_ts,
    COALESCE(source, '(direct)') AS source,
    COALESCE(medium, '(none)')   AS medium,
    campaign,
    COALESCE(source,'(direct)') || ' / ' || COALESCE(medium,'(none)') AS channel
FROM stg_events
WHERE event_name = 'session_start';
""")

# ---------------------------------------------------------------
# int_conversions (mirrors models/intermediate/int_conversions.sql)
# ---------------------------------------------------------------
conn.executescript("""
DROP TABLE IF EXISTS int_conversions;
CREATE TABLE int_conversions AS
SELECT
    user_pseudo_id || '-' || CAST(event_timestamp AS TEXT) AS conversion_id,
    user_pseudo_id,
    event_date AS conversion_date,
    event_ts_seconds AS conversion_ts,
    purchase_revenue
FROM stg_events
WHERE event_name = 'purchase';
""")

# ---------------------------------------------------------------
# mart_attribution_first_click
# ---------------------------------------------------------------
conn.executescript(f"""
DROP TABLE IF EXISTS mart_attribution_first_click;
CREATE TABLE mart_attribution_first_click AS
WITH eligible AS (
    SELECT
        c.conversion_id, c.conversion_date, c.conversion_ts, c.purchase_revenue,
        t.channel, t.touchpoint_ts, t.ga_session_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.conversion_id
            ORDER BY t.touchpoint_ts ASC, t.ga_session_id ASC   -- tie-breaker: lowest session id
        ) AS rn
    FROM int_conversions c
    JOIN int_touchpoints t
      ON t.user_pseudo_id = c.user_pseudo_id
     AND t.touchpoint_ts <= c.conversion_ts
     AND t.touchpoint_ts >= c.conversion_ts - ({LOOKBACK_DAYS} * 86400)
)
SELECT conversion_id, conversion_date, purchase_revenue, channel AS attributed_channel, 'first_click' AS model
FROM eligible WHERE rn = 1;
""")

# ---------------------------------------------------------------
# mart_attribution_last_click
# ---------------------------------------------------------------
conn.executescript(f"""
DROP TABLE IF EXISTS mart_attribution_last_click;
CREATE TABLE mart_attribution_last_click AS
WITH eligible AS (
    SELECT
        c.conversion_id, c.conversion_date, c.conversion_ts, c.purchase_revenue,
        t.channel, t.touchpoint_ts, t.ga_session_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.conversion_id
            ORDER BY t.touchpoint_ts DESC, t.ga_session_id DESC  -- tie-breaker: highest session id
        ) AS rn
    FROM int_conversions c
    JOIN int_touchpoints t
      ON t.user_pseudo_id = c.user_pseudo_id
     AND t.touchpoint_ts <= c.conversion_ts
     AND t.touchpoint_ts >= c.conversion_ts - ({LOOKBACK_DAYS} * 86400)
)
SELECT conversion_id, conversion_date, purchase_revenue, channel AS attributed_channel, 'last_click' AS model
FROM eligible WHERE rn = 1;
""")

# ---------------------------------------------------------------
# mart_channel_daily  (feeds the dashboard: totals, time series, breakdown)
# ---------------------------------------------------------------
conn.executescript("""
DROP TABLE IF EXISTS mart_channel_daily;
CREATE TABLE mart_channel_daily AS
SELECT model, conversion_date, attributed_channel,
       COUNT(*) AS conversions, ROUND(SUM(purchase_revenue),2) AS revenue
FROM (
    SELECT * FROM mart_attribution_first_click
    UNION ALL
    SELECT * FROM mart_attribution_last_click
)
GROUP BY model, conversion_date, attributed_channel
ORDER BY conversion_date, model, attributed_channel;
""")
conn.commit()

# ---------------------------------------------------------------
# Validation output
# ---------------------------------------------------------------
print("\n=== Totals by model ===")
for row in conn.execute("SELECT model, COUNT(*), ROUND(SUM(purchase_revenue),2) FROM (SELECT * FROM mart_attribution_first_click UNION ALL SELECT * FROM mart_attribution_last_click) GROUP BY model"):
    print(row)

print("\n=== Channel breakdown (last_click) ===")
for row in conn.execute("SELECT attributed_channel, COUNT(*), ROUND(SUM(purchase_revenue),2) FROM mart_attribution_last_click GROUP BY attributed_channel ORDER BY 3 DESC"):
    print(row)

print("\n=== Sample: same conversion, first vs last differ ===")
for row in conn.execute("""
    SELECT f.conversion_id, f.attributed_channel AS first_channel, l.attributed_channel AS last_channel
    FROM mart_attribution_first_click f
    JOIN mart_attribution_last_click l USING (conversion_id)
    WHERE f.attributed_channel != l.attributed_channel
    LIMIT 5
"""):
    print(row)

conn.close()
print("\nDone. Data in attribution.db")
