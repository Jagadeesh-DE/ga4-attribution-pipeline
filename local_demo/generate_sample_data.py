"""
generate_sample_data.py
------------------------
Generates synthetic event data shaped like the real GA4 BigQuery export
schema (bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*),
flattened into a single CSV for local use.

Why synthetic data:
We don't have BigQuery billing access in this environment, so we can't
query the real public dataset. This script produces data with the SAME
columns/semantics as the real GA4 export so that every model in
dbt_project/ can be pointed at the real table later by changing only
the `source` config — no logic changes needed.

Real GA4 export columns we mimic (subset relevant to attribution):
  event_date          STRING   'YYYYMMDD'
  event_timestamp     INTEGER  microseconds since epoch
  event_name          STRING   e.g. 'session_start','page_view','purchase'
  user_pseudo_id       STRING   device-scoped anonymous id
  ga_session_id        INTEGER  (from event_params, flattened here)
  source                STRING  (from event_params 'source' or traffic_source)
  medium                 STRING
  campaign               STRING
  purchase_revenue     FLOAT   (only present on 'purchase' events)

Reproduction: `python3 generate_sample_data.py` -> writes events.csv
"""
import csv
import random
import datetime

random.seed(42)  # deterministic output so results are reproducible

CHANNELS = [
    ("google", "organic", None),
    ("google", "cpc", "summer_sale"),
    ("(direct)", "(none)", None),
    ("facebook", "social", "brand_awareness"),
    ("newsletter", "email", "weekly_digest"),
    ("partner-blog.com", "referral", None),
]

N_USERS = 40
DAYS = 20  # generate 20 days of history; dashboard shows the most recent 14
START_DATE = datetime.date(2026, 7, 20)

EVENT_FUNNEL = ["session_start", "page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"]

def ts_micros(dt):
    return int(dt.timestamp() * 1_000_000)

rows = []
session_counter = 1000

for user_idx in range(N_USERS):
    user_pseudo_id = f"user_{user_idx:04d}.{random.randint(1000,9999)}"
    # each user gets 1-4 touchpoints (sessions) across the window, on random days/channels
    n_touchpoints = random.choice([1, 1, 2, 2, 3, 4])
    touch_days = sorted(random.sample(range(DAYS), k=min(n_touchpoints, DAYS)))
    will_convert = random.random() < 0.55  # 55% of users eventually purchase

    for i, day_offset in enumerate(touch_days):
        session_counter += 1
        channel = random.choice(CHANNELS)
        source, medium, campaign = channel
        day = START_DATE + datetime.timedelta(days=day_offset)
        hour = random.randint(7, 22)
        minute = random.randint(0, 59)
        session_start_dt = datetime.datetime.combine(day, datetime.time(hour, minute))

        is_last_touch = (i == len(touch_days) - 1)
        # decide which events happen in this session
        if is_last_touch and will_convert:
            funnel = EVENT_FUNNEL
        else:
            # non-converting session: random depth into the funnel, no purchase
            depth = random.randint(1, 4)
            funnel = EVENT_FUNNEL[:depth]

        t = session_start_dt
        for step, event_name in enumerate(funnel):
            t = t + datetime.timedelta(seconds=random.randint(5, 90))
            revenue = round(random.uniform(15, 220), 2) if event_name == "purchase" else ""
            rows.append({
                "event_date": day.strftime("%Y%m%d"),
                "event_timestamp": ts_micros(t),
                "event_name": event_name,
                "user_pseudo_id": user_pseudo_id,
                "ga_session_id": session_counter,
                "source": source,
                "medium": medium,
                "campaign": campaign or "",
                "purchase_revenue": revenue,
            })

# sort by timestamp to mimic natural ingestion order
rows.sort(key=lambda r: r["event_timestamp"])

with open("events.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} events for {N_USERS} users across {DAYS} days to events.csv")
