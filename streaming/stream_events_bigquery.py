"""
stream_events_bigquery.py
--------------------------
Streams 5-20 sample GA4-shaped events into BigQuery to demonstrate
near-real-time materialization on top of the dbt models.

WHY THIS DESIGN:
GA4's own BigQuery export has two paths:
  1. Daily batch export (events_YYYYMMDD) - lands ~once/day, hours of lag
  2. Streaming export (events_intraday_YYYYMMDD) - lands within minutes,
     but the table is continuously overwritten during the day and only
     finalized into the batch table afterward.

We can't attach to GA4's real streaming export without a live GA4
property, so this script DEMONSTRATES the same pattern GA4 uses:
  - append-only inserts into a small `events_streaming` table
  - an idempotency key (event_id) so re-running the script or retries
    never create duplicate rows
  - stg_events (dbt) unions this table with the batch export, so
    `dbt run --select stg_events+` picks up new rows on the next run
    without any schema changes

IDEMPOTENCY / DEDUPE STRATEGY:
Each event gets a deterministic event_id = hash(user_pseudo_id,
event_timestamp, event_name). We use a MERGE statement (not a plain
INSERT) keyed on event_id, so:
  - Re-sending the same batch (e.g. after a network retry) is a no-op
  - BigQuery's streaming buffer can still return duplicates for ~90
    minutes after insert; MERGE at query time is what actually
    guarantees dedupe, not the insert step itself
  - This mirrors how you'd handle at-least-once delivery from a real
    event source (Pub/Sub, Kafka, GA4's own collector)

EXPECTED LATENCY:
  - BigQuery streaming insert (tabledata.insertAll / Storage Write API):
    rows queryable within ~seconds, but land in the STREAMING BUFFER
    (not yet in permanent storage) for up to ~90 minutes. Streaming
    buffer rows CAN still be queried, just not UPDATE/DELETE'd, which
    is why we MERGE downstream instead of mutating events_streaming.
  - dbt run on stg_events+ after that: a few seconds for these small
    marts (views + a handful of small tables).
  - End-to-end "event happens -> shows in dashboard": realistically
    30-90 seconds if the dashboard auto-refreshes on a short interval,
    dominated by BigQuery streaming buffer visibility + your refresh
    cadence, not by dbt compute.

USAGE:
  pip install google-cloud-bigquery
  python3 stream_events_bigquery.py --project YOUR_PROJECT --n_events 12
"""
import argparse
import hashlib
import random
import time
import datetime

def make_event_id(user_pseudo_id, event_timestamp, event_name):
    raw = f"{user_pseudo_id}|{event_timestamp}|{event_name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

CHANNELS = [
    ("google", "organic", None),
    ("google", "cpc", "summer_sale"),
    ("(direct)", "(none)", None),
    ("facebook", "social", "brand_awareness"),
]
EVENTS = ["session_start", "page_view", "view_item", "add_to_cart", "purchase"]

def generate_events(n):
    events = []
    for i in range(n):
        now = datetime.datetime.now(datetime.timezone.utc)
        event_ts = int(now.timestamp() * 1_000_000) + i  # stagger by microsecond to keep order
        source, medium, campaign = random.choice(CHANNELS)
        event_name = random.choices(EVENTS, weights=[30, 30, 15, 15, 10])[0]
        user_pseudo_id = f"live_user_{random.randint(1,6):02d}.{random.randint(1000,9999)}"
        row = {
            "event_id": None,
            "event_date": now.strftime("%Y%m%d"),
            "event_timestamp": event_ts,
            "event_name": event_name,
            "user_pseudo_id": user_pseudo_id,
            "ga_session_id": random.randint(90000, 99999),
            "source": source,
            "medium": medium,
            "campaign": campaign,
            "purchase_revenue": round(random.uniform(15, 220), 2) if event_name == "purchase" else None,
            "ingested_at": now.isoformat(),
        }
        row["event_id"] = make_event_id(user_pseudo_id, event_ts, event_name)
        events.append(row)
    return events


def stream_to_bigquery(project, dataset, table, events):
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"

    # Ensure a staging (temp) table exists to MERGE from, so retries are safe.
    staging_table = f"{project}.{dataset}.{table}_incoming"
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{staging_table}` LIKE `{table_ref}`
    """).result()

    errors = client.insert_rows_json(staging_table, events)  # streaming insert
    if errors:
        raise RuntimeError(f"Streaming insert errors: {errors}")

    # MERGE from staging into the real streaming table, keyed on event_id.
    # This is what actually guarantees no duplicates end up in stg_events,
    # even if insert_rows_json is retried by the client library.
    merge_sql = f"""
        MERGE `{table_ref}` T
        USING `{staging_table}` S
        ON T.event_id = S.event_id
        WHEN NOT MATCHED THEN
          INSERT ROW
    """
    client.query(merge_sql).result()
    print(f"Merged {len(events)} events into {table_ref} (dedup on event_id)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=False, default=None, help="GCP project id")
    parser.add_argument("--dataset", default="ga4_attribution_dev")
    parser.add_argument("--table", default="events_streaming")
    parser.add_argument("--n_events", type=int, default=12, help="5-20 recommended")
    parser.add_argument("--dry_run", action="store_true", help="Print events instead of hitting BigQuery")
    args = parser.parse_args()

    events = generate_events(args.n_events)

    if args.dry_run or not args.project:
        print(f"[DRY RUN] Generated {len(events)} events (no --project given, or --dry_run set):\n")
        for e in events:
            print(e)
        print("\nTo actually stream these into BigQuery:")
        print("  python3 stream_events_bigquery.py --project YOUR_PROJECT --n_events 12")
    else:
        stream_to_bigquery(args.project, args.dataset, args.table, events)
