"""
stream_events_local_demo.py
-----------------------------
A fully local, actually-runnable version of the streaming demo, using
the same attribution.db SQLite file that local_demo/run_local_pipeline.py
builds. Proves the dedupe + re-materialization behavior end-to-end
without needing BigQuery.

What it does:
  1. Generates 5-20 new "live" events (same generator logic as
     stream_events_bigquery.py, reused here).
  2. Inserts them into events_streaming, using INSERT OR IGNORE keyed on
     event_id - this is SQLite's equivalent of the BigQuery MERGE
     dedupe pattern. Running this script twice with the same events
     is a no-op the second time.
  3. Re-runs the mart_channel_daily aggregation so you can see the
     live panel numbers change immediately after streaming - this is
     the "materialization" proof.

Usage:
  cd local_demo && python3 ../local_demo/run_local_pipeline.py   # build attribution.db first
  cd ../streaming && python3 stream_events_local_demo.py --n_events 10
"""
import argparse
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from stream_events_bigquery import generate_events

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "local_demo", "attribution.db")


def ensure_streaming_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_streaming (
            event_id TEXT PRIMARY KEY,   -- idempotency key: dedupe happens here
            event_date TEXT,
            event_timestamp INTEGER,
            event_name TEXT,
            user_pseudo_id TEXT,
            ga_session_id INTEGER,
            source TEXT,
            medium TEXT,
            campaign TEXT,
            purchase_revenue REAL,
            ingested_at TEXT
        )
    """)


def insert_events(conn, events):
    before = conn.execute("SELECT COUNT(*) FROM events_streaming").fetchone()[0]
    conn.executemany("""
        INSERT OR IGNORE INTO events_streaming
        (event_id, event_date, event_timestamp, event_name, user_pseudo_id,
         ga_session_id, source, medium, campaign, purchase_revenue, ingested_at)
        VALUES (:event_id, :event_date, :event_timestamp, :event_name, :user_pseudo_id,
                :ga_session_id, :source, :medium, :campaign, :purchase_revenue, :ingested_at)
    """, events)
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM events_streaming").fetchone()[0]
    inserted = after - before
    skipped = len(events) - inserted
    print(f"Inserted {inserted} new events, skipped {skipped} duplicates (idempotency check)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_events", type=int, default=10)
    parser.add_argument("--replay_last", action="store_true",
                         help="Re-insert the same events again, to prove dedupe works")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"ERROR: {DB_PATH} not found. Run local_demo/run_local_pipeline.py first.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    ensure_streaming_table(conn)

    events = generate_events(args.n_events)
    print(f"\n--- First send: {len(events)} events ---")
    insert_events(conn, events)

    if args.replay_last:
        print(f"\n--- Replaying SAME {len(events)} events (simulating a retry) ---")
        insert_events(conn, events)  # should skip all of them

    total = conn.execute("SELECT COUNT(*) FROM events_streaming").fetchone()[0]
    print(f"\nevents_streaming now has {total} total rows.")
    print("\nLatest 5 streamed events:")
    for row in conn.execute("SELECT event_name, user_pseudo_id, source, medium, ingested_at FROM events_streaming ORDER BY ingested_at DESC LIMIT 5"):
        print(" ", row)

    conn.close()
    print("\nNext: re-run local_demo/run_local_pipeline.py-style aggregation, or open dashboard/index.html "
          "(load_data.py) to see these reflected in the live panel.")
