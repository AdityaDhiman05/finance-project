import importlib
import time
from datetime import datetime

sched = importlib.import_module('schedule')

from fetch import fetch_current_batch, fetch_all_stocks
from clean import clean_batch
from store import store_batch
from config import STOCK_BATCHES, ALL_SYMBOLS, FETCH_INTERVAL

batch_index = 0


def run_pipeline():
    global batch_index
    current_batch = STOCK_BATCHES[batch_index % len(STOCK_BATCHES)]

    print("\n" + "=" * 55)
    print(f"  PIPELINE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Batch {batch_index % len(STOCK_BATCHES) + 1}/{len(STOCK_BATCHES)}: {current_batch}")
    print("=" * 55)

    try:
        print("\n[Step 1] Fetching...")
        raw_data = fetch_current_batch(batch_index)

        print("\n[Step 2] Cleaning...")
        cleaned_data = clean_batch(raw_data)

        print("\n[Step 3] Storing...")
        store_batch(cleaned_data)

        print(f"\n  Done! Next batch in {FETCH_INTERVAL} minutes.")
        print("=" * 55)

    except Exception as e:
        print(f"\n  ERROR: {e}")
        print("=" * 55)

    batch_index += 1


def initial_load():
    print("\n" + "=" * 55)
    print("  INITIAL LOAD — Fetching all 16 assets...")
    print("=" * 55)
    try:
        raw_data     = fetch_all_stocks()
        cleaned_data = clean_batch(raw_data)
        store_batch(cleaned_data)
        print(f"\n  Initial load complete — {len(cleaned_data)} symbols loaded!")
    except Exception as e:
        print(f"  ERROR: {e}")


print("\n" + "=" * 55)
print("  FINANCE PIPELINE v2.0")
print(f"  Assets  : {len(ALL_SYMBOLS)} across 4 classes")
print(f"  Batches : {len(STOCK_BATCHES)} batches of 4 symbols")
print(f"  Interval: every {FETCH_INTERVAL} minutes")
print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 55)

initial_load()

sched.every(FETCH_INTERVAL).minutes.do(run_pipeline)

print(f"\n  Scheduler live — next batch in {FETCH_INTERVAL} minutes...")
print("  Press Ctrl+C to stop.\n")

while True:
    sched.run_pending()
    time.sleep(30)