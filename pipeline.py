import importlib
import time
from datetime import datetime

sched = importlib.import_module('schedule')

from fetch import fetch_stock_data
from clean import clean_data
from store import store_data

SYMBOL   = "AAPL"
INTERVAL = 5

def run_pipeline():
    print("\n" + "=" * 45)
    print(f"  PIPELINE STARTED — {datetime.now()}")
    print("=" * 45)

    try:
        print("\n[Step 1] Fetching data...")
        raw = fetch_stock_data()
        print("         Fetch complete.")

        print("\n[Step 2] Cleaning data...")
        cleaned = clean_data(raw)
        print("         Clean complete.")

        print("\n[Step 3] Storing data...")
        store_data(cleaned)
        print("         Store complete.")

        print(f"\n  Pipeline finished successfully!")
        print(f"  Next run in {INTERVAL} minutes.")
        print("=" * 45)

    except Exception as e:
        print(f"\n  ERROR in pipeline: {e}")
        print("  Will retry next cycle.")
        print("=" * 45)

print("=" * 45)
print("  FINANCE SCHEDULER STARTED")
print(f"  Running every {INTERVAL} minutes")
print(f"  Stock: {SYMBOL}")
print(f"  Started at: {datetime.now()}")
print("=" * 45)

run_pipeline()

sched.every(INTERVAL).minutes.do(run_pipeline)

while True:
    sched.run_pending()
    time.sleep(30)