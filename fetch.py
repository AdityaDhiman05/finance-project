import twelvedata as td
import time
from datetime import datetime
from config import API_KEY, STOCK_BATCHES

INTERVAL = "5min"


def fetch_stock_data(symbol):
    """Fetch 5-minute intraday data for a single symbol."""
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] Fetching {symbol}...")

    client = td.TDClient(apikey=API_KEY)

    ts = client.time_series(
        symbol     = symbol,
        interval   = INTERVAL,
        outputsize = 50,
        timezone   = "America/New_York"
    )

    df = ts.as_pandas()
    print(f"  Got {len(df)} records — latest close: {df['close'].iloc[0]}")
    return df


def fetch_batch(batch):
    """Fetch all symbols in a batch with 8 second delay between each."""
    results = {}
    for symbol in batch:
        try:
            df = fetch_stock_data(symbol)
            results[symbol] = df
            time.sleep(8)
        except Exception as e:
            print(f"  ERROR fetching {symbol}: {e}")
    return results


def fetch_current_batch(batch_index):
    """Fetch one batch by index."""
    batch = STOCK_BATCHES[batch_index % len(STOCK_BATCHES)]
    print(f"\n  Fetching Batch {batch_index % len(STOCK_BATCHES) + 1}: {batch}")
    return fetch_batch(batch)


def fetch_all_stocks():
    """Fetch ALL 16 symbols — used for initial data load on startup."""
    all_data = {}
    for i, batch in enumerate(STOCK_BATCHES):
        print(f"\n  Batch {i+1}/{len(STOCK_BATCHES)}: {batch}")
        results = fetch_batch(batch)
        all_data.update(results)
        if i < len(STOCK_BATCHES) - 1:
            print(f"  Waiting 15s before next batch...")
            time.sleep(15)
    return all_data


if __name__ == "__main__":
    print("Testing fetch for AAPL only...")
    df = fetch_stock_data("AAPL")
    print(df.head())