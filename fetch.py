import requests
import pandas as pd
import time
from datetime import datetime
from config import API_KEY, STOCK_BATCHES

BASE_URL = "https://api.twelvedata.com/time_series"


# 🔷 FETCH SINGLE SYMBOL (FULLY SAFE)
def fetch_stock_data(symbol):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching {symbol}...")

    params = {
        "symbol": symbol,
        "interval": "5min",
        "outputsize": 50,
        "apikey": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        # ❌ API error
        if "values" not in data:
            print(f"ERROR API for {symbol}: {data}")
            return None

        df = pd.DataFrame(data["values"])

        # Convert datetime
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)

        # Convert numeric columns safely
        numeric_cols = ["open", "high", "low", "close"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 🔥 FIX: volume may not exist
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        else:
            df["volume"] = 0  # default value

        df = df.dropna()

        print(f"Fetched {len(df)} rows for {symbol}")
        return df

    except Exception as e:
        print(f"ERROR fetching {symbol}: {e}")
        return None


# 🔷 FETCH BATCH
def fetch_batch(batch):
    results = {}

    for symbol in batch:
        df = fetch_stock_data(symbol)

        if df is not None and not df.empty:
            results[symbol] = df
        else:
            print(f"Skipping {symbol} (no valid data)")

        time.sleep(8)

    return results


# 🔷 FETCH CURRENT BATCH
def fetch_current_batch(batch_index):
    batch = STOCK_BATCHES[batch_index % len(STOCK_BATCHES)]
    print(f"\nFetching batch: {batch}")
    return fetch_batch(batch)


# 🔷 FETCH ALL
def fetch_all_stocks():
    all_data = {}

    for i, batch in enumerate(STOCK_BATCHES):
        print(f"\nBatch {i+1}: {batch}")
        results = fetch_batch(batch)
        all_data.update(results)

        time.sleep(10)

    return all_data


# 🔷 TEST
if __name__ == "__main__":
    df = fetch_stock_data("BTC/USD")

    if df is not None:
        print(df.head())
    else:
        print("No data fetched")