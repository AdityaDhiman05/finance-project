import twelvedata as td
from datetime import datetime

API_KEY = "e56fbd6bacee40aabdeec516f94fd6c4"
SYMBOL = "AAPL"
INTERVAL = "5min"

def fetch_stock_data():
    print(f"[{datetime.now()}] Fetching data for {SYMBOL}...")

    client = td.TDClient(apikey=API_KEY)

    ts = client.time_series(
        symbol=SYMBOL,
        interval=INTERVAL,
        outputsize=100,
        timezone="America/New_York"
    )

    df = ts.as_pandas()

    print(f"Successfully fetched {len(df)} records")
    print(f"Latest timestamp  : {df.index[0]}")
    print(f"Latest close price: {df['close'].iloc[0]}")
    print(f"Latest volume     : {df['volume'].iloc[0]}")
    print("\nFirst 5 rows of data:")
    print(df.head())

    return df

if __name__ == "__main__":
    data = fetch_stock_data()