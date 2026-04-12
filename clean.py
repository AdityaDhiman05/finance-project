import pandas as pd


def clean_data(df, symbol="AAPL"):
    """Clean and standardise raw data from Twelve Data API."""
    print(f"  Cleaning {symbol}...")

    # Remove duplicate timestamps
    df = df[~df.index.duplicated(keep='first')]

    # Remove rows with missing values
    df = df.dropna()

    # Convert all columns to proper numeric types
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows that failed conversion
    df = df.dropna()

    # Round prices to 2 decimal places
    for col in ['open', 'high', 'low', 'close']:
        df[col] = df[col].round(2)

    # Sort newest first
    df = df.sort_index(ascending=False)

    # Add symbol column
    df['symbol'] = symbol

    print(f"  Clean done — {len(df)} records for {symbol}")
    return df


def clean_batch(batch_data):
    """Clean an entire batch of fetched data."""
    cleaned = {}
    for symbol, df in batch_data.items():
        try:
            cleaned[symbol] = clean_data(df, symbol)
        except Exception as e:
            print(f"  ERROR cleaning {symbol}: {e}")
    return cleaned


if __name__ == "__main__":
    from fetch import fetch_stock_data
    df = fetch_stock_data("AAPL")
    cleaned = clean_data(df, "AAPL")
    print(cleaned.head())