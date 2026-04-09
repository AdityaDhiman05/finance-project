import pandas as pd
import numpy as np

def clean_data(df):
    print("Starting data cleaning...")
    print(f"Records before cleaning: {len(df)}")

    # Step 1 - remove duplicate rows
    df = df[~df.index.duplicated(keep='first')]

    # Step 2 - remove rows with missing values
    df = df.dropna()

    # Step 3 - convert all columns to proper numbers
    df['open']   = pd.to_numeric(df['open'],   errors='coerce')
    df['high']   = pd.to_numeric(df['high'],   errors='coerce')
    df['low']    = pd.to_numeric(df['low'],    errors='coerce')
    df['close']  = pd.to_numeric(df['close'],  errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    # Step 4 - remove any rows that became NaN after conversion
    df = df.dropna()

    # Step 5 - round prices to 2 decimal places
    df['open']  = df['open'].round(2)
    df['high']  = df['high'].round(2)
    df['low']   = df['low'].round(2)
    df['close'] = df['close'].round(2)

    # Step 6 - make sure data is sorted newest first
    df = df.sort_index(ascending=False)

    # Step 7 - add a symbol column
    df['symbol'] = 'AAPL'

    print(f"Records after cleaning : {len(df)}")
    print(f"Columns                : {list(df.columns)}")
    print("\nCleaned data sample:")
    print(df.head())

    return df

if __name__ == "__main__":
    from fetch import fetch_stock_data
    raw_data = fetch_stock_data()
    cleaned_data = clean_data(raw_data)