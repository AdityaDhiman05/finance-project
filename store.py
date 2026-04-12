from sqlalchemy import create_engine, text
import pymysql
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME


def get_engine():
    """Create SQLAlchemy engine using pymysql."""
    engine = create_engine(
        "mysql+pymysql://",
        creator=lambda: pymysql.connect(
            host     = DB_HOST,
            user     = DB_USER,
            password = DB_PASSWORD,
            database = DB_NAME
        )
    )
    return engine


def create_table_if_not_exists(engine):
    """Create stock_prices table if it does not exist."""
    sql = text("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            datetime  DATETIME     NOT NULL,
            symbol    VARCHAR(10)  NOT NULL,
            open      DECIMAL(10,2),
            high      DECIMAL(10,2),
            low       DECIMAL(10,2),
            close     DECIMAL(10,2),
            volume    BIGINT,
            PRIMARY KEY (datetime, symbol)
        )
    """)
    with engine.connect() as conn:
        conn.execute(sql)
        conn.commit()


def store_data(df):
    """Store cleaned DataFrame into MySQL — skips duplicates silently."""
    engine = get_engine()
    create_table_if_not_exists(engine)

    df_to_store = df.copy()
    df_to_store.index.name = 'datetime'
    df_to_store = df_to_store.reset_index()

    inserted = 0
    skipped  = 0

    with engine.connect() as conn:
        for _, row in df_to_store.iterrows():
            try:
                result = conn.execute(text("""
                    INSERT IGNORE INTO stock_prices
                    (datetime, symbol, open, high, low, close, volume)
                    VALUES (:datetime, :symbol, :open, :high, :low, :close, :volume)
                """), {
                    "datetime" : row['datetime'],
                    "symbol"   : row['symbol'],
                    "open"     : float(row['open']),
                    "high"     : float(row['high']),
                    "low"      : float(row['low']),
                    "close"    : float(row['close']),
                    "volume"   : int(row['volume'])
                })
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Row error: {e}")
        conn.commit()

    symbol = df_to_store['symbol'].iloc[0]
    print(f"  {symbol} — Inserted: {inserted} | Skipped: {skipped}")


def store_batch(cleaned_data):
    """Store an entire batch of cleaned DataFrames."""
    for symbol, df in cleaned_data.items():
        try:
            store_data(df)
        except Exception as e:
            print(f"  ERROR storing {symbol}: {e}")


if __name__ == "__main__":
    from fetch import fetch_stock_data
    from clean import clean_data
    df      = fetch_stock_data("AAPL")
    cleaned = clean_data(df, "AAPL")
    store_data(cleaned)