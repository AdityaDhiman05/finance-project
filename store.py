from sqlalchemy import create_engine, text
import pymysql

DB_USER     = "root"
DB_PASSWORD = "MySQL@@2067#1403"
DB_HOST     = "localhost"
DB_NAME     = "finance_db"

def get_engine():
    engine = create_engine(
        "mysql+pymysql://",
        creator=lambda: pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    )
    return engine

def create_table_if_not_exists(engine):
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            datetime  DATETIME NOT NULL,
            symbol    VARCHAR(10) NOT NULL,
            open      DECIMAL(10,2),
            high      DECIMAL(10,2),
            low       DECIMAL(10,2),
            close     DECIMAL(10,2),
            volume    BIGINT,
            PRIMARY KEY (datetime, symbol)
        )
    """)
    with engine.connect() as conn:
        conn.execute(create_table_sql)
        conn.commit()
    print("Table ready.")

def store_data(df):
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
                insert_sql = text("""
                    INSERT IGNORE INTO stock_prices
                    (datetime, symbol, open, high, low, close, volume)
                    VALUES (:datetime, :symbol, :open, :high, :low, :close, :volume)
                """)
                result = conn.execute(insert_sql, {
                    "datetime": row['datetime'],
                    "symbol"  : row['symbol'],
                    "open"    : float(row['open']),
                    "high"    : float(row['high']),
                    "low"     : float(row['low']),
                    "close"   : float(row['close']),
                    "volume"  : int(row['volume'])
                })
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"Row error: {e}")
        conn.commit()

    print(f"Inserted : {inserted} new rows")
    print(f"Skipped  : {skipped} duplicate rows")
    print(f"Table    : stock_prices")
    print(f"Database : {DB_NAME}")
    print("Data stored successfully!")

if __name__ == "__main__":
    from fetch import fetch_stock_data
    from clean import clean_data
    raw     = fetch_stock_data()
    cleaned = clean_data(raw)
    store_data(cleaned)