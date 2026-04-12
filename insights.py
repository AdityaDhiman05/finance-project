import pymysql
import pandas as pd
import numpy as np
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, ALL_SYMBOLS

THRESHOLDS = {
    "AAPL"    : 260.0,
    "GOOGL"   : 180.0,
    "MSFT"    : 420.0,
    "AMZN"    : 200.0,
    "TSLA"    : 300.0,
    "NVDA"    : 130.0,
    "META"    : 580.0,
    "NFLX"    : 950.0,
    "BTC/USD" : 90000.0,
    "ETH/USD" : 2000.0,
    "BNB/USD" : 600.0,
    "XRP/USD" : 2.5,
    "EUR/USD" : 1.10,
    "GBP/USD" : 1.30,
    "JPY/USD" : 0.0070,
    "AUD/USD" : 0.65,
}


def get_connection():
    return pymysql.connect(
        host     = DB_HOST,
        user     = DB_USER,
        password = DB_PASSWORD,
        database = DB_NAME
    )


def get_data_from_db(symbol, limit=50):
    """Read latest N records for a symbol from MySQL."""
    conn  = get_connection()
    query = f"""
        SELECT datetime, open, high, low, close, volume
        FROM stock_prices
        WHERE symbol = '{symbol}'
        ORDER BY datetime DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def calculate_moving_averages(df):
    """Calculate MA5 and MA20."""
    close = df['close'].values
    ma5   = round(float(np.mean(close[:5])),  2) if len(close) >= 5  else None
    ma20  = round(float(np.mean(close[:20])), 2) if len(close) >= 20 else None
    return ma5, ma20


def detect_trend(ma5, ma20):
    """Detect trend from moving averages."""
    if ma5 is None or ma20 is None:
        return "INSUFFICIENT DATA"
    if ma5 > ma20:
        return "UPTREND"
    elif ma5 < ma20:
        return "DOWNTREND"
    return "SIDEWAYS"


def check_alert(symbol, current_price):
    """Generate price alert if price crosses threshold."""
    threshold = THRESHOLDS.get(symbol, 0)
    if threshold and current_price > threshold:
        return f"Price ${current_price} crossed threshold ${threshold}"
    return None


def get_insights(symbol="AAPL"):
    """Get full insights for a single symbol."""
    try:
        df            = get_data_from_db(symbol)
        current_price = round(float(df['close'].iloc[0]), 2)
        ma5, ma20     = calculate_moving_averages(df)
        trend         = detect_trend(ma5, ma20)
        alert         = check_alert(symbol, current_price)

        return {
            "symbol"        : symbol,
            "current_price" : current_price,
            "ma5"           : ma5,
            "ma20"          : ma20,
            "trend"         : trend,
            "alert"         : alert,
            "records"       : len(df)
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


def get_all_insights():
    """Get insights for all 16 tracked symbols."""
    return {symbol: get_insights(symbol) for symbol in ALL_SYMBOLS}


if __name__ == "__main__":
    print("\n=== INSIGHTS FOR ALL STOCKS ===\n")
    for symbol, data in get_all_insights().items():
        if "error" not in data:
            print(f"{symbol:10} | ${data['current_price']:>10} | MA5: {data['ma5']} | MA20: {data['ma20']} | {data['trend']}")
        else:
            print(f"{symbol:10} | ERROR: {data['error']}")