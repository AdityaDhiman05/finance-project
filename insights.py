import pymysql
import pandas as pd
import numpy as np

DB_USER     = "root"
DB_PASSWORD = "MySQL@@2067#1403"
DB_HOST     = "localhost"
DB_NAME     = "finance_db"

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def get_data_from_db(symbol="AAPL", limit=50):
    conn = get_connection()
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
    close = df['close'].values
    ma5  = round(float(np.mean(close[:5])),  2)
    ma20 = round(float(np.mean(close[:20])), 2)
    return ma5, ma20

def detect_trend(ma5, ma20):
    if ma5 > ma20:
        return "UPTREND"
    elif ma5 < ma20:
        return "DOWNTREND"
    else:
        return "SIDEWAYS"

def check_alert(current_price, threshold=260.0):
    if current_price > threshold:
        return f"ALERT: Price ${current_price} crossed threshold ${threshold}"
    return None

def get_insights(symbol="AAPL"):
    df = get_data_from_db(symbol)

    current_price = round(float(df['close'].iloc[0]), 2)
    ma5, ma20     = calculate_moving_averages(df)
    trend         = detect_trend(ma5, ma20)
    alert         = check_alert(current_price)

    insights = {
        "symbol"        : symbol,
        "current_price" : current_price,
        "ma5"           : ma5,
        "ma20"          : ma20,
        "trend"         : trend,
        "alert"         : alert
    }

    print("=" * 40)
    print(f"  INSIGHTS FOR {symbol}")
    print("=" * 40)
    print(f"  Current Price : ${current_price}")
    print(f"  MA5  (5-bar)  : ${ma5}")
    print(f"  MA20 (20-bar) : ${ma20}")
    print(f"  Trend         : {trend}")
    print(f"  Alert         : {alert if alert else 'None'}")
    print("=" * 40)

    return insights

if __name__ == "__main__":
    get_insights("AAPL")