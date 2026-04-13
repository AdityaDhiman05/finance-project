import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, ALL_SYMBOLS

# ─── Alert Thresholds ────────────────────────────────────────
THRESHOLDS = {
    "AAPL"    : 260.0,  "GOOGL"   : 180.0,
    "MSFT"    : 420.0,  "AMZN"    : 200.0,
    "TSLA"    : 300.0,  "NVDA"    : 130.0,
    "META"    : 580.0,  "NFLX"    : 950.0,
    "BTC/USD" : 90000.0,"ETH/USD" : 2000.0,
    "BNB/USD" : 600.0,  "XRP/USD" : 2.5,
    "EUR/USD" : 1.10,   "GBP/USD" : 1.30,
    "USD/JPY" : 150.0,  "AUD/USD" : 0.65,
}


# ─── Database Engine ─────────────────────────────────────────
def get_engine():
    encoded_password = quote_plus(DB_PASSWORD)
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"
    )


# ─── Fetch Data from MySQL ───────────────────────────────────
def get_data_from_db(symbol, limit=50):
    engine = get_engine()
    query  = text("""
        SELECT datetime, open, high, low, close, volume
        FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY datetime DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "limit": limit})
    return df


# ─── Moving Averages ─────────────────────────────────────────
def calculate_moving_averages(df):
    """Calculate MA5 and MA20 from close prices."""
    close = df['close'].values
    ma5   = round(float(np.mean(close[:5])),  2) if len(close) >= 5  else None
    ma20  = round(float(np.mean(close[:20])), 2) if len(close) >= 20 else None
    return ma5, ma20


# ─── Trend Detection ─────────────────────────────────────────
def detect_trend(ma5, ma20):
    """Detect UPTREND, DOWNTREND or SIDEWAYS from moving averages."""
    if ma5 is None or ma20 is None:
        return "INSUFFICIENT DATA"
    if ma5 > ma20:   return "UPTREND"
    elif ma5 < ma20: return "DOWNTREND"
    else:            return "SIDEWAYS"


# ─── RSI ─────────────────────────────────────────────────────
def calculate_rsi(df, period=14):
    """
    RSI = Relative Strength Index.
    Measures speed and magnitude of recent price changes.
    Above 70 = overbought (price rose too fast, may fall).
    Below 30 = oversold (price fell too fast, may recover).
    """
    if len(df) < period + 1:
        return None, "INSUFFICIENT DATA"

    # Get close prices oldest first for correct calculation
    close  = df['close'].iloc[::-1].values
    deltas = np.diff(close)

    # Separate gains and losses
    gains  = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    # Average gain and loss over the period
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    if avg_loss == 0:
        return 100.0, "OVERBOUGHT"

    rs  = avg_gain / avg_loss
    rsi = round(100 - (100 / (1 + rs)), 2)

    # Signal based on RSI value
    if rsi >= 70:   signal = "OVERBOUGHT"
    elif rsi <= 30: signal = "OVERSOLD"
    else:           signal = "NEUTRAL"

    return rsi, signal


# ─── Bollinger Bands ─────────────────────────────────────────
def calculate_bollinger_bands(df, period=20):
    """
    Bollinger Bands = three lines around price.
    Middle band  = 20-period moving average.
    Upper band   = middle + 2 standard deviations.
    Lower band   = middle - 2 standard deviations.
    Price near upper band = overbought.
    Price near lower band = oversold.
    Wide bands = high volatility. Narrow bands = low volatility.
    """
    if len(df) < period:
        return None, None, None, "INSUFFICIENT DATA"

    close       = df['close'].values[:period]
    middle_band = round(float(np.mean(close)), 2)
    std_dev     = float(np.std(close))
    upper_band  = round(middle_band + (2 * std_dev), 2)
    lower_band  = round(middle_band - (2 * std_dev), 2)

    # Where is current price relative to bands
    current_price = float(df['close'].iloc[0])

    if current_price >= upper_band:
        bb_signal = "PRICE AT UPPER BAND — Overbought"
    elif current_price <= lower_band:
        bb_signal = "PRICE AT LOWER BAND — Oversold"
    else:
        bb_signal = "PRICE WITHIN BANDS — Normal"

    return upper_band, middle_band, lower_band, bb_signal


# ─── Volume Spike Detection ──────────────────────────────────
def detect_volume_spike(df, threshold=1.5):
    """
    Volume spike = current volume is unusually high.
    If current volume > 1.5x average volume = spike detected.
    High volume often signals something significant is happening.
    Threshold of 1.5 means 50% above average = unusual.
    """
    if len(df) < 5 or 'volume' not in df.columns:
        return False, None, None

    # Skip if all volumes are 0 (forex pairs have no volume)
    if df['volume'].sum() == 0:
        return False, 0, 0

    current_volume = int(df['volume'].iloc[0])
    avg_volume     = float(np.mean(df['volume'].values[1:21]))

    if avg_volume == 0:
        return False, current_volume, 0

    spike_ratio = round(current_volume / avg_volume, 2)
    is_spike    = spike_ratio >= threshold

    return is_spike, current_volume, round(avg_volume, 0)


# ─── Price Alert ─────────────────────────────────────────────
def check_alert(symbol, current_price):
    """Fire alert if price crosses defined threshold."""
    threshold = THRESHOLDS.get(symbol)
    if threshold and current_price > threshold:
        return f"Price ${current_price} crossed threshold ${threshold}"
    return None


# ─── Single Symbol Insights ──────────────────────────────────
def get_insights(symbol="AAPL"):
    """Get basic insights — MA, trend, alert."""
    try:
        df = get_data_from_db(symbol)

        if df.empty:
            return {"symbol": symbol, "error": "No data in database"}

        current_price       = round(float(df['close'].iloc[0]), 2)
        ma5, ma20           = calculate_moving_averages(df)
        trend               = detect_trend(ma5, ma20)
        alert               = check_alert(symbol, current_price)

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


# ─── Full Summary — All Indicators ───────────────────────────
def get_summary(symbol="AAPL"):
    """
    Full technical summary combining all indicators.
    Returns MA, trend, RSI, Bollinger Bands, volume spike and alert.
    This is the main endpoint Chahat's dashboard will call.
    """
    try:
        df = get_data_from_db(symbol, limit=50)

        if df.empty:
            return {"symbol": symbol, "error": "No data in database"}

        # Basic info
        current_price = round(float(df['close'].iloc[0]), 2)

        # Moving averages and trend
        ma5, ma20 = calculate_moving_averages(df)
        trend     = detect_trend(ma5, ma20)

        # RSI
        rsi_value, rsi_signal = calculate_rsi(df)

        # Bollinger Bands
        upper_band, middle_band, lower_band, bb_signal = calculate_bollinger_bands(df)

        # Volume spike
        is_spike, current_vol, avg_vol = detect_volume_spike(df)

        # Price alert
        alert = check_alert(symbol, current_price)

        return {
            "symbol"        : symbol,
            "current_price" : current_price,

            # Moving averages
            "ma5"           : ma5,
            "ma20"          : ma20,
            "trend"         : trend,

            # RSI
            "rsi"           : {
                "value"     : rsi_value,
                "signal"    : rsi_signal
            },

            # Bollinger Bands
            "bollinger"     : {
                "upper"     : upper_band,
                "middle"    : middle_band,
                "lower"     : lower_band,
                "signal"    : bb_signal
            },

            # Volume
            "volume"        : {
                "current"   : current_vol,
                "average"   : avg_vol,
                "spike"     : is_spike
            },

            # Alert
            "alert"         : alert,
            "records"       : len(df)
        }

    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


# ─── All Symbols ─────────────────────────────────────────────
def get_all_insights():
    return {symbol: get_insights(symbol) for symbol in ALL_SYMBOLS}


def get_all_summaries():
    return {symbol: get_summary(symbol) for symbol in ALL_SYMBOLS}


# ─── Test Run ────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== FULL SUMMARY FOR AAPL ===\n")
    summary = get_summary("AAPL")
    for key, value in summary.items():
        print(f"  {key:15} : {value}")

    print("\n=== QUICK INSIGHTS ALL STOCKS ===\n")
    for symbol, data in get_all_insights().items():
        if "error" not in data:
            print(f"  {symbol:10} | ${data['current_price']:>10} | "
                  f"MA5: {data['ma5']} | MA20: {data['ma20']} | {data['trend']}")
        else:
            print(f"  {symbol:10} | ERROR: {data['error']}")