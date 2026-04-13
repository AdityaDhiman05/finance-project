from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
import pandas as pd
import pymysql
from insights import (
    get_insights, get_all_insights,
    get_summary, get_all_summaries
)
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, ALL_SYMBOLS

app = Flask(__name__)
CORS(app)


def get_engine():
    from urllib.parse import quote_plus
    encoded = quote_plus(DB_PASSWORD)
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{encoded}@{DB_HOST}/{DB_NAME}"
    )


# ─── Home ────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status"   : "running",
        "message"  : "Finance Analytics API v2.0",
        "version"  : "2.0",
        "symbols"  : ALL_SYMBOLS,
        "endpoints": {
            "home"          : "/",
            "symbols"       : "/symbols",
            "raw data"      : "/get-data?symbol=AAPL",
            "analytics"     : "/analytics?symbol=AAPL",
            "insights"      : "/insights?symbol=AAPL",
            "all insights"  : "/all-insights",
            "summary"       : "/summary?symbol=AAPL",
            "all summaries" : "/all-summaries",
        }
    })


# ─── Symbols ─────────────────────────────────────────────────
@app.route("/symbols", methods=["GET"])
def get_symbols():
    return jsonify({
        "status"  : "success",
        "count"   : len(ALL_SYMBOLS),
        "symbols" : ALL_SYMBOLS
    })


# ─── Raw Data ────────────────────────────────────────────────
@app.route("/get-data", methods=["GET"])
def get_data():
    symbol = request.args.get("symbol", "AAPL").upper()
    limit  = int(request.args.get("limit", 100))

    engine = get_engine()
    query  = text("""
        SELECT datetime, symbol, open, high, low, close, volume
        FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY datetime DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "limit": limit})

    df['datetime'] = df['datetime'].astype(str)
    return jsonify({
        "status" : "success",
        "symbol" : symbol,
        "count"  : len(df),
        "data"   : df.to_dict(orient="records")
    })


# ─── Analytics ───────────────────────────────────────────────
@app.route("/analytics", methods=["GET"])
def analytics():
    symbol = request.args.get("symbol", "AAPL").upper()

    engine = get_engine()
    query  = text("""
        SELECT datetime, close, volume
        FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY datetime DESC
        LIMIT 50
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol})

    df['ma5']      = df['close'].rolling(5).mean().round(2)
    df['ma20']     = df['close'].rolling(20).mean().round(2)
    df['datetime'] = df['datetime'].astype(str)

    return jsonify({
        "status"    : "success",
        "symbol"    : symbol,
        "count"     : len(df),
        "analytics" : df.to_dict(orient="records")
    })


# ─── Basic Insights ──────────────────────────────────────────
@app.route("/insights", methods=["GET"])
def insights():
    symbol = request.args.get("symbol", "AAPL").upper()
    result = get_insights(symbol)
    return jsonify({"status": "success", "insights": result})


# ─── All Basic Insights ──────────────────────────────────────
@app.route("/all-insights", methods=["GET"])
def all_insights():
    results = get_all_insights()
    return jsonify({
        "status"   : "success",
        "count"    : len(results),
        "insights" : results
    })


# ─── Full Summary (MA + RSI + Bollinger + Volume) ────────────
@app.route("/summary", methods=["GET"])
def summary():
    """
    Full technical analysis for one symbol.
    Returns MA5, MA20, trend, RSI, Bollinger Bands,
    volume spike detection and price alert in one response.
    """
    symbol = request.args.get("symbol", "AAPL").upper()
    result = get_summary(symbol)
    return jsonify({"status": "success", "summary": result})


# ─── All Summaries ───────────────────────────────────────────
@app.route("/all-summaries", methods=["GET"])
def all_summaries():
    """
    Full technical analysis for ALL 16 symbols at once.
    This is the main endpoint for Chahat's dashboard.
    """
    results = get_all_summaries()
    return jsonify({
        "status"    : "success",
        "count"     : len(results),
        "summaries" : results
    })


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  Finance Analytics API v2.0")
    print(f"  Tracking {len(ALL_SYMBOLS)} assets")
    print("\n  Endpoints:")
    print("  http://127.0.0.1:5000/")
    print("  http://127.0.0.1:5000/symbols")
    print("  http://127.0.0.1:5000/get-data?symbol=AAPL")
    print("  http://127.0.0.1:5000/analytics?symbol=AAPL")
    print("  http://127.0.0.1:5000/insights?symbol=AAPL")
    print("  http://127.0.0.1:5000/all-insights")
    print("  http://127.0.0.1:5000/summary?symbol=AAPL")
    print("  http://127.0.0.1:5000/all-summaries")
    print("=" * 50 + "\n")
    app.run(debug=True, port=5000)