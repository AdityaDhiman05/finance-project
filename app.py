from flask import Flask, jsonify
from sqlalchemy import create_engine, text
import pandas as pd
import pymysql
from insights import get_insights

app = Flask(__name__)

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

# ─── Endpoint 1 ───────────────────────────────────────
@app.route("/get-data", methods=["GET"])
def get_data():
    engine = get_engine()
    query  = text("""
        SELECT datetime, symbol, open, high, low, close, volume
        FROM stock_prices
        ORDER BY datetime DESC
        LIMIT 100
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['datetime'] = df['datetime'].astype(str)
    return jsonify({
        "status"  : "success",
        "count"   : len(df),
        "data"    : df.to_dict(orient="records")
    })

# ─── Endpoint 2 ───────────────────────────────────────
@app.route("/analytics", methods=["GET"])
def analytics():
    engine = get_engine()
    query  = text("""
        SELECT datetime, close, volume
        FROM stock_prices
        ORDER BY datetime DESC
        LIMIT 50
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df['ma5']  = df['close'].rolling(5).mean().round(2)
    df['ma20'] = df['close'].rolling(20).mean().round(2)
    df['datetime'] = df['datetime'].astype(str)

    return jsonify({
        "status"       : "success",
        "count"        : len(df),
        "analytics"    : df.to_dict(orient="records")
    })

# ─── Endpoint 3 ───────────────────────────────────────
@app.route("/insights", methods=["GET"])
def insights():
    result = get_insights("AAPL")
    return jsonify({
        "status"  : "success",
        "insights": result
    })

# ─── Health check ─────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status"   : "running",
        "message"  : "Finance API is live",
        "endpoints": ["/get-data", "/analytics", "/insights"]
    })

if __name__ == "__main__":
    print("Starting Finance API...")
    print("Endpoints available:")
    print("  http://127.0.0.1:5000/")
    print("  http://127.0.0.1:5000/get-data")
    print("  http://127.0.0.1:5000/analytics")
    print("  http://127.0.0.1:5000/insights")
    app.run(debug=True)