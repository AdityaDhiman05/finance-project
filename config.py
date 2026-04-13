# ─── Twelve Data API ────────────────────────────────────────
API_KEY = "e56fbd6bacee40aabdeec516f94fd6c4"

# ─── MySQL Database ─────────────────────────────────────────
DB_USER     = "root"
DB_PASSWORD = "MySQL@@2067#1403"
DB_HOST     = "localhost"
DB_NAME     = "finance_db"

STOCK_BATCHES = [
    ["AAPL", "GOOGL", "MSFT", "AMZN"],
    ["TSLA", "NVDA", "META", "NFLX"],
    ["BTC/USD", "ETH/USD", "BNB/USD", "XRP/USD"],
    ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"]  # ✅ FIXED
]

ALL_SYMBOLS = [s for batch in STOCK_BATCHES for s in batch]

FETCH_INTERVAL = 5  # in minutes