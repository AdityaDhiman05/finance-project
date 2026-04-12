# ─── Twelve Data API ────────────────────────────────────────
API_KEY = "e56fbd6bacee40aabdeec516f94fd6c4"

# ─── MySQL Database ─────────────────────────────────────────
DB_USER     = "root"
DB_PASSWORD = "MySQL@@2067#1403"
DB_HOST     = "localhost"
DB_NAME     = "finance_db"

# ─── Stock Batches (16 assets across 4 asset classes) ────────
STOCK_BATCHES = [
    # Batch 1 — US Big Tech
    ["AAPL", "GOOGL", "MSFT", "AMZN"],

    # Batch 2 — US Growth
    ["TSLA", "NVDA", "META", "NFLX"],

    # Batch 3 — Cryptocurrency
    ["BTC/USD", "ETH/USD", "BNB/USD", "XRP/USD"],

    # Batch 4 — Forex
    ["EUR/USD", "GBP/USD", "JPY/USD", "AUD/USD"],
]

# Flat list of all symbols
ALL_SYMBOLS = [s for batch in STOCK_BATCHES for s in batch]

# Fetch interval in minutes
FETCH_INTERVAL = 5