"""
fetch_data.py
Pulls real market data using yfinance and stores in DuckDB
"""
import yfinance as yf
import duckdb
import pandas as pd
from datetime import datetime, timedelta

# Tickers covering 5 major sectors
TICKERS = {
    "Technology": ["AAPL", "NVDA", "MSFT", "GOOGL"],
    "Energy":     ["XOM", "CVX", "COP"],
    "Finance":    ["JPM", "BAC", "GS"],
    "Healthcare": ["JNJ", "UNH", "PFE"],
    "Consumer":   ["AMZN", "WMT", "MCD"]
}

ALL_TICKERS = [t for tickers in TICKERS.values() for t in tickers]

def fetch_and_store(days_back: int = 90, db_path: str = "market_data.duckdb"):
    """Fetch stock data and store in DuckDB"""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)

    print(f"Fetching data for {len(ALL_TICKERS)} tickers...")
    
    all_data = []
    for sector, tickers in TICKERS.items():
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date)
                if hist.empty:
                    continue
                hist = hist.reset_index()
                hist["ticker"] = ticker
                hist["sector"] = sector
                hist = hist.rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume"
                })
                hist["date"] = hist["date"].dt.date
                all_data.append(hist[[
                    "date", "ticker", "sector",
                    "open", "high", "low", "close", "volume"
                ]])
                print(f"  ✓ {ticker} ({sector})")
            except Exception as e:
                print(f"  ✗ {ticker}: {e}")

    if not all_data:
        raise ValueError("No data fetched")

    df = pd.concat(all_data, ignore_index=True)

    # Store in DuckDB
    con = duckdb.connect(db_path)
    con.execute("DROP TABLE IF EXISTS stock_prices")
    con.execute("""
        CREATE TABLE stock_prices AS
        SELECT * FROM df
    """)
    
    # Add computed columns
    con.execute("""
        CREATE OR REPLACE VIEW stock_enriched AS
        SELECT *,
            close - open                                AS daily_change,
            ROUND((close - open) / open * 100, 2)      AS daily_change_pct,
            AVG(volume) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )                                           AS avg_volume_30d,
            AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )                                           AS avg_close_7d
        FROM stock_prices
    """)

    row_count = con.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    print(f"\n✅ Stored {row_count} rows in DuckDB")
    con.close()
    return df

if __name__ == "__main__":
    fetch_and_store()