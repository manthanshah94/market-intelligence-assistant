"""
market_summary.py
SQL analytics layer using DuckDB
Sector performance, top movers, weekly trends
"""
import duckdb
import pandas as pd

def get_sector_performance(db_path: str = "market_data.duckdb") -> pd.DataFrame:
    """Weekly performance by sector"""
    con = duckdb.connect(db_path)
    result = con.execute("""
        WITH latest_week AS (
            SELECT MAX(date) AS max_date FROM stock_prices
        )
        SELECT
            sector,
            ROUND(AVG(daily_change_pct), 2)     AS avg_daily_change_pct,
            ROUND(SUM(daily_change_pct), 2)      AS total_week_change_pct,
            COUNT(DISTINCT ticker)               AS num_tickers,
            ROUND(AVG(volume), 0)                AS avg_volume
        FROM stock_enriched
        WHERE date >= (SELECT max_date - INTERVAL 7 DAYS FROM latest_week)
        GROUP BY sector
        ORDER BY total_week_change_pct DESC
    """).df()
    con.close()
    return result


def get_top_movers(db_path: str = "market_data.duckdb", top_n: int = 5) -> dict:
    """Top gainers and losers over last 7 days"""
    con = duckdb.connect(db_path)

    gainers = con.execute(f"""
        WITH latest_week AS (
            SELECT MAX(date) AS max_date FROM stock_prices
        )
        SELECT
            ticker,
            sector,
            ROUND(SUM(daily_change_pct), 2)     AS total_change_pct,
            ROUND(AVG(close), 2)                AS avg_close
        FROM stock_enriched
        WHERE date >= (SELECT max_date - INTERVAL 7 DAYS FROM latest_week)
        GROUP BY ticker, sector
        ORDER BY total_change_pct DESC
        LIMIT {top_n}
    """).df()

    losers = con.execute(f"""
        WITH latest_week AS (
            SELECT MAX(date) AS max_date FROM stock_prices
        )
        SELECT
            ticker,
            sector,
            ROUND(SUM(daily_change_pct), 2)     AS total_change_pct,
            ROUND(AVG(close), 2)                AS avg_close
        FROM stock_enriched
        WHERE date >= (SELECT max_date - INTERVAL 7 DAYS FROM latest_week)
        GROUP BY ticker, sector
        ORDER BY total_change_pct ASC
        LIMIT {top_n}
    """).df()

    con.close()
    return {"gainers": gainers, "losers": losers}


def get_ticker_history(ticker: str, db_path: str = "market_data.duckdb") -> pd.DataFrame:
    """Full price history for a single ticker"""
    con = duckdb.connect(db_path)
    result = con.execute(f"""
        SELECT
            date,
            ticker,
            sector,
            open,
            high,
            low,
            close,
            volume,
            daily_change_pct,
            avg_close_7d
        FROM stock_enriched
        WHERE ticker = '{ticker}'
        ORDER BY date ASC
    """).df()
    con.close()
    return result


def get_market_snapshot(db_path: str = "market_data.duckdb") -> dict:
    """Overall market snapshot for the morning brief"""
    con = duckdb.connect(db_path)

    snapshot = con.execute("""
        WITH latest_day AS (
            SELECT MAX(date) AS max_date FROM stock_prices
        )
        SELECT
            COUNT(DISTINCT ticker)              AS total_tickers,
            COUNT(DISTINCT sector)              AS total_sectors,
            ROUND(AVG(daily_change_pct), 2)     AS overall_avg_change,
            COUNT(CASE WHEN daily_change_pct > 0
                  THEN 1 END)                   AS tickers_up,
            COUNT(CASE WHEN daily_change_pct < 0
                  THEN 1 END)                   AS tickers_down,
            MAX(date)                           AS latest_date
        FROM stock_enriched
        WHERE date = (SELECT max_date FROM latest_day)
    """).fetchone()

    con.close()

    return {
        "total_tickers":    snapshot[0],
        "total_sectors":    snapshot[1],
        "overall_avg_change": snapshot[2],
        "tickers_up":       snapshot[3],
        "tickers_down":     snapshot[4],
        "latest_date":      snapshot[5]
    }


if __name__ == "__main__":
    print("📊 Sector Performance (Last 7 Days):")
    sectors = get_sector_performance()
    print(sectors.to_string(index=False))

    print("\n🚀 Top Movers:")
    movers = get_top_movers()
    print("Gainers:")
    print(movers["gainers"].to_string(index=False))
    print("\nLosers:")
    print(movers["losers"].to_string(index=False))

    print("\n📈 Market Snapshot:")
    snapshot = get_market_snapshot()
    for k, v in snapshot.items():
        print(f"  {k}: {v}")