"""
anomaly_detector.py
Detects unusual market activity using statistical thresholds
Volume spikes, price swings, and sector divergences
"""
import duckdb
import pandas as pd

def get_anomalies(db_path: str = "market_data.duckdb") -> pd.DataFrame:
    """Detect anomalies across all tickers"""
    con = duckdb.connect(db_path)

    anomalies = []

    # ANOMALY 1: Volume spike — today's volume > 2x 30-day average
    volume_spikes = con.execute("""
        SELECT
            date,
            ticker,
            sector,
            volume,
            ROUND(avg_volume_30d, 0)        AS avg_volume_30d,
            ROUND(volume / avg_volume_30d, 2) AS volume_ratio,
            close,
            daily_change_pct,
            'Volume Spike'                  AS anomaly_type
        FROM stock_enriched
        WHERE avg_volume_30d > 0
            AND volume > avg_volume_30d * 2
        ORDER BY volume_ratio DESC
    """).df()

    if not volume_spikes.empty:
        volume_spikes["description"] = volume_spikes.apply(
            lambda r: f"{r['ticker']} traded {r['volume_ratio']}x its 30-day average volume on {r['date']}",
            axis=1
        )
        anomalies.append(volume_spikes)

    # ANOMALY 2: Price swing — single day move > 4%
    price_swings = con.execute("""
        SELECT
            date,
            ticker,
            sector,
            volume,
            avg_volume_30d,
            ROUND(volume / NULLIF(avg_volume_30d, 0), 2) AS volume_ratio,
            close,
            daily_change_pct,
            'Price Swing'                   AS anomaly_type
        FROM stock_enriched
        WHERE ABS(daily_change_pct) > 4
        ORDER BY ABS(daily_change_pct) DESC
    """).df()

    if not price_swings.empty:
        price_swings["description"] = price_swings.apply(
            lambda r: f"{r['ticker']} moved {r['daily_change_pct']}% in a single day on {r['date']}",
            axis=1
        )
        anomalies.append(price_swings)

    # ANOMALY 3: Sector divergence — one ticker moves opposite to its sector
    sector_divergence = con.execute("""
        WITH sector_avg AS (
            SELECT
                date,
                sector,
                ROUND(AVG(daily_change_pct), 2) AS sector_avg_pct
            FROM stock_enriched
            GROUP BY date, sector
        )
        SELECT
            s.date,
            s.ticker,
            s.sector,
            s.volume,
            s.avg_volume_30d,
            ROUND(s.volume / NULLIF(s.avg_volume_30d, 0), 2) AS volume_ratio,
            s.close,
            s.daily_change_pct,
            'Sector Divergence'             AS anomaly_type
        FROM stock_enriched s
        JOIN sector_avg sa
            ON s.date = sa.date
            AND s.sector = sa.sector
        WHERE ABS(s.daily_change_pct - sa.sector_avg_pct) > 3
            AND ABS(s.daily_change_pct) > 2
        ORDER BY ABS(s.daily_change_pct - sa.sector_avg_pct) DESC
    """).df()

    if not sector_divergence.empty:
        sector_divergence["description"] = sector_divergence.apply(
            lambda r: f"{r['ticker']} diverged significantly from its {r['sector']} sector peers on {r['date']}",
            axis=1
        )
        anomalies.append(sector_divergence)

    con.close()

    if not anomalies:
        return pd.DataFrame()

    combined = pd.concat(anomalies, ignore_index=True)
    combined = combined.sort_values("date", ascending=False)
    return combined


def get_recent_anomalies(days: int = 7, db_path: str = "market_data.duckdb") -> pd.DataFrame:
    """Get anomalies from the last N days only"""
    all_anomalies = get_anomalies(db_path)
    if all_anomalies.empty:
        return all_anomalies
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
    all_anomalies["date"] = pd.to_datetime(all_anomalies["date"])
    return all_anomalies[all_anomalies["date"] >= cutoff]


if __name__ == "__main__":
    df = get_recent_anomalies(days=7)
    if df.empty:
        print("No anomalies detected in last 7 days")
    else:
        print(f"Found {len(df)} anomalies in last 7 days:\n")
        for _, row in df.iterrows():
            print(f"  ⚠️  {row['description']}")