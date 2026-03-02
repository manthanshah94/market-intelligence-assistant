"""
app.py
Streamlit web UI for the Market Intelligence Assistant
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from data.fetch_data import fetch_and_store
from analytics.market_summary import (
    get_sector_performance,
    get_top_movers,
    get_market_snapshot,
    get_ticker_history
)
from analytics.anomaly_detector import get_recent_anomalies
from ai.ai_agent import generate_morning_brief, answer_question, explain_anomaly

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Market Intelligence Assistant",
    page_icon="📈",
    layout="wide"
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: #1c1f26;
        border-radius: 10px;
        padding: 16px;
        border-left: 4px solid #00d4aa;
    }
    .anomaly-card {
        background: #1c1f26;
        border-radius: 10px;
        padding: 16px;
        border-left: 4px solid #ff4b4b;
        margin-bottom: 10px;
    }
    .brief-card {
        background: #1c1f26;
        border-radius: 10px;
        padding: 20px;
        border-left: 4px solid #4b9eff;
        font-size: 15px;
        line-height: 1.7;
    }
    .stButton > button {
        background-color: #00d4aa;
        color: #0e1117;
        font-weight: bold;
        border: none;
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)

DB_PATH = "market_data.duckdb"

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("📈 Market Intelligence Assistant")
st.caption("Powered by Claude AI · Real market data via yfinance · Analytics by DuckDB")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Controls")
    if st.button("🔄 Refresh Market Data", use_container_width=True):
        with st.spinner("Fetching latest market data..."):
            fetch_and_store(days_back=90, db_path=DB_PATH)
        st.success("Data refreshed!")

    st.divider()
    st.caption("Tracking 16 stocks across 5 sectors")
    st.caption("Technology · Finance · Healthcare · Consumer · Energy")

# ── Market Snapshot ────────────────────────────────────────────────────────────
snapshot = get_market_snapshot(DB_PATH)
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Latest Date", str(snapshot["latest_date"]))
with col2:
    st.metric("Stocks Up", snapshot["tickers_up"], delta="↑")
with col3:
    st.metric("Stocks Down", snapshot["tickers_down"], delta="↓")
with col4:
    avg = snapshot["overall_avg_change"]
    st.metric("Avg Daily Change", f"{avg}%", delta=f"{avg}%")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📰 Morning Brief",
    "💬 Ask Anything",
    "⚠️ Anomalies",
    "📊 Market Overview"
])

# ── Tab 1: Morning Brief ───────────────────────────────────────────────────────
with tab1:
    st.subheader("AI-Generated Morning Brief")
    st.caption("Claude analyzes real market data and writes a professional summary")

    if st.button("Generate Morning Brief", use_container_width=False):
        with st.spinner("Claude is analyzing the markets..."):
            brief = generate_morning_brief(DB_PATH)
        st.markdown(f'<div class="brief-card">{brief}</div>',
                    unsafe_allow_html=True)
    else:
        st.info("Click 'Generate Morning Brief' to get today's AI-powered market summary.")

# ── Tab 2: Ask Anything ────────────────────────────────────────────────────────
with tab2:
    st.subheader("Ask Anything About the Market")
    st.caption("Plain English questions → Claude generates SQL → runs against real data → plain English answer")

    # Example questions
    st.markdown("**Try these:**")
    example_cols = st.columns(3)
    examples = [
        "Which sector had the worst week?",
        "What happened to Goldman Sachs?",
        "Which stock had the highest volume spike?",
        "What are the top 3 gainers this week?",
        "Show me NVDA price history",
        "Which healthcare stock performed best?"
    ]
    for i, example in enumerate(examples):
        with example_cols[i % 3]:
            if st.button(example, use_container_width=True, key=f"ex_{i}"):
                st.session_state["question"] = example

    st.divider()
    question = st.text_input(
        "Your question:",
        value=st.session_state.get("question", ""),
        placeholder="e.g. Which sector performed best this week?"
    )

    if question:
        with st.spinner("Claude is thinking..."):
            result = answer_question(question, DB_PATH)

        st.markdown("### 💬 Answer")
        st.markdown(f'<div class="brief-card">{result["answer"]}</div>',
                    unsafe_allow_html=True)

        with st.expander("🔍 See generated SQL"):
            st.code(result["sql"], language="sql")

        if not result["results"].empty and "error" not in result["results"].columns:
            with st.expander("📋 See raw data"):
                st.dataframe(result["results"], use_container_width=True)

# ── Tab 3: Anomalies ───────────────────────────────────────────────────────────
with tab3:
    st.subheader("⚠️ Detected Market Anomalies")
    st.caption("Unusual volume spikes, price swings, and sector divergences — explained by Claude")

    days = st.slider("Look back (days)", min_value=1, max_value=30, value=7)
    anomalies = get_recent_anomalies(days=days, db_path=DB_PATH)

    if anomalies.empty:
        st.success(f"No anomalies detected in the last {days} days.")
    else:
        st.warning(f"Found {len(anomalies)} anomalies in the last {days} days")

        for _, row in anomalies.iterrows():
            with st.container():
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.markdown(
                        f'<div class="anomaly-card">'
                        f'<b>{row["anomaly_type"]}</b> · {row["ticker"]} ({row["sector"]})<br>'
                        f'{row["description"]}'
                        f'</div>',
                        unsafe_allow_html=True
                    )
                with col_b:
                    if st.button("Explain with AI", key=f"explain_{_}"):
                        with st.spinner("Claude is analyzing..."):
                            explanation = explain_anomaly(row["description"])
                        st.info(explanation)

# ── Tab 4: Market Overview ─────────────────────────────────────────────────────
with tab4:
    st.subheader("📊 Market Overview")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Sector Performance (This Week)")
        sectors = get_sector_performance(DB_PATH)
        fig = px.bar(
            sectors,
            x="sector",
            y="total_week_change_pct",
            color="total_week_change_pct",
            color_continuous_scale=["#ff4b4b", "#ffffff", "#00d4aa"],
            color_continuous_midpoint=0,
            labels={"total_week_change_pct": "Weekly Change %", "sector": "Sector"}
        )
        fig.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#1c1f26",
            font_color="white",
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("#### Top Movers (This Week)")
        movers = get_top_movers(DB_PATH)
        gainers = movers["gainers"].copy()
        losers = movers["losers"].copy()
        gainers["type"] = "Gainer"
        losers["type"] = "Loser"
        all_movers = pd.concat([gainers, losers])
        all_movers = all_movers.sort_values("total_change_pct", ascending=True)

        fig2 = px.bar(
            all_movers,
            x="total_change_pct",
            y="ticker",
            color="type",
            orientation="h",
            color_discrete_map={"Gainer": "#00d4aa", "Loser": "#ff4b4b"},
            labels={"total_change_pct": "Weekly Change %", "ticker": ""}
        )
        fig2.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#1c1f26",
            font_color="white",
            showlegend=False
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("#### Price Chart")
    all_tickers = ["AAPL", "NVDA", "MSFT", "GOOGL", "XOM", "CVX",
                   "COP", "JPM", "BAC", "GS", "JNJ", "UNH",
                   "PFE", "AMZN", "WMT", "MCD"]
    selected = st.selectbox("Select ticker", all_tickers)
    history = get_ticker_history(selected, DB_PATH)

    if not history.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=history["date"],
            y=history["close"],
            name="Close",
            line=dict(color="#00d4aa", width=2)
        ))
        fig3.add_trace(go.Scatter(
            x=history["date"],
            y=history["avg_close_7d"],
            name="7-day avg",
            line=dict(color="#4b9eff", width=1, dash="dot")
        ))
        fig3.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#1c1f26",
            font_color="white",
            xaxis_title="Date",
            yaxis_title="Price ($)",
            hovermode="x unified"
        )
        st.plotly_chart(fig3, use_container_width=True)