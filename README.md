# Market Intelligence Assistant

An AI-powered market analytics tool built with Claude AI, DuckDB, yfinance, and Streamlit.

## What it does
- Fetches real stock market data for 16 tickers across 5 sectors
- Detects anomalies — volume spikes, price swings, sector divergences
- Generates AI-powered morning market briefs
- Answers plain English questions about market data (Text-to-SQL)
- Explains anomalies in plain English

## Tech Stack
- **yfinance** — real market data, free
- **DuckDB** — local analytics layer
- **Claude AI (Anthropic)** — AI brain for narration and Text-to-SQL
- **Streamlit** — web UI
- **Plotly** — interactive charts
- **Python 3.10**

## Project Structure
```
├── app.py                      # Streamlit web UI
├── data/
│   └── fetch_data.py           # yfinance data ingestion
├── analytics/
│   ├── anomaly_detector.py     # Statistical anomaly detection
│   └── market_summary.py       # DuckDB SQL analytics
├── ai/
│   └── ai_agent_example.py     # AI layer structure (implementation private)
└── requirements.txt
```

## Setup
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/market-intelligence-assistant.git
cd market-intelligence-assistant

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ANTHROPIC_API_KEY="your-key-here"

# Fetch market data
python3 data/fetch_data.py

# Launch the app
streamlit run app.py
```

## Part of my #BuildingInPublic series
Week 3 of building in public — one AI/Data Engineering project per week.
