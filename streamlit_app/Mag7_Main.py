import sys
from pathlib import Path
import streamlit as st

APP_DIR = Path(__file__).resolve().parent  # .../streamlit_app
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

st.set_page_config(
    page_title="MAG7 Intel Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 MAG7 Market & Signal Intelligence")

st.caption(
    "Validated core signal • Explainable regimes • Risk context • Research validation"
)

st.info(
    "Navigation: use the **Pages** sidebar.\n\n"
    "🟢 **Production Truth pages** show the canonical `signal_core` state (no performance).\n"
    "🟡 **Research pages** use `signal_research_*` tables and may include look-ahead metrics."
)

st.markdown("""
### What you can explore
- **Overview** – latest core signal snapshot across tickers
- **Stock & Macro** – a snapshot of tickers with macro overlays and indicator correlations
- **Signal by Momentum/Reversion** – classifies each trading day into one of three actionable states
- **Signal by Bucket Values** – signal history, persistence, and ranking
- **Ticker Deep Dive** – price context + regime corridor + signal locator
- **Regimes** – distribution and explanatory regime behaviour
- **Risk Context** – volatility/drawdown + macro risk overlays (no gating)
""")
