import streamlit as st
import pandas as pd
import altair as alt
import numpy as np

# Set Streamlit page configuration
st.set_page_config(
    page_title="Interactive Stock Price Viewer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. Data Loading ---
@st.cache_data
def load_data(price_file, indicators_file):
    """Loads and merges stock price data with indicators safely."""
    debug_info = {}
    
    # A. Load Price Data
    try:
        df_prices = pd.read_csv(price_file)
        df_prices['date'] = pd.to_datetime(df_prices['date'], utc=True).dt.tz_localize(None).dt.normalize()
        
        numeric_cols = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
        for col in numeric_cols:
            df_prices[col] = pd.to_numeric(df_prices[col], errors='coerce')
        
        df_prices = df_prices.dropna(subset=['close', 'volume'])
        debug_info['price_count'] = len(df_prices)
    except Exception as e:
        st.error(f"Error loading Price CSV: {e}")
        return pd.DataFrame(), {}

    # B. Load Indicators Data
    try:
        df_indicators = pd.read_csv(indicators_file)
        date_col = 'Date' if 'Date' in df_indicators.columns else 'date'
        
        df_indicators[date_col] = pd.to_datetime(df_indicators[date_col], utc=True).dt.tz_localize(None).dt.normalize()
        df_indicators['Fear_Greed'] = pd.to_numeric(df_indicators['Fear_Greed'], errors='coerce')
        
        indicators_subset = df_indicators[[date_col, 'Fear_Greed']].copy()
        indicators_subset.rename(columns={date_col: 'date'}, inplace=True)
        indicators_subset = indicators_subset.dropna(subset=['date']).drop_duplicates(subset=['date'])
        
    except Exception as e:
        st.warning(f"Could not load Indicators CSV ({e}). Plotting prices only.")
        indicators_subset = pd.DataFrame(columns=['date', 'Fear_Greed'])

    # C. Merge
    merged_df = pd.merge(df_prices, indicators_subset, on='date', how='left')
    return merged_df, debug_info

# --- 2. Analysis Function ---
def analyze_combined_performance(df, ticker, target_date, lookback_window=200, analysis_window=100, num_brackets=10):
    """
    Analyzes next-day performance grouping by BOTH Price Range AND Fear/Greed Sentiment.
    """
    results = []
    
    # Prepare Data
    df = df.copy()
    df = df.reset_index(drop=True) 
    target_date = pd.to_datetime(target_date)
    
    # Filter & Sort
    df = df[df['ticker'] == ticker]
    df = df[df['date'] <= target_date]
    df = df.sort_values(['ticker', 'date'])
    
    fg_bins = [0, 33.33, 66.67, 100]
    fg_labels = ['Fear', 'Neutral', 'Greed']
    
    for ticker, group in df.groupby('ticker'):
        # Step A: Define Price Brackets (Last 200 Days)
        subset_200 = group.tail(lookback_window)
        if len(subset_200) < 2: continue
            
        min_close = subset_200['close'].min()
        max_close = subset_200['close'].max()
        p_bins = np.linspace(min_close, max_close, num_brackets + 1)
        
        # Step B: Prepare Analysis Set (Last 100 Days)
        subset_100 = group.tail(analysis_window).copy()
        
        # Calculate Next Day Gain
        subset_100['next_day_gain'] = subset_100['close'].shift(-1) - subset_100['close']
        
        # Assign Price Bracket
        subset_100['price_bracket'] = pd.cut(subset_100['close'], bins=p_bins, include_lowest=True)
        
        # Assign Fear/Greed Bracket
        subset_100['fg_bracket'] = pd.cut(subset_100['Fear_Greed'], bins=fg_bins, labels=fg_labels, include_lowest=True)
        
        # Step C: Aggregate
        stats = subset_100.groupby(['price_bracket', 'fg_bracket'], observed=False)['next_day_gain'].agg(['mean', 'count'])
        
        stats['ticker'] = ticker
        results.append(stats)

    if not results:
        return pd.DataFrame()

    return pd.concat(results)

def create_heatmap(df, title):
    """Helper to create a standard heatmap chart."""
    df = df.reset_index()
    df['price_bracket'] = df['price_bracket'].astype(str)
    
    base = alt.Chart(df).encode(
        x=alt.X('fg_bracket:O', title=None, sort=['Fear', 'Neutral', 'Greed'], axis=alt.Axis(labels=True)),
        y=alt.Y('price_bracket:O', title=None, sort='descending', axis=alt.Axis(labels=False))
    )
    
    heatmap = base.mark_rect().encode(
        color=alt.Color('mean:Q', title=None, scale=alt.Scale(scheme='redyellowgreen'), legend=None),
        tooltip=['fg_bracket', 'price_bracket', alt.Tooltip('mean', format='$.2f'), 'count']
    )
    
    text = base.mark_text().encode(
        text=alt.Text('mean:Q', format='.1f'),
        color=alt.value('black')
    )
    
    return (heatmap + text).properties(title=title, height=200)

# --- 3. Main Application Logic ---

PRICE_FILE = '../data/stocks/prices_20200101_20251203.csv'
INDICATORS_FILE = '../data/stocks/all_cnn_indicators_final.csv'

data, debug = load_data(PRICE_FILE, INDICATORS_FILE)

st.title("Interactive Stock Price & Volume Chart 📈")

if not data.empty:
    all_tickers = sorted(data['ticker'].unique())
    selected_ticker = st.sidebar.selectbox("Select Ticker:", options=all_tickers)
    
    # Global Ticker Data (Sorted)
    ticker_data = data[data['ticker'] == selected_ticker].sort_values('date').reset_index(drop=True)

    if ticker_data.empty:
        st.warning(f"No data for {selected_ticker}.")
    else:
        # --- Sidebar: Main Plot Range ---
        min_date = ticker_data['date'].min().date()
        max_date = ticker_data['date'].max().date()
        
        st.sidebar.markdown("---")
        st.sidebar.subheader("Main Plot Range")
        col_s1, col_s2 = st.sidebar.columns(2)
        start_date = col_s1.date_input("Start", value=min_date, min_value=min_date, max_value=max_date)
        end_date = col_s2.date_input("End", value=max_date, min_value=min_date, max_value=max_date)

        # --- A. Main Charts (Price/Volume) ---
        mask = (ticker_data['date'].dt.date >= start_date) & (ticker_data['date'].dt.date <= end_date)
        df_filtered = ticker_data.loc[mask]
        
        x_axis_no_labels = alt.X('date:T', axis=alt.Axis(labels=False, title=None))
        common_tooltip = [alt.Tooltip('date', format='%Y-%m-%d'), 'close', 'volume', 'Fear_Greed']
        
        price_chart = alt.Chart(df_filtered).mark_line().encode(
            x=x_axis_no_labels, y=alt.Y('close:Q', scale=alt.Scale(zero=False)), tooltip=common_tooltip
        ).properties(height=300, title=f"{selected_ticker} Price History")
        
        has_fg = df_filtered['Fear_Greed'].notna().any()
        vol_color = alt.condition('isValid(datum.Fear_Greed)', alt.Color('Fear_Greed:Q', scale=alt.Scale(range=['red', 'green']), legend=None), alt.value('gray')) if has_fg else alt.value('steelblue')
        
        vol_chart = alt.Chart(df_filtered).mark_bar().encode(
            x=alt.X('date:T', title='Date'), y='volume:Q', color=vol_color, tooltip=common_tooltip
        ).properties(height=100)
        
        st.altair_chart((price_chart & vol_chart).resolve_scale(x='shared').interactive(), use_container_width=True)

        # --- B. Comparative Performance Matrix (5-Column) ---
        st.markdown("---")
        st.subheader("📊 Comparative Sentiment Performance Matrix")
        
        col_ctrl1, col_ctrl2 = st.columns([1, 4])
        with col_ctrl1:
            matrix_date_input = st.date_input("Matrix Center Date", value=end_date, min_value=min_date, max_value=max_date)
        
        try:
            target_ts = pd.to_datetime(matrix_date_input)
            idx_list = ticker_data.index[ticker_data['date'] == target_ts].tolist()
            
            if not idx_list:
                st.warning(f"No trading data found for {matrix_date_input}. Please select a valid trading day.")
            else:
                center_idx = idx_list[0]
                
                # Defined Scenarios for 5 Columns: [T-2, T-1, T, T+1, T+2]
                scenarios = [
                    (center_idx - 2, "-2 Days"),
                    (center_idx - 1, "-1 Day"),
                    (center_idx, f"Center ({matrix_date_input.strftime('%m-%d')})"),
                    (center_idx + 1, "+1 Day"),
                    (center_idx + 2, "+2 Days")
                ]
                
                # Create 5 Columns
                cols = st.columns(5)
                
                for i, (idx, label) in enumerate(scenarios):
                    with cols[i]:
                        if 0 <= idx < len(ticker_data):
                            row = ticker_data.iloc[idx]
                            row_date = row['date']
                            row_close = row['close']
                            
                            # Get Fear/Greed and format it
                            row_fg = row.get('Fear_Greed', float('nan'))
                            fg_str = f"{row_fg:.0f}" if pd.notna(row_fg) else "N/A"
                            
                            st.markdown(f"**{label}**")
                            # Display Date | Price | Fear/Greed
                            st.caption(f"{row_date.strftime('%Y-%m-%d')}")
                            st.markdown(f"**${row_close:.2f}** | F&G: **{fg_str}**")
                            
                            perf_df = analyze_combined_performance(data, selected_ticker, row_date)
                            
                            if not perf_df.empty:
                                chart = create_heatmap(perf_df, title="")
                                st.altair_chart(chart, use_container_width=True)
                            else:
                                st.info("No Data")
                        else:
                            st.markdown(f"**{label}**")
                            st.caption("Out of Range")

        except Exception as e:
            st.error(f"Error calculating dates: {e}")

else:
    st.info("Awaiting data...")