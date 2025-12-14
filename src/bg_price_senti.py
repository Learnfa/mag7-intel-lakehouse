import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# --- Configuration ---
DATA_SOURCE_PROJECT = "spring-carving-477913-t2"
DATASET_ID = "mag7_intel_core"
TABLE_PRICES = "fact_prices"
TABLE_SENTIMENT = "fact_macro_sentiment_daily"

# --- 1. Data Fetching Function ---
@st.cache_data(ttl=3600)
def load_data():
    """
    Fetches price data and joins it with detailed macro sentiment data from BigQuery.
    """
    client = bigquery.Client() 

    # Added specific market indicators to the query
    query = f"""
        SELECT
            p.trade_date,
            p.ticker,
            p.open,
            p.high,
            p.low,
            p.adj_close,
            p.volume,
            p.fwd_return_1d,
            p.fwd_return_5d,
            p.fwd_return_10d,
            p.fwd_return_20d,
            p.ma_20,
            p.ma_50,
            p.ma_200,
            s.fear_greed,
            s.mkt_sp500,
            s.mkt_sp125,
            s.stock_strength,
            s.stock_breadth,
            s.put_call,
            s.volatility,
            s.volatility_50,
            s.safe_haven,
            s.junk_bonds
        FROM
            `{DATA_SOURCE_PROJECT}.{DATASET_ID}.{TABLE_PRICES}` AS p
        LEFT JOIN
            `{DATA_SOURCE_PROJECT}.{DATASET_ID}.{TABLE_SENTIMENT}` AS s
        ON
            p.trade_date = s.trade_date
        ORDER BY
            p.trade_date ASC
    """
    
    df = client.query(query).to_dataframe()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df

# --- 2. Streamlit Application ---
st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")
st.title("📈 Stock Analysis & Sentiment Dashboard")

try:
    with st.spinner('Fetching data from BigQuery...'):
        df = load_data()

    if not df.empty:
        # --- 3. Sidebar Controls ---
        ticker_list = sorted(df['ticker'].unique().tolist())
        selected_ticker = st.sidebar.selectbox("Select Ticker", ticker_list)
        
        # Date Selection
        min_date_avail = df['trade_date'].min().date()
        max_date_avail = df['trade_date'].max().date()

        st.sidebar.markdown("---")
        st.sidebar.subheader("Time Period")
        
        start_date = st.sidebar.date_input("Start Date", value=min_date_avail, min_value=min_date_avail, max_value=max_date_avail)
        end_date = st.sidebar.date_input("End Date", value=max_date_avail, min_value=min_date_avail, max_value=max_date_avail)

        if start_date > end_date:
            st.sidebar.error("Error: Start Date must be before End Date.")

        # --- 4. Data Processing & Filtering ---
        # Filter by Ticker
        df_filtered = df[df['ticker'] == selected_ticker].copy()
        
        # Filter by Date Range
        mask = (df_filtered['trade_date'].dt.date >= start_date) & (df_filtered['trade_date'].dt.date <= end_date)
        df_filtered = df_filtered.loc[mask]

        # Handle missing Fear/Greed (simple fill for visualization)
        df_filtered['fear_greed'] = df_filtered['fear_greed'].fillna(50)

        if df_filtered.empty:
            st.warning(f"No data available for {selected_ticker} between {start_date} and {end_date}.")
        else:
            # --- 5. Main Charts (Price & Volume) ---
            fig = make_subplots(
                rows=2, cols=1, 
                shared_xaxes=True, 
                vertical_spacing=0.05, 
                row_heights=[0.7, 0.3],
                specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
            )

            # Hover Template
            hover_template = (
                "<b>Date:</b> %{x|%Y-%m-%d}<br>" +
                "<b>Close:</b> %{y:.2f}<br>" +
                "<b>Volume:</b> %{customdata[3]:,.0f}<br>" +
                "<b>Fear/Greed:</b> %{customdata[4]:.0f}" +
                "<extra></extra>"
            )

            custom_data = df_filtered[['open', 'high', 'low', 'volume', 'fear_greed']].values

            # Price Line
            fig.add_trace(go.Scatter(
                x=df_filtered['trade_date'], y=df_filtered['adj_close'],
                mode='lines', name='Adj Close',
                line=dict(color='white', width=2),
                customdata=custom_data, hovertemplate=hover_template
            ), row=1, col=1)

            # Moving Averages
            for ma, color in zip(['ma_20', 'ma_50', 'ma_200'], ['cyan', 'yellow', 'magenta']):
                fig.add_trace(go.Scatter(
                    x=df_filtered['trade_date'], y=df_filtered[ma], 
                    mode='lines', name=ma.upper(), line=dict(color=color, width=1)
                ), row=1, col=1)

            # Volume Bar
            fig.add_trace(go.Bar(
                x=df_filtered['trade_date'], y=df_filtered['volume'],
                name='Volume',
                marker=dict(
                    color=df_filtered['fear_greed'],
                    colorscale='RdYlGn', cmin=0, cmax=100,
                    colorbar=dict(title="Fear/Greed", len=0.3, y=0.15)
                )
            ), row=2, col=1)

            fig.update_layout(
                height=600,
                hovermode="x unified",
                template="plotly_dark",
                title_text=f"{selected_ticker} Price & Volume Analysis",
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", y=1.02, x=0, xanchor="left")
            )
            fig.update_xaxes(matches='x')
            st.plotly_chart(fig, use_container_width=True)

            # --- 6. Correlation Analysis Section ---
            st.markdown("---")
            st.markdown("### 🔍 Sentiment & Return Correlations")
            
            # --- Tab 1: Interactive Scatter Analysis ---
            tab1, tab2 = st.tabs(["Indicator vs Return Scatter", "Macro Indicators Heatmap"])
            
            with tab1:
                col1, col2 = st.columns([1, 1])
                
                # --- Left Column: Controls ---
                with col1:
                    st.markdown("#### Configure Plot")
                    
                    # 1. Select Return Horizon (Y-Axis)
                    horizon_options = {
                        'fwd_return_1d': '1-Day Return',
                        'fwd_return_5d': '5-Day Return',
                        'fwd_return_10d': '10-Day Return',
                        'fwd_return_20d': '20-Day Return'
                    }
                    selected_y = st.selectbox(
                        "Select Return Horizon (Y-Axis):", 
                        options=list(horizon_options.keys()),
                        format_func=lambda x: horizon_options[x]
                    )

                    # 2. Select Indicator (X-Axis)
                    # Define available indicators including components
                    indicator_options = [
                        'fear_greed', 'mkt_sp500', 'mkt_sp125', 'stock_strength', 
                        'stock_breadth', 'put_call', 'volatility', 'volatility_50', 
                        'safe_haven', 'junk_bonds'
                    ]
                    # Filter only those present in dataframe
                    available_indicators = [col for col in indicator_options if col in df_filtered.columns]
                    
                    selected_x = st.selectbox(
                        "Select Sentiment/Market Indicator (X-Axis):",
                        options=available_indicators,
                        index=0
                    )

                # --- Right Column: Scatter Plot ---
                with col2:
                    # Prepare data
                    scatter_cols = [selected_x, selected_y, 'volume']
                    scatter_df = df_filtered[scatter_cols].dropna()

                    fig_scatter = px.scatter(
                        scatter_df,
                        x=selected_x,
                        y=selected_y,
                        size="volume",
                        color=selected_y,
                        color_continuous_scale="RdYlGn",
                        trendline="ols",
                        title=f"{selected_x} vs {horizon_options[selected_y]}",
                        labels={
                            selected_x: selected_x.replace('_', ' ').title(), 
                            selected_y: "Forward Return"
                        }
                    )
                    fig_scatter.update_layout(template="plotly_dark", height=450)
                    fig_scatter.update_yaxes(tickformat=".1%")
                    st.plotly_chart(fig_scatter, use_container_width=True)

            # --- Tab 2: Market Indicators Matrix (Heatmap) ---
            with tab2:
                st.markdown("#### 📊 Correlation Heatmap: Returns vs. Macro Indicators")
                
                # Define variable groups
                return_vars = ['fwd_return_1d', 'fwd_return_5d', 'fwd_return_10d', 'fwd_return_20d']
                market_vars = [
                    'mkt_sp500', 'mkt_sp125', 'stock_strength', 'stock_breadth', 
                    'put_call', 'volatility', 'volatility_50', 'safe_haven', 'junk_bonds'
                ]
                
                # Check for columns and drop NaN
                available_market_vars = [c for c in market_vars if c in df_filtered.columns]
                
                if available_market_vars:
                    # Calculate full correlation matrix
                    full_corr_df = df_filtered[return_vars + available_market_vars].dropna()
                    full_corr_matrix = full_corr_df.corr()
                    
                    # Slice the matrix: Rows = Returns, Columns = Market Indicators
                    target_matrix = full_corr_matrix.loc[return_vars, available_market_vars]
                    
                    # Plot Heatmap
                    fig_ind_corr = px.imshow(
                        target_matrix,
                        text_auto=".2f",
                        aspect="auto",
                        color_continuous_scale="RdBu_r",
                        zmin=-1, zmax=1,
                        labels=dict(x="Market Indicator", y="Forward Return", color="Corr"),
                    )
                    
                    # Update layout for better readability
                    fig_ind_corr.update_layout(
                        template="plotly_dark",
                        height=500,
                        xaxis=dict(side="bottom")
                    )
                    
                    st.plotly_chart(fig_ind_corr, use_container_width=True)
                else:
                    st.warning("Market indicator data not available for calculation.")

    else:
        st.warning("No data returned from BigQuery.")

except Exception as e:
    st.error(f"An error occurred: {e}")