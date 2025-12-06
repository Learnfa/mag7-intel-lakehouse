import requests
import pandas as pd
from fake_useragent import UserAgent
from datetime import datetime, timedelta
import numpy as np 
import sys # <-- Import the sys module

BASE_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
ua = UserAgent()

def process_api_list_to_df(api_data_list, column_name):
    """
    Converts a list of API data points (x=timestamp, y=value)
    into a date-indexed pandas DataFrame.
    """
    if not api_data_list:
        return pd.DataFrame()
        
    df_temp = pd.DataFrame(api_data_list)
    df_temp['Date'] = pd.to_datetime(df_temp['x'], unit='ms').dt.strftime('%Y-%m-%d')
    df_temp = df_temp.rename(columns={'y': column_name})
    df_temp = df_temp.set_index('Date')
    return df_temp[[column_name]]

def fetch_fng_data(direction: str, days: int) -> pd.DataFrame:
    """
    Fetches and processes CNN Fear & Greed Index data based on a relative time window.
    """
    today = datetime.now().date()
    
    if direction.lower() == 'forward':
        api_start_date = today.strftime('%Y-%m-%d')
        end_date = (today + timedelta(days=days)).strftime('%Y-%m-%d')
    elif direction.lower() == 'backward':
        api_start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    else:
        raise ValueError("Direction must be 'forward' or 'backward'.")

    print(f"--- Fetching data: {api_start_date} to {end_date} ---")

    # API Call
    headers = {'User-Agent': ua.random}
    try:
        r = requests.get(BASE_URL + api_start_date, headers=headers)
        r.raise_for_status() 
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
        return pd.DataFrame()

    # Process and Join
    df_fng = process_api_list_to_df(data.get('fear_and_greed_historical', {}).get('data', []), 'Fear_Greed')
    df_sp500 = process_api_list_to_df(data.get('market_momentum_sp500', {}).get('data', []), 'Mkt_sp500')
    df_sp125 = process_api_list_to_df(data.get('market_momentum_sp125', {}).get('data', []), 'Mkt_sp125')
    df_stock_strength = process_api_list_to_df(data.get('stock_price_strength', {}).get('data', []), 'Stock_Strength')
    df_stock_breadth = process_api_list_to_df(data.get('stock_price_breadth', {}).get('data', []), 'Stock_breadth')
    df_put_call = process_api_list_to_df(data.get('put_call_options', {}).get('data', []), 'Put_Call')
    df_volatility = process_api_list_to_df(data.get('market_volatility_vix', {}).get('data', []), 'Volatility')
    df_volatility_50 = process_api_list_to_df(data.get('market_volatility_vix_50', {}).get('data', []), 'Volatility_50')
    df_safe_haven = process_api_list_to_df(data.get('safe_haven_demand', {}).get('data', []), 'Safe_Haven')
    df_junk_bonds = process_api_list_to_df(data.get('junk_bond_demand', {}).get('data', []), 'Junk_Bonds')

    final_df = df_fng.join([
        df_sp500, df_sp125, df_stock_strength, df_stock_breadth, df_put_call, 
        df_volatility, df_volatility_50, df_safe_haven, df_junk_bonds
    ], how='outer')
    
    final_df.sort_index(inplace=True)

    # Filter the final DataFrame to the exact END_DATE
    final_df = final_df.loc[final_df.index <= end_date]

    return final_df

# --- Execution Block ---

if __name__ == "__main__":
    # sys.argv[0] is the script name itself
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <direction> <days>")
        print("Example: python script_name.py forward 7")
        print("Example: python script_name.py backward 14")
        sys.exit(1)

    direction = sys.argv[1].lower()
    
    try:
        days = int(sys.argv[2])
    except ValueError:
        print("Error: The second argument (days) must be an integer.")
        sys.exit(1)

    try:
        df = fetch_fng_data(direction, days)
        
        if not df.empty:
            filename = f"cnn_fng_indicators_{direction}_{days}_days.csv"
            df.to_csv(filename)
            print(f"\n✅ Successfully created {filename}!")
            print(f"Shape: {df.shape}")
            print("\nHead of data:")
            print(df.head() if direction == 'backward' else df.tail())
        else:
            print("\n❌ DataFrame is empty. Check API connection or input parameters.")

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)