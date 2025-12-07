import pandas as pd
import os
import yfinance as yf
from yahoofinancials import YahooFinancials

# --------- Paths & Env ---------

#PROJECT_ROOT = Path(__file__).resolve().parents[2]
#load_dotenv(PROJECT_ROOT / ".env")

#BASE_OUTPUT = os.getenv("OUTPUT_DIR", "./data")
#BASE_OUTPUT_DIR = PROJECT_ROOT / BASE_OUTPUT

MAG7 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META']
# 1. Initialize an empty list to store the DataFrames
list_mag7_actions = []

for x in MAG7:
    ticker = yf.Ticker(x)
    df = ticker.actions
    # Ensure the DataFrame is not empty before adding the 'Symbol' column
    if not df.empty:
        df['Symbol'] = x
        # 2. Append the current stock's actions DataFrame to the list
        list_mag7_actions.append(df)
    else:
        print(f"No actions found for {x}")

# 3. Concatenate all DataFrames in the list *after* the loop is finished
df_mag7 = pd.concat(list_mag7_actions)

#output_dir = BASE_OUTPUT_DIR / "stocks"
#output_dir = "./data/stocks"
#output_dir.mkdir(parents=True, exist_ok=True)
output_path = "./data/stocks/mag7.csv"

df_mag7.to_csv(output_path,index=False)