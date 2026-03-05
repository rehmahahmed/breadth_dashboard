import pandas as pd
import datetime
import time
import urllib.request
import json
import pyotp
import numpy as np
import os
from SmartApi import SmartConnect
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. CREDENTIALS & CONFIGURATION
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

INPUT_FILE = "nifty750list.csv"
OUTPUT_FILE = "market_breadth_history.csv"
INTERVAL = "ONE_DAY"

# We need 200 trading days for the 200 DMA, so we pull 1 full calendar year
end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=365)

TO_DATE = end_date.strftime("%Y-%m-%d 15:30")
FROM_DATE = start_date.strftime("%Y-%m-%d 09:15")

# ==========================================
# 2. LOGIN & FETCH TOKENS
# ==========================================
print("Logging into Angel One...")
smartApi = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
login_data = smartApi.generateSession(CLIENT_CODE, PIN, totp)

if not login_data['status']:
    print("Login Failed:", login_data['message'])
    exit()

print("Fetching instrument tokens...")
instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = urllib.request.urlopen(instrument_url)
instrument_list = json.loads(response.read())

token_map = {inst['symbol'].replace('-EQ', ''): inst['token'] 
             for inst in instrument_list if inst['exch_seg'] == 'NSE' and inst['symbol'].endswith('-EQ')}

# ==========================================
# 3. LOAD SYMBOLS & FETCH DATA
# ==========================================
try:
    df_tickers = pd.read_csv(INPUT_FILE)
    symbols = df_tickers['Symbol'].tolist()
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit()

print(f"Fetching 1-year history for {len(symbols)} stocks. This will take ~10-15 minutes...")
raw_data_rows = []

for i, symbol in enumerate(symbols):
    symbol = str(symbol).strip()
    if symbol not in token_map: continue

    historicParam = {
        "exchange": "NSE", "symboltoken": token_map[symbol],
        "interval": INTERVAL, "fromdate": FROM_DATE, "todate": TO_DATE
    }

    # --- NEW: RETRY LOGIC FOR RATE LIMITS ---
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist_data = smartApi.getCandleData(historicParam)
            
            # If successful
            if hist_data and hist_data.get('status') and hist_data.get('data'):
                for row in hist_data['data']:
                    raw_data_rows.append({
                        'Date': row[0][:10],
                        'Symbol': symbol,
                        'Close': row[4]
                    })
                break # Break out of the retry loop, move to next stock
            
            # If we hit the rate limit specifically
            elif hist_data and hist_data.get('errorcode') == 'AB1004':
                print(f"Rate limited on {symbol}. Cooling down for 3 seconds... (Attempt {attempt+1}/{max_retries})")
                time.sleep(3) # Wait longer before retrying
            
            # If it's some other API error (like invalid token), just skip
            else:
                break 

        except Exception as e:
            # Handle network disconnects or timeouts
            print(f"Network error on {symbol}: {e}. Retrying...")
            time.sleep(2)
            
    # Base rate limit delay (1 second is much safer for 750 stocks)
    time.sleep(1) 
    
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")
# ==========================================
# 4. CALCULATE METRICS FOR ENTIRE UNIVERSE
# ==========================================
if not raw_data_rows:
    print("No data fetched from Angel API. Exiting.")
    exit()

print("Pivoting data and calculating moving averages...")
df_all = pd.DataFrame(raw_data_rows)

# Pivot so Dates are rows and Symbols are columns (much faster for DMA math)
df_close = df_all.pivot(index='Date', columns='Symbol', values='Close')
df_close.index = pd.to_datetime(df_close.index)
df_close = df_close.sort_index()

# Calculate Technicals
daily_returns = df_close.pct_change() * 100
dma_20 = df_close.rolling(window=20).mean()
dma_50 = df_close.rolling(window=50).mean()
dma_200 = df_close.rolling(window=200).mean()

# Extract today's exact values
latest_close = df_close.iloc[-1]
latest_return = daily_returns.iloc[-1]
latest_20 = dma_20.iloc[-1]
latest_50 = dma_50.iloc[-1]
latest_200 = dma_200.iloc[-1]

# ==========================================
# 5. AGGREGATE COUNTS & SAVE
# ==========================================
print("Aggregating breadth metrics...")
metrics = {
    "Date": datetime.datetime.now().strftime("%Y-%m-%d"),
    "Up_4.5_pct": (latest_return >= 4.5).sum(),
    "Down_4.5_pct": (latest_return <= -4.5).sum(),
    "Up_20_pct": (latest_return >= 20.0).sum(),
    "Down_20_pct": (latest_return <= -20.0).sum(),
    "Above_20_DMA": (latest_close > latest_20).sum(),
    "Below_20_DMA": (latest_close < latest_20).sum(),
    "Above_50_DMA": (latest_close > latest_50).sum(),
    "Below_50_DMA": (latest_close < latest_50).sum(),
    "Above_200_DMA": (latest_close > latest_200).sum(),
    "Below_200_DMA": (latest_close < latest_200).sum()
}

df_today = pd.DataFrame([metrics])

if os.path.exists(OUTPUT_FILE):
    df_history = pd.read_csv(OUTPUT_FILE)
    df_final = pd.concat([df_history, df_today], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['Date'], keep='last')
else:
    df_final = df_today

df_final.to_csv(OUTPUT_FILE, index=False)

print("\n=== TODAY'S BREADTH SUMMARY ===")
for key, value in metrics.items():
    print(f"{key}: {value}")
print(f"\n[SUCCESS] Saved to {OUTPUT_FILE}")
