import os
import pickle
import time
import pandas as pd
import yfinance as yf
from datetime import datetime

STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"

def read_stock_list():
    try:
        df = pd.read_csv(STOCK_LIST_PATH)
        tickers = df["SYMBOL"].astype(str).tolist()
        # Clean and format tickers for Yahoo Finance
        tickers = [t.strip() for t in tickers]
        tickers = [t if t.startswith("^") or t.endswith(".NS") else f"{t}.NS" for t in tickers]
        return tickers
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

def bulk_download(tickers):
    """Downloads all tickers in one single network request."""
    print(f"🚀 Starting Bulk Download for {len(tickers)} stocks...")
    start_time = time.time()
    
    # yf.download is much faster than yf.Ticker().history() in a loop
    data = yf.download(
        tickers=tickers,
        period="1y",
        interval="1d",
        group_by='ticker',
        auto_adjust=True,
        threads=True, # Uses internal threading for speed
        timeout=30
    )
    
    end_time = time.time()
    print(f"⏱ Download finished in {end_time - start_time:.2f} seconds")
    return data

def save_as_pickle(bulk_data):
    if not os.path.exists(RESULTS_PKL_DIR):
        os.makedirs(RESULTS_PKL_DIR, exist_ok=True)
    
    date_suffix = datetime.now().strftime("%d%m%y")
    filename = f"stock_data_{date_suffix}.pkl"
    filepath = os.path.join(RESULTS_PKL_DIR, filename)
    
    # Transform bulk data into your existing dictionary format
    final_dict = {}
    
    # yf.download returns a MultiIndex DataFrame if multiple tickers are requested
    tickers = bulk_data.columns.get_level_values(0).unique()
    
    for ticker in tickers:
        stock_df = bulk_data[ticker].dropna()
        if not stock_df.empty:
            # Format key to remove .NS if needed for your storage logic
            clean_key = ticker[:-3] if ticker.endswith(".NS") else ticker
            # Convert to split format as per your original logic
            final_dict[clean_key] = stock_df.to_dict("split")
            
    with open(filepath, "wb") as f:
        pickle.dump(final_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    print(f"✅ Saved {len(final_dict)} stocks to {filepath}")

if __name__ == "__main__":
    ticker_list = read_stock_list()
    if ticker_list:
        all_data = bulk_download(ticker_list)
        if not all_data.empty:
            save_as_pickle(all_data)
        else:
            print("❌ No data was downloaded.")
