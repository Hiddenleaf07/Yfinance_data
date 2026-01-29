import os
import pickle
import time
import random
import pandas as pd
import yfinance as yf
from datetime import datetime

# --- CONFIGURATION ---
STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
# 100 is a sweet spot; too high and the URL string gets too long for the API
BATCH_SIZE = 100         
# GitHub Actions IPs need more "breathing room" than local machines
SLEEP_BETWEEN_BATCHES = (5, 12) 

def read_stock_list(stock_list_path=STOCK_LIST_PATH):
    try:
        df = pd.read_csv(stock_list_path)
        tickers = df["SYMBOL"].astype(str).tolist()
        return [t if t.startswith("^") or t.endswith(".NS") else f"{t}.NS" for t in tickers]
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

def download_all_stocks(tickers, period="1y", interval="1d"):
    """Downloads tickers in batches. yfinance will use curl_cffi internally if installed."""
    all_data = {}
    failed = []
    
    total_tickers = len(tickers)
    print(f"Starting download for {total_tickers} stocks...")

    for i in range(0, total_tickers, BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_str = " ".join(batch)
        print(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} tickers)...")

        try:
            # IMPORTANT: We removed 'session=session'. 
            # yfinance 0.2.40+ handles the curl_cffi integration automatically.
            data = yf.download(
                tickers=batch_str,
                period=period,
                interval=interval,
                group_by='ticker',
                threads=True,
                progress=False,
                timeout=30
            )

            for ticker in batch:
                try:
                    # Handle MultiIndex returned by bulk download
                    if len(batch) > 1:
                        if ticker in data.columns.levels[0]:
                            ticker_df = data[ticker].dropna(how='all')
                        else:
                            ticker_df = pd.DataFrame()
                    else:
                        ticker_df = data.dropna(how='all')
                    
                    if not ticker_df.empty:
                        all_data[ticker] = ticker_df.round(2)
                    else:
                        failed.append(ticker)
                except Exception:
                    failed.append(ticker)

        except Exception as e:
            print(f"Major error in batch: {e}")
            failed.extend(batch)

        # Anti-ban sleep (Crucial for GitHub Actions)
        if i + BATCH_SIZE < total_tickers:
            wait = random.uniform(*SLEEP_BETWEEN_BATCHES)
            print(f"Waiting {wait:.2f}s to avoid rate limits...")
            time.sleep(wait)

    print(f"Finished: {len(all_data)} success, {len(failed)} failed.")
    return all_data, failed

def save_stock_data(stock_data, save_dir=RESULTS_PKL_DIR):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    date_suffix = datetime.now().strftime("%d%m%y")
    filepath = os.path.join(save_dir, f"stock_data_{date_suffix}.pkl")
    
    try:
        converted_data = {}
        for k, v in stock_data.items():
            clean_key = k[:-3] if k.endswith(".NS") else k
            df_copy = v.copy()
            
            # Standardize index to Kolkata time
            if not isinstance(df_copy.index.dtype, pd.DatetimeTZDtype):
                df_copy.index = pd.to_datetime(df_copy.index).tz_localize(
                    "Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward"
                )
            converted_data[clean_key] = df_copy.to_dict("split")

        with open(filepath, "wb") as f:
            pickle.dump(converted_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        print(f"Saved {len(converted_data)} stocks to {filepath}")
        return filepath
    except Exception as e:
        print(f"Save error: {e}")
        return None

if __name__ == "__main__":
    start_time = time.time()
    
    tickers = read_stock_list()
    if tickers:
        stock_data, failed_tickers = download_all_stocks(tickers)
        if stock_data:
            save_stock_data(stock_data)
        
        if failed_tickers:
            print(f"Tickers that failed ({len(failed_tickers)} total).")
            
    print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")