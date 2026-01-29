import os
import pickle
import time
import random
import pandas as pd
import yfinance as yf
from datetime import datetime
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# --- CONFIGURATION ---
STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
BATCH_SIZE = 101         # Yahoo likes ~100 tickers per bulk request
SLEEP_BETWEEN_BATCHES = (3, 7) # Random range in seconds to avoid fingerprinting

def get_smart_session():
    """Create a session that mimics a browser and handles retries."""
    session = Session()
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    session.headers.update({
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    
    # Retry strategy for 429 (Rate Limit) and 500-series errors
    retries = Retry(
        total=5,
        backoff_factor=2, # Wait 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def read_stock_list(stock_list_path=STOCK_LIST_PATH):
    try:
        df = pd.read_csv(stock_list_path)
        tickers = df["SYMBOL"].astype(str).tolist()
        # Ensure .NS suffix for NSE stocks
        tickers = [t if t.startswith("^") or t.endswith(".NS") else f"{t}.NS" for t in tickers]
        return tickers
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

def download_all_stocks(tickers, period="1y", interval="1d"):
    """Downloads tickers in large batches using yfinance's bulk capability."""
    all_data = {}
    failed = []
    session = get_smart_session()
    
    total_tickers = len(tickers)
    print(f"Starting bulk download for {total_tickers} stocks...")

    for i in range(0, total_tickers, BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_str = " ".join(batch)
        print(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} tickers)...")

        try:
            # yf.download is much faster for bulk and harder to rate limit
            # than individual yf.Ticker calls in a loop.
            data = yf.download(
                tickers=batch_str,
                period=period,
                interval=interval,
                group_by='ticker',
                session=session,
                threads=True, # Uses internal yfinance threading
                progress=False
            )

            for ticker in batch:
                try:
                    # If multiple tickers, data has MultiIndex columns [Ticker, PriceType]
                    if len(batch) > 1:
                        ticker_df = data[ticker].dropna(how='all')
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

        # Anti-ban sleep (Jitter)
        if i + BATCH_SIZE < total_tickers:
            wait = random.uniform(*SLEEP_BETWEEN_BATCHES)
            print(f"Sleeping for {wait:.2f}s...")
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
            # Clean ticker name for storage (remove .NS)
            clean_key = k[:-3] if k.endswith(".NS") else k
            
            # Format DataFrame for pickle compatibility
            df_copy = v.copy()
            if not isinstance(df_copy.index.dtype, pd.DatetimeTZDtype):
                df_copy.index = pd.to_datetime(df_copy.index).tz_localize(
                    "Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward"
                )
            converted_data[clean_key] = df_copy.to_dict("split")

        with open(filepath, "wb") as f:
            pickle.dump(converted_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        print(f"Saved data to {filepath}")
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
            print(f"Tickers that failed: {failed_tickers}")
            
    print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")