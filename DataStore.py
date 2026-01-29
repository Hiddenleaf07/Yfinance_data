import os
import pickle
import time
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
BATCH_SIZE = 100          # balanced batch size
MAX_WORKERS = 8 if (os.getenv("CI") or os.getenv("GITHUB_ACTIONS")) else 14  # 8 for CI, 14 for local
MAX_RETRIES = 2           # quick retries only
CI_ENVIRONMENT = os.getenv("CI") or os.getenv("GITHUB_ACTIONS")  # detect CI

# User-Agent headers to avoid being blocked
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def create_session():
    """Create a requests session with proper User-Agent and retry strategy."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session

def read_stock_list(stock_list_path=STOCK_LIST_PATH):
    """Read stock tickers from CSV file."""
    try:
        df = pd.read_csv(stock_list_path)
        tickers = df["SYMBOL"].astype(str).tolist()
        tickers = [t if t.startswith("^") or t.endswith(".NS") else f"{t}.NS" for t in tickers]
        return tickers
    except Exception as e:
        print(f"Error reading stock list from {stock_list_path}: {e}")
        return []

def download_single_stock(stock_code, period, interval):
    """Download data for a single stock with minimal delays."""
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            # Create session with User-Agent
            session = create_session()
            ticker = yf.Ticker(stock_code, session=session)
            
            # Tiny delay on retry only
            if attempt > 0:
                time.sleep(0.2 * attempt)
            
            data = ticker.history(
                period=period,
                interval=interval,
                auto_adjust=True,
                rounding=True,
                timeout=10
            )
            if not data.empty:
                return stock_code, data.round(2)
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                attempt += 1
                if attempt <= MAX_RETRIES:
                    time.sleep(1 * attempt)  # only 1-2 sec delay on 429
            else:
                print(f"Error downloading {stock_code} (attempt {attempt+1}): {e}")
                attempt += 1
    return stock_code, None

def download_batch_stocks(tickers, period="1y", interval="1d"):
    """Download stock data in parallel batches - optimized for speed."""
    all_data = {}
    failed = []
    total = len(tickers)
    print(f"[Batch Download] Starting {total} stocks, batch {BATCH_SIZE}, workers {MAX_WORKERS} (CI: {bool(CI_ENVIRONMENT)})")
    overall_start = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch = tickers[batch_start:batch_start+BATCH_SIZE]
        batch_num = batch_start//BATCH_SIZE+1
        batch_start_time = time.time()
        batch_success = 0
        batch_failed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {
                executor.submit(download_single_stock, ticker, period, interval): ticker
                for ticker in batch
            }
            for future in as_completed(future_to_ticker):
                stock_code, data = future.result()
                if data is not None:
                    all_data[stock_code] = data
                    batch_success += 1
                else:
                    failed.append(stock_code)
                    batch_failed += 1

        batch_time = time.time() - batch_start_time
        print(f"[Batch {batch_num}] {batch_success}✓ {batch_failed}✗ ({batch_time:.1f}s)")

    # Retry failed tickers with single worker
    if failed:
        print(f"[Retry] Attempting {len(failed)} failed tickers...")
        retry_start = time.time()
        retry_failed = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_ticker = {
                executor.submit(download_single_stock, ticker, period, interval): ticker
                for ticker in failed
            }
            for future in as_completed(future_to_ticker):
                stock_code, data = future.result()
                if data is not None:
                    all_data[stock_code] = data
                else:
                    retry_failed.append(stock_code)
        
        recovered = len(failed) - len(retry_failed)
        retry_time = time.time() - retry_start
        print(f"[Retry] Recovered {recovered}, Still failed {len(retry_failed)} ({retry_time:.1f}s)")
        failed = retry_failed

    overall_end = time.time()
    print(f"[Batch Download] Finished: {len(all_data)} downloaded, {len(failed)} failed. "
          f"Total time: {overall_end - overall_start:.2f} seconds")
    return all_data, failed

def save_stock_data(stock_data, save_dir=RESULTS_PKL_DIR):
    """Save stock data dict to a pickle file."""
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    date_suffix = datetime.now().strftime("%d%m%y")
    filename = f"stock_data_{date_suffix}.pkl"
    filepath = os.path.join(save_dir, filename)
    try:
        converted_data = {}
        for k, v in stock_data.items():
            new_key = k[:-3] if k.endswith(".NS") else k
            if hasattr(v, "to_dict"):
                df_copy = v.copy()
                if not isinstance(df_copy.index.dtype, pd.DatetimeTZDtype):
                    df_copy.index = pd.to_datetime(df_copy.index).tz_localize(
                        "Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward"
                    )
                converted_data[new_key] = df_copy.to_dict("split")
            else:
                converted_data[new_key] = v
        with open(filepath, "wb") as f:
            pickle.dump(converted_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"Saved stock data for {len(converted_data)} tickers to {filepath}")
        return filepath
    except Exception as e:
        print(f"Error saving pickle file: {e}")
        return None

def load_stock_data(pickle_path):
    """Load stock data dict from pickle file and convert dicts in 'split' format to DataFrames if needed."""
    if not os.path.exists(pickle_path):
        print(f"Pickle file {pickle_path} does not exist.")
        return {}
    try:
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
        for k, v in data.items():
            if isinstance(v, dict) and set(v.keys()) == {"index", "columns", "data"}:
                data[k] = pd.DataFrame(**v)
        print(f"Loaded stock data for {len(data)} tickers from {pickle_path}")
        return data
    except Exception as e:
        print(f"Error loading pickle file: {e}")
        return {}

if __name__ == "__main__":
    tickers = read_stock_list()
    if not tickers:
        print("No tickers to download.")
    else:
        stock_data, failed = download_batch_stocks(tickers, period="1y", interval="1d")
        save_path = save_stock_data(stock_data)
        loaded_data = load_stock_data(save_path) if save_path else None