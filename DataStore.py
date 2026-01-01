import os
import pickle
import time
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
BATCH_SIZE = 158          # original batch size
MAX_WORKERS = 14          # original worker count
MAX_RETRIES = 0           # no retries
BATCH_DELAY = 1.0         # 1 second delay between batches
REQUEST_TIMEOUT = 10      # timeout per request
TARGET_HOUR = 9           # 9:00 AM to account for delays
TARGET_MINUTE = 0
ACTUAL_RUN_HOUR = 9       # Actual run time (9:29 AM)
ACTUAL_RUN_MINUTE = 29
CUTOFF_HOUR = 10          # Stop running after 10:00 AM
CUTOFF_MINUTE = 0

def check_should_run():
    """Smart scheduling with delay handling and weekend exclusion."""
    now = datetime.now()
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    # Skip weekends (Saturday=5, Sunday=6)
    if weekday >= 5:
        print(f"[Scheduler] ✗ Skipping weekend ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][weekday]})")
        return False
    
    # Force run via environment variable (manual trigger)
    force_run = os.getenv('RUN_FORCE', 'false').lower() == 'true'
    if force_run:
        print(f"[Scheduler] ✓ FORCE RUN enabled at {now.strftime('%H:%M:%S')} (all checks bypassed)")
        return True
    
    current_hour = now.hour
    current_minute = now.minute
    current_time_minutes = current_hour * 60 + current_minute
    
    actual_run_minutes = ACTUAL_RUN_HOUR * 60 + ACTUAL_RUN_MINUTE
    cutoff_minutes = CUTOFF_HOUR * 60 + CUTOFF_MINUTE
    
    # If before 9:29 AM, wait for that time
    if current_time_minutes < actual_run_minutes:
        wait_time = actual_run_minutes - current_time_minutes
        print(f"[Scheduler] ⏳ Early start at {now.strftime('%H:%M:%S')} - waiting {wait_time}min until {ACTUAL_RUN_HOUR:02d}:{ACTUAL_RUN_MINUTE:02d}")
        time.sleep(wait_time * 60)
        return True
    
    # If between 9:29 AM and 10:00 AM, run immediately (handles delays up to 31 minutes)
    elif actual_run_minutes <= current_time_minutes < cutoff_minutes:
        delay_minutes = current_time_minutes - actual_run_minutes
        print(f"[Scheduler] ✓ Running at {now.strftime('%H:%M:%S')} (Delay: {delay_minutes}min from target {ACTUAL_RUN_HOUR:02d}:{ACTUAL_RUN_MINUTE:02d})")
        return True
    
    # If after 10:00 AM, skip this run
    else:
        print(f"[Scheduler] ✗ Current time {now.strftime('%H:%M:%S')} is beyond cutoff {CUTOFF_HOUR:02d}:{CUTOFF_MINUTE:02d} - skipping")
        return False

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
    """Download data for a single stock."""
    try:
        ticker = yf.Ticker(stock_code)
        data = ticker.history(
            period=period,
            interval=interval,
            auto_adjust=True,
            rounding=True,
            timeout=REQUEST_TIMEOUT,
        )
        if not data.empty:
            return stock_code, data.round(2)
    except Exception as e:
        print(f"Error downloading {stock_code}: {e}")
    return stock_code, None

def download_batch_stocks(tickers, period="1y", interval="1d"):
    """Download stock data in parallel batches with strategic delays to avoid rate limiting."""
    all_data = {}
    failed = []
    total = len(tickers)
    print(f"[Batch Download] Starting download for {total} stocks, batch size {BATCH_SIZE}, workers {MAX_WORKERS}")
    overall_start = time.time()

    for batch_idx, batch_start in enumerate(range(0, total, BATCH_SIZE)):
        batch = tickers[batch_start:batch_start+BATCH_SIZE]
        batch_num = batch_idx + 1
        print(f"[Batch Download] Processing batch {batch_num}: {len(batch)} stocks")
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

        batch_end_time = time.time()
        print(f"[Batch Download] Batch finished: Downloaded {batch_success}, Failed {batch_failed} "
              f"(Time: {batch_end_time - batch_start_time:.2f}s)")
        
        # Add strategic delay between batches to avoid rate limiting
        if batch_idx < (total - 1) // BATCH_SIZE:
            time.sleep(BATCH_DELAY)

    # Retry failed tickers with longer delays
    if failed:
        print(f"[Batch Download] Retrying {len(failed)} failed stocks with exponential backoff...")
        retry_start_time = time.time()
        retry_failed = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
        
        retry_end_time = time.time()
        print(f"[Batch Download] Retry finished: "
              f"Recovered {len(failed) - len(retry_failed)}, Still failed {len(retry_failed)} "
              f"(Time: {retry_end_time - retry_start_time:.2f}s)")
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
    # Check if should run (allows 5-minute window around target time)
    if not check_should_run():
        print("[Scheduler] Skipping run - not in target time window")
        exit(0)
    
    tickers = read_stock_list()
    if not tickers:
        print("No tickers to download.")
    else:
        run_start = datetime.now()
        print(f"[Start] Download started at {run_start.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        stock_data, failed = download_batch_stocks(tickers, period="1y", interval="1d")
        save_path = save_stock_data(stock_data)
        loaded_data = load_stock_data(save_path) if save_path else None
        
        run_end = datetime.now()
        duration = (run_end - run_start).total_seconds()
        print(f"[End] Download completed at {run_end.strftime('%Y-%m-%d %H:%M:%S IST')} (Duration: {duration:.1f}s)")
