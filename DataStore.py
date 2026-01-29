import os
import pickle
import time
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURATION ---
STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
BATCH_SIZE = 300         
MAX_WORKERS = 4  # Reduced slightly to prevent CPU thrashing in GitHub Actions

def read_stock_list():
    try:
        df = pd.read_csv(STOCK_LIST_PATH)
        tickers = df["SYMBOL"].astype(str).tolist()
        return [t.strip() + ".NS" if not (t.endswith(".NS") or t.startswith("^")) else t.strip() for t in tickers]
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

def download_batch_worker(batch, batch_idx):
    """
    Independent worker process. 
    Processes don't share memory, so 'dictionary changed size' errors are impossible.
    """
    start_ts = time.time()
    batch_data = {}
    batch_failed = []
    
    try:
        # We download in one hit per process
        data = yf.download(
            tickers=" ".join(batch),
            period="1y",
            interval="1d",
            group_by='ticker',
            threads=True, # Internal yfinance threading is okay here
            progress=False,
            timeout=45
        )

        for ticker in batch:
            try:
                # Handle MultiIndex
                if ticker in data.columns.levels[0]:
                    t_df = data[ticker].dropna(how='all')
                    if not t_df.empty:
                        # Convert to dict early to make serialization between processes faster
                        batch_data[ticker] = t_df.round(2)
                else:
                    batch_failed.append(ticker)
            except:
                batch_failed.append(ticker)
    except Exception as e:
        print(f"❌ Process {batch_idx} Error: {e}")
        batch_failed.extend(batch)

    print(f"⏱️ Batch {batch_idx} finished in {time.time() - start_ts:.2f}s")
    return batch_data, batch_failed

def download_all_parallel(tickers):
    all_results = {}
    all_failed = []
    
    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    print(f"🚀 Launching {len(batches)} Parallel Processes...")

    # ProcessPoolExecutor is the key for thread-safety
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(download_batch_worker, batches[i], i + 1): i 
            for i in range(len(batches))
        }
        
        for future in as_completed(future_to_batch):
            batch_data, batch_failed = future.result()
            all_results.update(batch_data)
            all_failed.extend(batch_failed)

    return all_results, all_failed

def save_stock_data(stock_data):
    if not os.path.exists(RESULTS_PKL_DIR):
        os.makedirs(RESULTS_PKL_DIR, exist_ok=True)
    
    path = os.path.join(RESULTS_PKL_DIR, f"stock_data_{datetime.now().strftime('%d%m%y')}.pkl")
    
    converted = {}
    for k, v in stock_data.items():
        clean_key = k.replace(".NS", "")
        df = v.copy()
        if not isinstance(df.index.dtype, pd.DatetimeTZDtype):
            df.index = pd.to_datetime(df.index).tz_localize("Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward")
        converted[clean_key] = df.to_dict("split")

    with open(path, "wb") as f:
        pickle.dump(converted, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"💾 Saved {len(converted)} tickers to {path}")

if __name__ == "__main__":
    # Multi-processing requires the main guard
    total_start = time.time()
    
    tickers = read_stock_list()
    if tickers:
        final_data, failed = download_all_parallel(tickers)
        if final_data:
            save_stock_data(final_data)
        
        print(f"✅ Success: {len(final_data)} | ❌ Failed: {len(failed)}")
    
    print(f"🏁 TOTAL TIME: {time.time() - total_start:.2f} seconds")