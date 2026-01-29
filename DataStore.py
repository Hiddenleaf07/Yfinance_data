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
MAX_WORKERS = 8  # Keeping 8 as your last run was stable

def read_stock_list():
    try:
        df = pd.read_csv(STOCK_LIST_PATH)
        tickers = df["SYMBOL"].astype(str).tolist()
        return [t.strip() + ".NS" if not (t.endswith(".NS") or t.startswith("^")) else t.strip() for t in tickers]
    except Exception as e:
        return []

def download_batch_worker(batch, batch_idx):
    start_ts = time.time()
    batch_results = {}
    
    try:
        data = yf.download(
            tickers=" ".join(batch),
            period="1y",
            interval="1d",
            group_by='ticker',
            threads=True,
            progress=False,
            timeout=30
        )

        # Vectorized check is faster
        if isinstance(data.columns, pd.MultiIndex):
            for ticker in batch:
                if ticker in data.columns.levels[0]:
                    t_df = data[ticker].dropna(how='all')
                    if not t_df.empty:
                        # Pre-convert to dict inside the process to save main-thread CPU
                        clean_key = ticker[:-3] if ticker.endswith(".NS") else ticker
                        batch_results[clean_key] = t_df.round(2).to_dict("split")
        else:
            # Single ticker case
            if not data.empty:
                ticker = batch[0]
                clean_key = ticker[:-3] if ticker.endswith(".NS") else ticker
                batch_results[clean_key] = data.round(2).to_dict("split")
                
    except Exception:
        pass

    print(f"⏱️ Batch {batch_idx} finished in {time.time() - start_ts:.2f}s")
    return batch_results

def download_all_parallel(tickers):
    all_results = {}
    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    
    print(f"🚀 Launching Parallel Engine...")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_batch_worker, b, i+1) for i, b in enumerate(batches)]
        for future in as_completed(futures):
            all_results.update(future.result())

    return all_results

def save_stock_data(stock_data):
    if not os.path.exists(RESULTS_PKL_DIR):
        os.makedirs(RESULTS_PKL_DIR, exist_ok=True)
    
    path = os.path.join(RESULTS_PKL_DIR, f"stock_data_{datetime.now().strftime('%d%m%y')}.pkl")
    
    # Files are already in 'split' dict format from the workers! 
    # Just dump it.
    with open(path, "wb") as f:
        pickle.dump(stock_data, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"💾 Saved {len(stock_data)} tickers.")

if __name__ == "__main__":
    total_start = time.time()
    
    tickers = read_stock_list()
    if tickers:
        # download_all now returns the fully formatted dict
        final_data = download_all_parallel(tickers)
        if final_data:
            save_stock_data(final_data)
        
    print(f"🏁 TOTAL TIME: {time.time() - total_start:.2f} seconds")