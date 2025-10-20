# fast_downloader.py
import os
import pickle
import time
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import signal

STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"
BATCH_SIZE = 100          # Reduced from 100 → 60
MAX_WORKERS = 12         # Use more threads (Yahoo allows short bursts)
TIMEOUT_PER_TICKER = 8   # Critical: kill slow requests
MAX_RETRIES = 1

def read_stock_list(stock_list_path=STOCK_LIST_PATH):
    try:
        df = pd.read_csv(stock_list_path)
        tickers = df["SYMBOL"].astype(str).tolist()
        tickers = [t if t.startswith("^") or t.endswith(".NS") else f"{t}.NS" for t in tickers]
        return tickers
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

def download_single_with_timeout(ticker, period="1y", interval="1d", timeout=TIMEOUT_PER_TICKER):
    """Wrap yfinance call with timeout using threading (no signal on Windows)"""
    def _download():
        try:
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(
                period=period,
                interval=interval,
                auto_adjust=True,
                rounding=True,
                timeout=timeout - 2  # inner timeout
            )
            if not hist.empty:
                return hist.round(2)
        except Exception as e:
            pass  # suppress per-ticker errors
        return None

    from concurrent.futures import ThreadPoolExecutor as Pool
    with Pool(max_workers=1) as executor:
        future = executor.submit(_download)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            return None

def download_batch(tickers, period="1y", interval="1d"):
    all_data = {}
    failed = []
    total = len(tickers)
    
    print(f"📥 Downloading {total} tickers in batches of {BATCH_SIZE} (workers: {MAX_WORKERS})...")
    overall_start = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"\n📦 Batch {i//BATCH_SIZE + 1}: {len(batch)} tickers")
        batch_start = time.time()
        batch_data = {}
        batch_failed = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all
            futures = {
                executor.submit(download_single_with_timeout, t, period, interval): t
                for t in batch
            }
            # Collect results with timeout per future (already handled inside)
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        batch_data[ticker] = df
                    else:
                        batch_failed.append(ticker)
                except Exception:
                    batch_failed.append(ticker)

        all_data.update(batch_data)
        failed.extend(batch_failed)
        batch_time = time.time() - batch_start
        print(f"✅ Batch done: {len(batch_data)} success, {len(batch_failed)} failed ({batch_time:.1f}s)")

    # Optional: retry failed (only if few)
    if failed and len(failed) < 50:
        print(f"\n🔁 Quick retry for {len(failed)} failed...")
        retry_success = {}
        with ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(download_single_with_timeout, t, period, interval): t for t in failed}
            for fut in as_completed(futs):
                t = futs[fut]
                try:
                    df = fut.result()
                    if df is not None:
                        retry_success[t] = df
                except:
                    pass
        all_data.update(retry_success)
        failed = [t for t in failed if t not in retry_success]

    total_time = time.time() - overall_start
    print(f"\n🏁 Total: {len(all_data)} downloaded, {len(failed)} failed in {total_time:.1f}s")
    return all_data, failed

def save_stock_data(stock_data, save_dir=RESULTS_PKL_DIR):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    date_str = datetime.now().strftime("%d%m%y")
    path = os.path.join(save_dir, f"stock_data_{date_str}.pkl")
    try:
        clean_data = {}
        for k, df in stock_data.items():
            key = k[:-3] if k.endswith(".NS") else k
            # Ensure datetime index & timezone
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
            clean_data[key] = df
        with open(path, "wb") as f:
            pickle.dump(clean_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"💾 Saved to {path}")
        return path
    except Exception as e:
        print(f"❌ Save failed: {e}")
        return None

if __name__ == "__main__":
    tickers = read_stock_list()
    if not tickers:
        print("No tickers found.")
    else:
        print(f"Total tickers: {len(tickers)}")
        data, failed = download_batch(tickers, period="1y", interval="1d")
        save_stock_data(data)