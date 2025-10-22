# DataStore.py
import os
import pickle
import time
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

# Silence yfinance logs
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# === CONFIG ===
STOCK_LIST_PATH = "Indices/EQUITY_L.csv"
RESULTS_PKL_DIR = "results_pkl"

# Known delisted/suspended NSE stocks
DELISTED = {
    "BINANIIND"
}

BATCH_SIZE = 180        # ← Increased! (2077 / 180 = 12 batches)
MAX_WORKERS = 8         # ← Slightly higher (safe with timeout)
TIMEOUT = 2             # ← Aggressive: kill slow requests fast

# === HELPERS ===
def read_stock_list(stock_list_path=STOCK_LIST_PATH):
    try:
        df = pd.read_csv(stock_list_path)
        raw = df["SYMBOL"].astype(str).tolist()
        tickers = []
        for sym in raw:
            if sym in DELISTED:
                continue
            tickers.append(sym if sym.endswith(".NS") or sym.startswith("^") else f"{sym}.NS")
        print(f"⏭️  Skipped {len(raw) - len(tickers)} delisted stocks.")
        return tickers
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def download_single(ticker):
    try:
        df = yf.download(
            ticker,
            period="1y",
            interval="1d",
            auto_adjust=True,
            rounding=True,
            timeout=TIMEOUT,
            progress=False
        )
        return ticker, df.round(2) if not df.empty else None
    except:
        return ticker, None

def download_all(tickers):
    all_data = {}
    total_failed = 0
    total = len(tickers)
    print(f"🚀 Starting download for {total} stocks (batch={BATCH_SIZE}, workers={MAX_WORKERS})")
    overall_start = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"\n📦 Batch {i//BATCH_SIZE + 1}: {len(batch)} tickers")
        batch_start = time.time()
        success = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_single, t): t for t in batch}
            for future in as_completed(futures):
                ticker, df = future.result()
                if df is not None:
                    all_data[ticker] = df
                    success += 1
                else:
                    failed += 1

        batch_time = time.time() - batch_start
        total_failed += failed
        print(f"✅ Batch done: {success} success, {failed} failed ({batch_time:.1f}s)")

    total_time = time.time() - overall_start
    print(f"\n🏁 Total: {len(all_data)} downloaded, {total_failed} failed in {total_time:.1f}s")
    return all_data

def save_stock_data(stock_data, save_dir=RESULTS_PKL_DIR):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    path = os.path.join(save_dir, f"stock_data_{datetime.now().strftime('%d%m%y')}.pkl")
    clean = {}
    for k, df in stock_data.items():
        key = k[:-3] if k.endswith(".NS") else k
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
        clean[key] = df
    with open(path, "wb") as f:
        pickle.dump(clean, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"💾 Saved to {path}")
    return path

# === MAIN ===
if __name__ == "__main__":
    tickers = read_stock_list()
    if not tickers:
        exit()
    data = download_all(tickers)
    save_stock_data(data)