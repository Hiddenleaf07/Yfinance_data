import os
import pickle
import pandas as pd
import time

def load_and_optimize(original_path, optimized_path):
    """
    Load stock data. If an optimized version exists, load it.
    Otherwise, create the optimized version from the original file.
    """
    if os.path.exists(optimized_path):
        print("📦 Loading optimized stock data...")
        start = time.time()
        try:
            with open(optimized_path, "rb") as f:
                data = pickle.load(f)
            elapsed = time.time() - start
            print(f"✅ Loaded {len(data)} tickers in {elapsed:.2f} seconds.")
            return data
        except Exception as e:
            print(f"⚠️ Error loading optimized file: {e}. Falling back to original.")

    if not os.path.exists(original_path):
        print(f"❌ Original pickle file {original_path} not found.")
        return {}

    print("📦 Loading original stock data...")
    start = time.time()
    try:
        with open(original_path, "rb") as f:
            data = pickle.load(f)

        print("⚙️ Converting dicts to DataFrames...")
        for k, v in data.items():
            if isinstance(v, dict):
                try:
                    # Handle dicts shaped like DataFrame.to_dict('split')
                    if set(v.keys()) >= {"index", "columns", "data"}:
                        # Ensure columns are simple strings
                        columns = [col if isinstance(col, str) else col[0] for col in v["columns"]]
                        df = pd.DataFrame(v["data"], index=v["index"], columns=columns)
                    else:
                        df = pd.DataFrame(**v)
                    df = df.sort_index(ascending=True)
                    data[k] = df
                except Exception as e:
                    print(f"❌ Failed to convert {k}: {e}")

        print(f"💾 Saving optimized data to {optimized_path}...")
        with open(optimized_path, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

        elapsed = time.time() - start
        print(f"✅ Optimized {len(data)} tickers in {elapsed:.2f} seconds.")
        return data
    except Exception as e:
        print(f"❌ Error loading original pickle: {e}")
        return {}

if __name__ == "__main__":
    original_pickle = "/workspaces/Yfinance_data/results_pkl/stock_data_261025.pkl"
    optimized_pickle = "stock_data_optimized.pkl"
    load_and_optimize(original_pickle, optimized_pickle)