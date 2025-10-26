#!/usr/bin/env python3
"""
optimize_pickle.py

Load an existing pickle created by `DataStore.save_stock_data`, convert any stored dicts
back into pandas.DataFrame objects (with consistent columns and timezone), and write an
optimized pickle next to the original file with the suffix `_optimized.pkl`.

Optionally create a gzipped copy (`.pkl.gz`) for easy downloading.

Usage:
    python optimize_pickle.py /path/to/results_pkl/stock_data_261025.pkl

Options:
    --output PATH    Explicit optimized output path. If omitted, uses original dir + suffix.
    --compress       Also write a gzipped copy (.pkl.gz) suitable for download.
    --force          Recreate the optimized file even if it already exists.

"""
import argparse
import gzip
import os
import pickle
import time
from typing import Dict, Any

import pandas as pd


EXPECTED_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
DEFAULT_TZ = "Asia/Kolkata"


def convert_value_to_df(v: Any) -> Any:
    """Convert stored value v into a pandas DataFrame when appropriate.
    If conversion isn't possible, return the original value.
    """
    # If it's already a DataFrame, normalize it
    if isinstance(v, pd.DataFrame):
        df = v.copy()
    elif isinstance(v, dict):
        # Typical saved format from DataFrame.to_dict('split')
        if set(v.keys()) >= {"index", "columns", "data"}:
            columns = [col if isinstance(col, str) else (col[0] if isinstance(col, (list, tuple)) else str(col))
                       for col in v["columns"]]
            try:
                df = pd.DataFrame(v["data"], index=v["index"], columns=columns)
            except Exception:
                # Fallback: let pandas try a generic construction
                df = pd.DataFrame(v)
        else:
            try:
                df = pd.DataFrame(v)
            except Exception:
                return v
    else:
        return v

    # Ensure expected columns and order (missing columns stay as NaN)
    df = df.reindex(columns=EXPECTED_COLUMNS)

    # Ensure datetime index and consistent timezone
    try:
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)
        if df.index.tz is None:
            df.index = df.index.tz_localize(DEFAULT_TZ, ambiguous="NaT", nonexistent="shift_forward")
    except Exception:
        # If index conversion fails, leave as-is but continue
        pass

    # Round numeric columns to 2 decimal places for storage size and consistency
    try:
        df = df.round(2)
    except Exception:
        pass

    return df


def optimize_pickle(original_path: str, optimized_path: str, compress: bool = False, force: bool = False) -> bool:
    """Create an optimized pickle from original_path and save to optimized_path.
    If compress is True, also write a gzip file with .pkl.gz suffix.
    Returns True on success.
    """
    if not os.path.exists(original_path):
        print(f"❌ Original pickle not found: {original_path}")
        return False

    if os.path.exists(optimized_path) and not force:
        print(f"📦 Optimized file already exists: {optimized_path} (use --force to recreate)")
        return True

    start = time.time()
    print(f"📥 Loading original pickle: {original_path}")
    try:
        with open(original_path, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        print(f"❌ Failed to load original pickle: {e}")
        return False

    print("🔧 Converting stored dicts to DataFrames where possible...")
    converted = {}
    for k, v in data.items():
        try:
            converted[k] = convert_value_to_df(v)
        except Exception as e:
            print(f"⚠️  Conversion failed for {k}: {e}")
            converted[k] = v

    print(f"💾 Saving optimized pickle to: {optimized_path}")
    try:
        os.makedirs(os.path.dirname(optimized_path) or ".", exist_ok=True)
        with open(optimized_path, "wb") as f:
            pickle.dump(converted, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"❌ Failed to save optimized pickle: {e}")
        return False

    if compress:
        gz_path = optimized_path + ".gz" if not optimized_path.endswith(".gz") else optimized_path
        if not gz_path.endswith(".pkl.gz"):
            gz_path = gz_path.replace(".pkl", ".pkl.gz") if gz_path.endswith(".pkl") else gz_path + ".pkl.gz"
        print(f"🗜️  Writing gzipped download file: {gz_path}")
        try:
            with gzip.open(gz_path, "wb") as gz:
                pickle.dump(converted, gz, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            print(f"⚠️  Failed to write gzipped file: {e}")

    elapsed = time.time() - start
    print(f"✅ Optimized {len(converted)} items in {elapsed:.2f}s")
    return True


def main():
    p = argparse.ArgumentParser(description="Optimize a stock-data pickle for faster loading in apps")
    p.add_argument("original", help="Path to the original pickle file")
    p.add_argument("--output", "-o", help="Path for optimized pickle. Defaults to ORIGINAL_OPTIMIZED in same dir")
    p.add_argument("--compress", "-z", action="store_true", help="Also create a gzipped .pkl.gz copy for download")
    p.add_argument("--force", "-f", action="store_true", help="Recreate optimized file even if it exists")
    args = p.parse_args()

    original = args.original
    if args.output:
        optimized = args.output
    else:
        base, ext = os.path.splitext(os.path.basename(original))
        optimized = os.path.join(os.path.dirname(original) or ".", f"{base}_optimized{ext}")

    success = optimize_pickle(original, optimized, compress=args.compress, force=args.force)
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
