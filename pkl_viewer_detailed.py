import os
import pickle
import pandas as pd
from datetime import datetime

def view_pkl_file_detailed(file_path):
    """
    View and validate the contents of a pickle file with detailed analysis
    """
    print(f"\n📂 Analyzing pickle file: {file_path}")
    print("=" * 50)
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False
            
        # Check file size
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        print(f"📊 File size: {file_size:.2f} MB")
        
        # Load the pickle file
        print("\n🔍 Loading pickle file...")
        start_time = datetime.now()
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        load_time = (datetime.now() - start_time).total_seconds()
        print(f"⏱️  Load time: {load_time:.2f} seconds")
        
        # Analyze content
        if isinstance(data, dict):
            total_symbols = len(data)
            print(f"\n📈 Contains data for {total_symbols} symbols")
            
            # Calculate overall statistics
            total_rows = 0
            total_memory = 0
            date_ranges = []
            
            print("\n🔍 Detailed Analysis:")
            print("=" * 50)
            
            # Sample size for detailed analysis
            sample_size = min(5, len(data))
            print(f"\nDetailed analysis of first {sample_size} symbols:")
            
            for symbol, df in list(data.items())[:sample_size]:
                print(f"\n📊 Symbol: {symbol}")
                print("-" * 30)
                if isinstance(df, pd.DataFrame):
                    total_rows += len(df)
                    mem_usage = df.memory_usage().sum() / 1024  # KB
                    total_memory += mem_usage
                    
                    print(f"Type: DataFrame")
                    print(f"Shape: {df.shape}")
                    print(f"Date Range: {df.index.min()} to {df.index.max()}")
                    print(f"Columns: {list(df.columns)}")
                    print(f"Memory Usage: {mem_usage:.2f} KB")
                    
                    # Data types
                    print("\nColumn Data Types:")
                    for col, dtype in df.dtypes.items():
                        print(f"  - {col}: {dtype}")
                    
                    # Sample data
                    print("\nFirst 3 rows:")
                    print(df.head(3))
                    
                    if df.index.tz:
                        print(f"\nTimezone: {df.index.tz}")
                    else:
                        print("\nWarning: No timezone set")
                        
                    # Basic statistics
                    print("\nBasic Statistics:")
                    print(df.describe().round(2))
                else:
                    print(f"Type: {type(df)}")
            
            # Overall statistics
            print("\n📊 Overall Statistics:")
            print("=" * 50)
            print(f"Total Symbols: {total_symbols}")
            print(f"Average Rows per Symbol: {total_rows/total_symbols:.2f}")
            print(f"Total Memory Usage: {total_memory/1024:.2f} MB")
            
            return True
        else:
            print(f"❌ Unexpected data type: {type(data)}")
            return False
            
    except Exception as e:
        print(f"❌ Error reading pickle file: {str(e)}")
        return False

if __name__ == "__main__":
    # Use the specific pickle file path
    pkl_path = "/workspaces/Yfinance_data/results_pkl/stock_data_261025.pkl"
    view_pkl_file_detailed(pkl_path)