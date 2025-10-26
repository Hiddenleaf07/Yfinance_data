import os
import pickle
import pandas as pd
from datetime import datetime

def view_pkl_file(file_path):
    """
    View and validate the contents of a pickle file
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
            print(f"\n📈 Contains data for {len(data)} symbols")
            print("\nSample Analysis:")
            print("-" * 30)
            
            for symbol, df in list(data.items())[:3]:  # Show first 3 symbols
                print(f"\nSymbol: {symbol}")
                if isinstance(df, pd.DataFrame):
                    print(f"Type: DataFrame")
                    print(f"Shape: {df.shape}")
                    print(f"Date Range: {df.index.min()} to {df.index.max()}")
                    print(f"Columns: {list(df.columns)}")
                    print(f"Memory Usage: {df.memory_usage().sum() / 1024:.2f} KB")
                    if df.index.tz:
                        print(f"Timezone: {df.index.tz}")
                    else:
                        print("Warning: No timezone set")
                else:
                    print(f"Type: {type(df)}")
            
            return True
        else:
            print(f"❌ Unexpected data type: {type(data)}")
            return False
            
    except Exception as e:
        print(f"❌ Error reading pickle file: {str(e)}")
        return False

if __name__ == "__main__":
    # Test the most recent pickle file in results_pkl directory
    pkl_dir = "results_pkl"
    if os.path.exists(pkl_dir):
        pkl_files = [f for f in os.listdir(pkl_dir) if f.endswith('.pkl')]
        if pkl_files:
            latest_pkl = max(pkl_files, key=lambda x: os.path.getctime(os.path.join(pkl_dir, x)))
            view_pkl_file(os.path.join(pkl_dir, latest_pkl))
        else:
            print("No pickle files found in results_pkl directory")
    else:
        print(f"Directory {pkl_dir} not found")