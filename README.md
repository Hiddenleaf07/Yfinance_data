# For Extremly Lazy People


## Optimizing saved pickle files

If you use `DataStore.save_stock_data` it writes a pickle into `results_pkl/` (for example
`results_pkl/stock_data_261025.pkl`). Use the included `optimize_pickle.py` to convert the
stored dicts back into pandas DataFrames and produce a ready-to-use optimized pickle.

Example:

	python optimize_pickle.py results_pkl/stock_data_261025.pkl

This will write `results_pkl/stock_data_261025_optimized.pkl` next to the original. Add
`--compress` to also create a gzipped `.pkl.gz` file suitable for download.
