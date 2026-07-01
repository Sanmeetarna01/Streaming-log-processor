import os
import glob
import pandas as pd

parquet_files = glob.glob("streamingoutput/output/**/*.parquet", recursive=True)

if not parquet_files:
    print("No Parquet files found.")
else:
    df = pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True
    )

    print(df)