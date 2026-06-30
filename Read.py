import pandas as pd
import glob

files = glob.glob("output/date=*/part-*.parquet")

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

print(df)