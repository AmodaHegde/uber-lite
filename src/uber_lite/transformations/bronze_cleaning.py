import re

import pandas as pd

# 1. Read the CSV safely without DtypeWarning
df = pd.read_csv("/mnt/d/Downloads/taxi_part__1.csv", low_memory=False)

# 2. Standardize column names to clean snake_case
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .map(lambda col: re.sub(r"[^\w]+", "_", col).strip("_"))
)

print(df.columns.tolist())

# 3. If saving back to CSV:
df.to_csv("/mnt/d/Downloads/taxi_part__1_cleaned.csv", index=False)
