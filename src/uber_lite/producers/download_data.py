import subprocess
from pathlib import Path

import pandas as pd


def windows_to_wsl_path(win_path: str) -> Path:
    wsl_path_str = (
        subprocess.check_output(["wslpath", win_path]).decode("utf-8").strip()
    )
    return Path(wsl_path_str)


windows_location = r"D:/Downloads/Taxi.csv"
linux_location = windows_to_wsl_path(windows_location)

chunk_size = 10
for chunk in pd.read_csv(linux_location, chunksize=chunk_size):
    # chunk is a DataFrame. To "process" the rows in the chunk:
    for index, row in chunk.iterrows():
        print(row)
