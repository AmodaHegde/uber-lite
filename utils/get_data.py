#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.cityofnewyork.us", None)

# Example authenticated client (needed for non-public datasets):
client = Socrata(
    "data.cityofnewyork.us",
    "VrIlUBJWAJoFck1mgPFU2v2nQ",
    username="itsahegde@gmail.com",
    password="ucc6WMEOX7FRPK",
)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("u253-aew4", limit=20)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# print(list(results_df))


def save_to_csv(df, filepath):
    """
    Save a pandas DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save
        filepath (str): Path to save the CSV file

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")  # noqa: TRY004

        df.to_csv(filepath, index=False)
        print(f"Successfully saved DataFrame to {filepath}")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"Error saving DataFrame: {e}")
        return False


# Example usage:
save_to_csv(results_df, "output.csv")
