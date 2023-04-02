"""
This module contains the code to download the historic yield curve data from the bogleheads forum.

Original:
https://www.bogleheads.org/forum/viewtopic.php?p=5488291#p5488291

Latest update:
https://www.bogleheads.org/forum/viewtopic.php?p=5721637#p5721637
"""

import urllib.request
import pandas as pd
import io
from src.constants import DATA_PATH


def get_monthly_yield_curves() -> pd.DataFrame:
    data_name = "bogleheads_monthly_yield_curves.parquet"

    if (DATA_PATH / "raw" / data_name).exists():
        return pd.read_parquet(DATA_PATH / "raw" / data_name)

    url = "https://drive.google.com/u/0/uc?id=1azbWYdUDHjjtgxJ-logORbsGOmKanqxJ"
    with urllib.request.urlopen(url) as response:
        data = response.read()

    df = pd.read_excel(
        io.BytesIO(data), 
        skiprows=3, usecols="Q:AB"
    )
    
    df.columns = [c[:-2] if c.endswith(".1") else c for c in df.columns]
    df = df.rename({"End of Month": "date"}, axis=1)
    # Drop empty columns
    df = df.drop(df.columns[df.isna().sum() == df.shape[0]], axis=1)
    df.set_index("date").to_parquet(DATA_PATH / "raw" / data_name)
    return pd.read_parquet(DATA_PATH / "raw" / data_name)
