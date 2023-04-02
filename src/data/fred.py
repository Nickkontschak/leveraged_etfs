import requests
import pandas as pd
import io
from typing import Optional
from src.constants import DATA_PATH


def get_from_fred(name: str, update: bool = False, column_name: Optional[str] = None) -> pd.Series:
    if column_name is None:
        column_name = name

    data_name = f"freq_{name}.parquet"
    if (DATA_PATH / "raw" / data_name).exists() and not update:
        return pd.read_parquet(DATA_PATH / "raw" / data_name)[column_name]

    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={name}'
    data = requests.get(url).content

    df = pd.read_csv(
        io.BytesIO(data)
    )
    df.columns = ["date", column_name]
    df.set_index("date").to_parquet(DATA_PATH / "raw" / data_name)
    return pd.read_parquet(DATA_PATH / "raw" / data_name)[column_name]
