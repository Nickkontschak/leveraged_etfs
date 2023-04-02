import urllib.request
import pandas as pd
import io
from src.constants import DATA_PATH


def get_historical_vix() -> None:
    url = "https://cdn.cboe.com/resources/us/indices/vixarchive.xls"
    with urllib.request.urlopen(url) as response:
        data = response.read()
    df_vix = pd.read_excel(io.BytesIO(data), skiprows=1)
    df_vix.columns = ["date", "open", "high", "low", "close"]
    df_vix.set_index("date").to_parquet(
        DATA_PATH / "raw" / "vix_historical.parquet")


def get_current_vix() -> None:
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    with urllib.request.urlopen(url) as response:
        data = response.read()
    df_vix = pd.read_csv(io.BytesIO(data))
    df_vix.columns = ["date", "open", "high", "low", "close"]
    df_vix["date"] = pd.to_datetime(df_vix["date"])
    df_vix.set_index("date").to_parquet(
        DATA_PATH / "raw" / "vix_current.parquet")


def get_vix(update: bool = False) -> pd.DataFrame:
    if not (DATA_PATH / "raw" / "vix_historical.parquet").exists():
        get_historical_vix()
    if update or not (DATA_PATH / "vix_current.parquet").exists():
        get_current_vix()

    df_vix = pd.concat([
        pd.read_parquet(DATA_PATH / "raw" / "vix_historical.parquet"),
        pd.read_parquet(DATA_PATH / "raw" / "vix_current.parquet")
    ])
    return df_vix
