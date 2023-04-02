import pandas as pd
from io import BytesIO

from .base import DataProvider


class _VIXHistorical(DataProvider):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return "https://cdn.cboe.com/resources/us/indices/vixarchive.xls"

    def _parse_response_content(self, content: bytes) -> pd.DataFrame:
        return pd.read_excel(BytesIO(content), skiprows=1)

    def _preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data.columns = ["date", "open", "high", "low", "close"]
        return data.set_index("date")


class _VIXCurrent(DataProvider):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"

    def _parse_response_content(self, content: bytes) -> pd.DataFrame:
        return pd.read_csv(BytesIO(content))

    def _preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data.columns = ["date", "open", "high", "low", "close"]
        data["date"] = pd.to_datetime(data["date"])
        return data.set_index("date")


class VIX(DataProvider):
    def __init__(self):
        self.historic_provider = _VIXHistorical("vix_historical")
        self.current_provider = _VIXCurrent("vix_current")


    def get_data(self, update: bool = False) -> pd.DataFrame:
        if not self.historic_provider.filename.exists():
            historic_data = self.historic_provider.get_data()
        else:
            historic_data = pd.read_parquet(self.historic_provider.filename)

        if update or not self.current_provider.filename.exists():
            current_data = self.current_provider.get_data(update=True)
        else:
            current_data = pd.read_parquet(self.current_provider.filename)

        data = pd.concat([historic_data, current_data], ignore_index=True)
        return data
