import pandas as pd
import requests
from pathlib import Path
from src.constants import DATA_PATH


class DataProvider:
    @property
    def filename(self) -> Path:
        return DATA_PATH / "raw" / f"{self.name}.parquet"

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def url(self) -> str:
        raise NotImplementedError

    def get_data(self, update: bool = False) -> pd.DataFrame:
        # self.filename = Path(f"{self.name}.parquet")

        if not update and self.filename.exists():
            return pd.read_parquet(self.filename)

        response = self._request_data()
        data = self._handle_response(response)
        data = self._preprocess(data)
        data.to_parquet(self.filename)
        return data

    # In case we a specific API instead of requests (e.g NasdaqDataLink)
    def _request_data(self) -> requests.Response:
        response = requests.get(self.url)
        response.raise_for_status()
        return response

    def _handle_response(self, response: requests.Response) -> pd.DataFrame:
        content = response.content
        return self._parse_response_content(content)

    def _parse_response_content(self, content: bytes) -> pd.DataFrame:
        raise NotImplementedError

    def _preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
