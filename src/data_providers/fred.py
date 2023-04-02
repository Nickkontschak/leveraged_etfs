
import pandas as pd
from io import BytesIO
from typing import Optional

from .base import DataProvider


class Fred(DataProvider):
    def __init__(self, name: str, column_name: Optional[str] = None):
        self._name = name
        if column_name is None:
            self.column_name = name

    @property
    def name(self) -> str:
        return f"fred_{self._name}"

    @property
    def url(self) -> str:
        return f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={self.name}'

    def _parse_response_content(self, content: bytes) -> pd.DataFrame:
        return pd.read_csv(BytesIO(content))

    def _preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data.columns = ["date", self.column_name]
        # FRED returns "." for missing values
        data[self.column_name] = pd.to_numeric(data[self.column_name], errors="coerce")
        data["date"] = pd.to_datetime(data["date"])
        return data.set_index("date")
