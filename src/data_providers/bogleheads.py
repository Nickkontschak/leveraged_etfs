"""
This module contains the code to download the historic yield curve data from the bogleheads forum.

Original:
https://www.bogleheads.org/forum/viewtopic.php?p=5488291#p5488291

Latest update:
https://www.bogleheads.org/forum/viewtopic.php?p=5721637#p5721637
"""

import pandas as pd
from io import BytesIO

from .base import DataProvider


class Bogleheads(DataProvider):
    @property
    def name(self) -> str:
        return "bogleheads_monthly_yield_curves"

    @property
    def url(self) -> str:
        return "https://drive.google.com/u/0/uc?id=1azbWYdUDHjjtgxJ-logORbsGOmKanqxJ"

    def _parse_response_content(self, content: bytes) -> pd.DataFrame:
        return pd.read_excel(BytesIO(content), skiprows=3, usecols="Q:AB")

    def _preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data.columns = [
            c[:-2] if c.endswith(".1") else c for c in data.columns]
        data = data.rename({"End of Month": "date"}, axis=1)
        data = data.dropna(how='all', axis=1)
        return data.set_index("date")
