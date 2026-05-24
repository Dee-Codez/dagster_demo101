from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class SourceConnector(ABC):
    @abstractmethod
    def extract(self, query_name: str) -> pd.DataFrame: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> SourceConnector:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
