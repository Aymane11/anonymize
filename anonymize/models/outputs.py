from typing import Any, Literal, Union
from pydantic import BaseModel
from abc import ABC, abstractmethod
import polars as pl

from loguru import logger


class AbstractDataOutput(ABC):
    @abstractmethod
    def write_data(self, data: Any):
        pass


class CSVOutput(BaseModel, AbstractDataOutput):
    type: Literal["csv"]
    path: str
    separator: str = ","

    def write_data(self, data: pl.LazyFrame) -> None:
        logger.info(f"Writing transformed data to {self.path}, using separator {self.separator!r}")
        data.sink_csv(path=self.path, separator=self.separator)


Output = Union[CSVOutput]
