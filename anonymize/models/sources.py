from typing import Literal, Union
from pydantic import BaseModel

from abc import ABC, abstractmethod
from loguru import logger
import polars as pl


class AbstractDataSource(ABC):
    @abstractmethod
    def read_data(self):
        pass


class CSVSource(BaseModel, AbstractDataSource):
    type: Literal["csv"]
    path: str
    separator: str = ","

    @property
    def is_database(self) -> bool:
        return False

    def read_data(self) -> pl.LazyFrame:
        logger.info(f"Reading CSV file {self.path}, using separator {self.separator!r}")
        return pl.scan_csv(self.path, separator=self.separator)


class DBSource(BaseModel, AbstractDataSource):
    type: Literal["db"]
    uri: str
    table: str

    @property
    def is_database(self) -> bool:
        return True

    # TODO: needs to be optimized in case of large tables
    def read_data(self) -> pl.LazyFrame:
        logger.info(f"Reading data from table {self.table} using uri {self.uri}")
        # TODO: make this more robust (use ORM maybe?)
        query = f"SELECT * FROM {self.table}"
        return pl.read_database_uri(query=query, uri=self.uri, engine="connectorx").lazy()


Source = Union[CSVSource, DBSource]
