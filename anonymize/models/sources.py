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
    type: Literal["csv"] = "csv"
    path: str
    separator: str = ","
    _data_read: bool = False

    @property
    def is_database(self) -> bool:
        return False

    def read_data(self) -> pl.LazyFrame:
        if self._data_read:
            raise StopIteration
        self._data_read = True
        logger.info(f"Reading CSV file {self.path}, using separator {self.separator!r}")
        return pl.scan_csv(self.path, separator=self.separator)

    def __iter__(self):
        self._data_read = False
        return self

    def __next__(self):
        return self.read_data()


class DBSource(BaseModel, AbstractDataSource):
    type: Literal["db"] = "db"
    uri: str
    table: str
    _offset: int = 0

    @property
    def is_database(self) -> bool:
        return True

    def read_data(self, limit=10000) -> pl.LazyFrame:
        if self._offset != 0:
            logger.remove()
        logger.info(f"Reading data from table {self.table} using uri {self.uri}")
        # TODO: make this more robust (use ORM maybe?)
        query = f"SELECT * FROM {self.table} LIMIT {limit} OFFSET {self._offset}"
        self._offset += limit  # Update the offset
        return pl.read_database_uri(query=query, uri=self.uri, engine="connectorx").lazy()

    def __iter__(self):
        return self

    def __next__(self):
        data = self.read_data()
        if data.first().collect().is_empty():
            raise StopIteration
        return data


Source = Union[CSVSource, DBSource]
