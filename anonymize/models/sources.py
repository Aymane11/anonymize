from typing import Literal, Union
from pydantic import BaseModel


class CSVSource(BaseModel):
    type: Literal["csv"]
    path: str
    separator: str


class PostgresSource(BaseModel):
    type: Literal["postgres"]
    host: str
    port: int
    username: str
    password: str
    database: str
    table: str


Source = Union[CSVSource, PostgresSource]
