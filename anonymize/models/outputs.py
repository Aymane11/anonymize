from typing import Literal, Union
from pydantic import BaseModel


class CSVOutput(BaseModel):
    type: Literal["csv"]
    path: str
    separator: str


Output = Union[CSVOutput]
