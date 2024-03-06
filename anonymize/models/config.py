import yaml
from typing import List
from pydantic import BaseModel, Field, ValidationError

from .sources import Source
from .outputs import Output
from .rules import Rule


class Config(BaseModel):
    source: Source
    output: Output
    rules: List[Rule] = Field(..., discriminator="method")


def load_config(path: str) -> Config:
    with open(path, "r") as file:
        config_dict = yaml.safe_load(file)
        return validate_config(config_dict)


def validate_config(config_dict: dict) -> Config:
    try:
        return Config(**config_dict)
    except ValidationError as e:
        raise e
