import yaml
from models.transforms import Transform
from models.sources import Source
from models.outputs import Output
from typing import List
from pydantic import BaseModel, Field, ValidationError


class Rule(BaseModel):
    column: str
    transform: Transform = Field(..., discriminator="method")


class Config(BaseModel):
    source: Source
    output: Output
    rules: List[Rule]


def load_config(path: str) -> Config:
    with open(path, "r") as file:
        config_dict = yaml.safe_load(file)
        return validate_config(config_dict)


def validate_config(config_dict: dict) -> Config:
    try:
        return Config(**config_dict)
    except ValidationError as e:
        raise e
