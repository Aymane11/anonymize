from abc import abstractmethod, ABC
import random
import string
from typing import Callable, ClassVar, Dict, Literal, Union
from pydantic import BaseModel, ConfigDict, Field, validator

import hashlib
from loguru import logger
import polars as pl
from mimesis import Generic

faker = Generic()


class AbstractTransform(ABC):
    @abstractmethod
    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        pass


class HashTransform(BaseModel, AbstractTransform):
    column: str
    method: Literal["hash"] = "hash"
    algorithm: str
    salt: str

    @validator("algorithm")
    def validate_algorithm(cls, v):
        if v not in hashlib.algorithms_available:
            raise ValueError(f"algorithm must be one of {hashlib.algorithms_available}")
        return v

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        col_type = data.schema[self.column]
        if col_type != pl.String:
            logger.warning(
                f"Column {self.column} is of type {col_type}, hashing results will be of type String"
            )
        logger.info(f"Applying {self.algorithm} hash transformation on column {self.column}")
        hash_fun = getattr(hashlib, self.algorithm)
        return data.with_columns(
            pl.col(self.column)
            .cast(pl.String)
            .map_elements(
                lambda x: hash_fun((x + self.salt).encode()).hexdigest(), return_dtype=pl.String
            )
        )


class FakeTransform(BaseModel, AbstractTransform):
    column: str
    method: Literal["fake"] = "fake"
    faker_type: str
    _faker_methods: ClassVar[Dict[str, Callable[[], str]]] = {
        "email": faker.person.email,
        "firstname": faker.person.first_name,
        "lastname": faker.person.last_name,
        "fullname": faker.person.full_name,
    }

    @validator("faker_type")
    def validate_type(cls, v):
        available_types = cls._faker_methods.keys()
        if v not in available_types:
            raise ValueError(f"faker_type must be one of {available_types}")
        return v

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        faker_method = self._faker_methods.get(self.faker_type)
        if not faker_method:
            raise ValueError(f"Unknown faker type {self.faker_type}")

        col_type = data.schema[self.column]
        if col_type != pl.String:
            logger.warning(
                f"Column {self.column} is of type {col_type}, faking results will be of type String"
            )
        logger.info(f"Applying fake {self.faker_type} transformation on column {self.column}")
        return data.with_columns(
            pl.col(self.column).map_elements(lambda _: faker_method(), return_dtype=pl.String)
        )


class MaskRightTransform(BaseModel, AbstractTransform):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    column: str
    method: Literal["mask_right"] = "mask_right"
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        col_type = data.schema[self.column]
        if col_type != pl.String:
            logger.warning(
                f"Column {self.column} is of type {col_type}, right masking results will be of type String"
            )
        logger.info(
            f"Applying mask_right ({self.n_chars}/{self.mask_char}) transformation on column {self.column}"
        )
        return data.with_columns(
            pl.when(pl.col(self.column).cast(pl.String).str.len_chars() > self.n_chars)
            .then(
                pl.concat_str(
                    [
                        pl.col(self.column)
                        .cast(pl.String)
                        .str.slice(
                            0, pl.col(self.column).cast(pl.String).str.len_chars() - self.n_chars
                        ),
                        pl.lit(self.mask_char * self.n_chars),
                    ],
                    ignore_nulls=True,
                ),
            )
            .otherwise(pl.col(self.column).cast(pl.String).str.replace_all(r".", self.mask_char))
            .alias(self.column)
        )


class MaskLeftTransform(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    column: str
    method: Literal["mask_left"] = "mask_left"
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        col_type = data.schema[self.column]
        if col_type != pl.String:
            logger.warning(
                f"Column {self.column} is of type {col_type}, left masking results will be of type String"
            )
        logger.info(
            f"Applying mask_left ({self.n_chars}/{self.mask_char}) transformation on column {self.column}"
        )
        return data.with_columns(
            pl.when(pl.col(self.column).cast(pl.String).str.len_chars() > self.n_chars)
            .then(
                pl.concat_str(
                    [
                        pl.lit(self.mask_char * self.n_chars),
                        pl.col(self.column).cast(pl.String).str.slice(self.n_chars),
                    ],
                    ignore_nulls=True,
                ),
            )
            .otherwise(pl.col(self.column).cast(pl.String).str.replace_all(r".", self.mask_char))
            .alias(self.column)
        )


class DestroyTransform(BaseModel):
    column: str
    method: Literal["destroy"] = "destroy"
    replace_with: str = "CONFIDENTIAL"

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        col_type = data.schema[self.column]
        if col_type != pl.String:
            logger.warning(
                f"Column {self.column} is of type {col_type}, destroy results will be of type String"
            )
        logger.info(
            f"Applying destroy transformation on column {self.column} with replacement {self.replace_with}"
        )
        return data.with_columns(pl.lit(self.replace_with).alias(self.column))


class ShuffleRule(BaseModel):
    column: str
    method: Literal["shuffle"] = "shuffle"

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        logger.info(f"Applying shuffle transformation on column {self.column}")
        shuffled_digits = list(string.digits)
        shuffled_letters = list(string.ascii_letters)
        random.shuffle(shuffled_digits)
        random.shuffle(shuffled_letters)
        return data.with_columns(
            pl.col(self.column)
            .cast(pl.String)
            .str.replace_many(
                list(string.digits + string.ascii_letters), list(shuffled_digits + shuffled_letters)
            )
            .alias(self.column)
        )


Rule = Union[
    HashTransform,
    FakeTransform,
    MaskRightTransform,
    MaskLeftTransform,
    DestroyTransform,
    ShuffleRule,
]
