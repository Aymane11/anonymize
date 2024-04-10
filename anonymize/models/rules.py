from abc import abstractmethod, ABC
from typing import Literal, Union
from pydantic import BaseModel, Field, validator

import hashlib
from loguru import logger
import polars as pl
from faker import Faker

faker = Faker()


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
        import hashlib

        if v not in hashlib.algorithms_available:
            raise ValueError(f"algorithm must be one of {hashlib.algorithms_available}")
        return v

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
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

    @validator("faker_type")
    def validate_type(cls, v):
        AVAILABLE_TYPES = {"email", "firstname"}
        if v not in AVAILABLE_TYPES:
            raise ValueError(f"faker_type must be one of {AVAILABLE_TYPES}")
        return v

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        logger.info(f"Applying fake {self.faker_type} transformation on column {self.column}")
        if self.faker_type == "email":
            faker_method = faker.email
        elif self.faker_type == "firstname":
            faker_method = faker.first_name
        return data.with_columns(
            pl.col(self.column)
            .cast(pl.String)
            .map_elements(lambda x: faker_method(), return_dtype=pl.String)
        )


class MaskRightTransform(BaseModel, AbstractTransform):
    column: str
    method: Literal["mask_right"] = "mask_right"
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
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

    class Config:
        coerce_numbers_to_str = True


class MaskLeftTransform(BaseModel):
    column: str
    method: Literal["mask_left"] = "mask_left"
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
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

    class Config:
        coerce_numbers_to_str = True


class DestroyTransform(BaseModel):
    column: str
    method: Literal["destroy"] = "destroy"
    replace_with: str = "CONFIDENTIAL"

    def apply(self, data: pl.LazyFrame) -> pl.LazyFrame:
        logger.info(
            f"Applying destroy transformation on column {self.column} with replacement {self.replace_with}"
        )
        return data.with_columns(pl.lit(self.replace_with).alias(self.column))


Rule = Union[HashTransform, FakeTransform, MaskRightTransform, MaskLeftTransform, DestroyTransform]
