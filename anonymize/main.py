import hashlib
from typing import Callable
import polars as pl
from config import load_config
from loguru import logger
import sys
from reader import read_csv
from faker import Faker

logger.remove()  # All configured handlers are removed
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
)

config = load_config("config.yml")
if config.source.type == "csv":
    df = read_csv(file=config.source.path, separator=config.source.separator)

faker = Faker()


def hash_transform(df, column, algorithm, salt, **kwargs):
    logger.info(f"Applying {algorithm} hash transformation on column {column}...")
    hash_fun = getattr(hashlib, algorithm)
    return df.with_columns(
        pl.col(column)
        .cast(pl.String)
        .map_elements(lambda x: hash_fun((x + salt).encode()).hexdigest())
    )


def fake_transform(df, column, faker_type, **kwargs):
    logger.info(f"Applying fake {faker_type} transformation on column {column}...")
    if faker_type == "firstname":
        return df.with_columns(
            pl.col(column).cast(pl.String).map_elements(lambda x: faker.first_name())
        )
    elif faker_type == "email":
        return df.with_columns(
            pl.col(column).cast(pl.String).map_elements(lambda x: faker.ascii_safe_email())
        )


def mask_right_transform(df, column, mask_char, n_chars, **kwargs):
    logger.info(f"Applying mask_right ({n_chars}/{mask_char}) transformation on column {column}...")
    return df.with_columns(
        pl.when(pl.col(column).cast(pl.String).str.len_chars() > n_chars)
        .then(
            pl.concat_str(
                [
                    pl.col(column)
                    .cast(pl.String)
                    .str.slice(0, pl.col(column).cast(pl.String).str.len_chars() - n_chars),
                    pl.lit(mask_char * n_chars),
                ],
                ignore_nulls=True,
            ),
        )
        .otherwise(pl.lit(mask_char * n_chars))
        .alias(column)
    )


def mask_left_transform(df, column, mask_char, n_chars, **kwargs):
    logger.info(f"Applying mask_left ({n_chars}/{mask_char}) transformation on column {column}...")
    return df.with_columns(
        pl.when(pl.col(column).cast(pl.String).str.len_chars() > n_chars)
        .then(
            pl.concat_str(
                [
                    pl.lit(mask_char * n_chars),
                    pl.col(column).cast(pl.String).str.slice(n_chars),
                ],
                ignore_nulls=True,
            ),
        )
        .otherwise(pl.lit(mask_char * n_chars))
        .alias(column)
    )


transformations: dict[str, Callable] = {
    "hash": hash_transform,
    "fake": fake_transform,
    "mask_right": mask_right_transform,
    "mask_left": mask_left_transform,
}

# Apply transformations
for rule in config.rules:
    column = rule.column
    method = rule.transform.method
    df = transformations[method](df, column, **rule.transform.model_dump())

if config.output.type == "csv":
    logger.info("Writing transformed data to CSV...")
    df.sink_csv(path=config.output.path, separator=config.output.separator)
    logger.info("Done.")

