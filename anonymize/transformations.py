import polars as pl
import hashlib
from faker import Faker
from loguru import logger


faker = Faker()


def hash_transform(df, column, algorithm, salt, **kwargs):
    logger.info(f"Applying {algorithm} hash transformation on column {column}")
    hash_fun = getattr(hashlib, algorithm)
    return df.with_columns(
        pl.col(column)
        .cast(pl.String)
        .map_elements(lambda x: hash_fun((x + salt).encode()).hexdigest(), return_dtype=pl.String)
    )


def fake_transform(df, column, faker_type, **kwargs):
    logger.info(f"Applying fake {faker_type} transformation on column {column}")
    if faker_type == "firstname":
        return df.with_columns(
            pl.col(column)
            .cast(pl.String)
            .map_elements(lambda x: faker.first_name(), return_dtype=pl.String)
        )
    elif faker_type == "email":
        return df.with_columns(
            pl.col(column)
            .cast(pl.String)
            .map_elements(lambda x: faker.ascii_safe_email(), return_dtype=pl.String)
        )


def mask_right_transform(df, column, mask_char, n_chars, **kwargs):
    logger.info(f"Applying mask_right ({n_chars}/{mask_char}) transformation on column {column}")
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
        .otherwise(pl.col(column).cast(pl.String).str.replace_all(r".", mask_char))
        .alias(column)
    )


def mask_left_transform(df, column, mask_char, n_chars, **kwargs):
    logger.info(f"Applying mask_left ({n_chars}/{mask_char}) transformation on column {column}")
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
        .otherwise(pl.col(column).cast(pl.String).str.replace_all(r".", mask_char))
        .alias(column)
    )
