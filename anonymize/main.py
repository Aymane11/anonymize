import hashlib
import polars as pl
from config import load_config, validate_config

print("Validating config...")
validate_config("config.yaml")
print("Loading config...")
config = load_config("config.yml")
if config["source"]["type"] == "csv":
    print("Reading CSV file...")
    from reader import read_csv

    df = read_csv(file=config["source"]["path"], separator=config["source"]["separator"])
    print(df.dtypes)

# Apply transformations
for rule in config["rules"]:
    column = rule["column"]
    method = rule["transform"]["method"]

    if method == "hash":
        import hashlib

        algorithm = rule["transform"]["algorithm"]
        salt = rule["transform"]["salt"]
        print(f"Applying {algorithm} hash transformation on column {column}...")
        hash_fun = getattr(hashlib, algorithm)
        df = df.with_columns(
            pl.col(column)
            .cast(pl.String)
            .map_elements(lambda x: hash_fun((x + salt).encode()).hexdigest())
        )
    elif method == "fake":
        from faker import Faker

        faker_type = rule["transform"]["type"]
        print(f"Applying fake {faker_type} transformation on column {column}...")
        if faker_type == "firstname":
            df = df.with_columns(
                pl.col(column).cast(pl.String).map_elements(lambda x: Faker().first_name())
            )
        elif faker_type == "email":
            df = df.with_columns(
                pl.col(column).cast(pl.String).map_elements(lambda x: Faker().ascii_safe_email())
            )
    elif method == "mask_right":
        mask_char = rule["transform"]["mask_char"]
        n_chars = rule["transform"]["n_chars"]
        print(f"Applying mask_right ({n_chars}/{mask_char}) transformation on column {column}...")
        # mask the last n characters
        df = df.with_columns(
            pl.when(pl.col(column).cast(pl.String).str.len_chars() > n_chars)
            .then(
                pl.concat_str(
                    [
                        pl.col(column)
                        .cast(pl.String)
                        .str.slice(0, pl.col(column).cast(pl.String).str.len_chars() - n_chars),
                        pl.lit(mask_char * n_chars),
                    ]
                ),
            )
            .otherwise(pl.lit(mask_char * n_chars))
            .alias(column)
        )
    elif method == "mask_left":
        mask_char = rule["transform"]["mask_char"]
        n_chars = rule["transform"]["n_chars"]
        print(f"Applying mask_left ({n_chars}/{mask_char}) transformation on column {column}...")
        # mask the last n characters
        df = df.with_columns(
            pl.when(pl.col(column).cast(pl.String).str.len_chars() > n_chars)
            .then(
                pl.concat_str(
                    [
                        pl.lit(mask_char * n_chars),
                        pl.col(column).cast(pl.String).str.slice(n_chars),
                    ]
                ),
            )
            .otherwise(pl.lit(mask_char * n_chars))
            .alias(column)
        )


print("Writing transformed data to CSV...")
df.sink_csv(path=config["output"]["path"], separator=config["output"]["separator"])
print("Done.")
