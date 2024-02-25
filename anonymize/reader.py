import polars as pl


def read_csv(file, separator=",") -> pl.LazyFrame:
    return pl.scan_csv(file, separator=separator)
