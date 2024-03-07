from polars.testing import assert_frame_equal
import polars as pl


def compare_dataframes(left: pl.DataFrame, right: pl.DataFrame):
    assert_frame_equal(left, right)
