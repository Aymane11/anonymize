import polars as pl
from anonymize.transformations import (
    fake_transform,
    hash_transform,
    mask_right_transform,
    mask_left_transform,
)
from .conftest import compare_dataframes


def test_hash_transform():
    df = pl.DataFrame({"data": ["ABC123", "Hello World!"]})
    actual = hash_transform(df, "data", "sha256", "salt")

    expected = pl.DataFrame(
        {
            "data": [
                "f1e453052b58b0d982cc2c3222983f901ff9814dec4ecae666f795570f17f07a",
                "f13d75a86ecce97c9aef2bd350a7e81b4c0c914861bf914b1429c7db141666d5",
            ]
        }
    )

    compare_dataframes(actual, expected)


def test_mask_right_transform():
    df = pl.DataFrame({"name": ["John", "Doe", "", "A"]})
    actual = mask_right_transform(df, "name", "*", 2)

    expected = pl.DataFrame({"name": ["Jo**", "D**", "", "*"]})

    compare_dataframes(actual, expected)


def test_mask_left_transform():
    df = pl.DataFrame({"name": ["Johnny", "Doe", "", "1b", "."]})
    actual = mask_left_transform(df, "name", "*", 3)

    expected = pl.DataFrame({"name": ["***nny", "***", "", "**", "*"]})

    compare_dataframes(actual, expected)


def test_fake_transform_firstname():
    df = pl.DataFrame({"name": ["John", "Doe", "Alice"]})
    actual = fake_transform(df, "name", "firstname")

    assert actual["name"].dtype == pl.String


def test_fake_transform_email():
    df = pl.DataFrame({"email": ["john@example.com", "doe@example.com", "alice@example.com"]})
    actual = fake_transform(df, "email", "email")

    assert set(actual["email"].str.contains("@").to_list()) == {True}
