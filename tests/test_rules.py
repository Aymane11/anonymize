from anonymize.models.rules import FakeTransform, HashTransform
import pytest
from contextlib import nullcontext as does_not_raise
import polars as pl
from anonymize.models.rules import MaskRightTransform, MaskLeftTransform, DestroyTransform
from .conftest import compare_dataframes


@pytest.mark.parametrize(
    "algorithm,expectation",
    [
        ("md5", does_not_raise()),
        ("invalid_algo", pytest.raises(ValueError, match=r"algorithm must be one of .+")),
    ],
)
def test_hash_transform_algorithm(algorithm, expectation):
    with expectation:
        HashTransform(column="column_name", method="hash", algorithm=algorithm, salt="salt_value")


@pytest.mark.parametrize(
    "fake_method,expectation",
    [
        ("email", does_not_raise()),
        ("number", pytest.raises(ValueError, match=r"faker_type must be one of .+")),
    ],
)
def test_fake_method(fake_method, expectation):
    with expectation:
        FakeTransform(column="column_name", method="fake", faker_type=fake_method)


def test_hash_transform():
    df = pl.DataFrame({"data": ["ABC123", "Hello World!"]})
    actual = HashTransform(column="data", algorithm="sha256", salt="salt").apply(df)

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
    actual = MaskRightTransform(column="name", n_chars=2, mask_char="*").apply(df)

    expected = pl.DataFrame({"name": ["Jo**", "D**", "", "*"]})

    compare_dataframes(actual, expected)


def test_mask_left_transform():
    df = pl.DataFrame({"name": ["Johnny", "Doe", "", "1b", "."]})
    actual = MaskLeftTransform(column="name", n_chars=3, mask_char="*").apply(df)

    expected = pl.DataFrame({"name": ["***nny", "***", "", "**", "*"]})

    compare_dataframes(actual, expected)


def test_fake_transform_firstname():
    df = pl.DataFrame({"name": ["John", "Doe", "Alice"]})
    actual = FakeTransform(column="name", faker_type="firstname").apply(df)

    assert actual["name"].dtype == pl.String


def test_fake_transform_email():
    df = pl.DataFrame({"mail": ["john@example.com", "doe@example.com", "alice@example.com"]})
    actual = FakeTransform(column="mail", faker_type="email").apply(df)

    assert set(actual["mail"].str.contains("@").to_list()) == {True}


def test_destroy_transform():
    df = pl.DataFrame({"name": ["John", "Doe", "Alice"]})
    actual = DestroyTransform(column="name", replace_with="hidden data").apply(df)

    expected = pl.DataFrame({"name": ["hidden data", "hidden data", "hidden data"]})

    compare_dataframes(actual, expected)
