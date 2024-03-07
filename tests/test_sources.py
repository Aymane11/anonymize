import polars as pl
from anonymize.models.sources import CSVSource, DBSource
from unittest.mock import patch
import pytest

from tests.conftest import compare_dataframes


@patch(
    "polars.scan_csv",
    return_value=pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
)
def test_csv_source_read_data(csv_read_mocker):
    csv_source = CSVSource(path="data.csv", separator="|")
    actual = csv_source.read_data()

    csv_read_mocker.assert_called_once_with("data.csv", separator="|")

    assert csv_source.is_database is False
    assert isinstance(actual, pl.LazyFrame)


@patch(
    "polars.scan_csv",
    return_value=pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
)
def test_csv_source_iteration(_):
    csv_source = CSVSource(path="data.csv", separator="|")

    compare_dataframes(
        next(csv_source), pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]})
    )
    with pytest.raises(StopIteration):
        next(csv_source)


@patch(
    "polars.read_database_uri",
    return_value=pl.DataFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
)
def test_db_source_read_data(db_read_mocker):
    db_source = DBSource(type="db", uri="db://localhost:5432", table="users")
    actual = db_source.read_data()

    db_read_mocker.assert_called_once_with(
        query="SELECT * FROM users LIMIT 10000 OFFSET 0",
        uri="db://localhost:5432",
        engine="connectorx",
    )

    assert db_source.is_database is True
    assert isinstance(actual, pl.LazyFrame)


@patch(
    "polars.read_database_uri",
    side_effect=[
        pl.DataFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
        pl.DataFrame({"id": [4, 5, 6], "name": ["Bob", "Eve", "Charlie"]}),
        pl.DataFrame(),
    ],
)
def test_db_source_iteration(_):
    db_source = DBSource(uri="postgresql://user:pass@localhost:5432", table="users")

    compare_dataframes(
        next(db_source), pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]})
    )
    compare_dataframes(
        next(db_source), pl.LazyFrame({"id": [4, 5, 6], "name": ["Bob", "Eve", "Charlie"]})
    )
    with pytest.raises(StopIteration):
        next(db_source)
