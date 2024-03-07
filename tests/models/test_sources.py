import polars as pl
from anonymize.models.sources import CSVSource, DBSource
from unittest.mock import patch


@patch(
    "polars.scan_csv",
    return_value=pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
)
def test_csv_source_read_data(csv_read_mocker):
    csv_source = CSVSource(type="csv", path="data.csv", separator="|")
    actual = csv_source.read_data()

    csv_read_mocker.assert_called_once_with("data.csv", separator="|")

    assert isinstance(actual, pl.LazyFrame)


@patch(
    "polars.read_database_uri",
    return_value=pl.DataFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]}),
)
def test_db_source_read_data(db_read_mocker):
    db_source = DBSource(type="db", uri="db://localhost:5432", table="users")
    actual = db_source.read_data()

    db_read_mocker.assert_called_once_with(
        query="SELECT * FROM users", uri="db://localhost:5432", engine="connectorx"
    )

    assert isinstance(actual, pl.LazyFrame)
