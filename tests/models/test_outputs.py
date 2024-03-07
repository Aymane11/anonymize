import polars as pl
from anonymize.models.outputs import CSVOutput
from unittest.mock import patch


@patch("polars.LazyFrame.sink_csv")
def test_csv_source_read_data(csv_read_mocker):
    data = pl.LazyFrame({"id": [1, 2, 3], "name": ["John", "Doe", "Alice"]})
    CSVOutput(type="csv", path="data.csv", separator="|").write_data(data)

    csv_read_mocker.assert_called_once_with(path="data.csv", separator="|")
