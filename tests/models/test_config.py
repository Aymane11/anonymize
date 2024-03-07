from pydantic import ValidationError
import pytest
from anonymize import load_config, Config
from unittest.mock import mock_open, patch


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="source:\n  type: csv\n  path: data.csv\noutput:\n  type: csv\n  path: output.csv\nrules:\n  - column: name\n    method: fake\n    faker_type: firstname",
)
def test_load_config_valid(_):
    path = "valid_config.yaml"
    config = load_config(path)
    assert isinstance(config, Config)


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="source:\n  path: data.csv\noutput:\n  type: csv\n  path: output.csv\nrules:\n  - method: hash\n    columns:\n      - name\n",
)
def test_load_config_invalid(_):
    path = "invalid_config.yaml"
    with pytest.raises(ValidationError):
        load_config(path)
