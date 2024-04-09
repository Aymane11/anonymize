# anonymize

Data anonymizer from CSV/Database to CSV file. (more sources/outputs to come)

## Setup

### Requirements
- Python 3.9
- poetry (optional)

### Installation
- Clone the repository
- Install dependencies
```sh
poetry install
or
pip install -r requirements.txt
```
- Create a `config.yml` file with the configuration for source, output and rules to apply. (see Config) ([example config](config.yml))
- Run: `python -m anonymize -c config.yml`


## Config

### Sources (see [`sources.py`](anonymize/models/sources.py))

#### Database
List of [supported databases](https://github.com/sfu-db/connector-x#sources).
```yaml
source:
  type: db
  uri: postgres://postgres:pass@localhost:5432/postgres
  table: mydata
```

#### CSV
```yaml
source:
  type: csv
  path: /path/to/data.csv
  separator: '|' # Optional, default is ','
```
---
### Outputs (see [`outputs.py`](anonymize/models/outputs.py))

#### CSV
```yaml
output:
  type: csv
  path: /path/to/output.csv
  separator: '|' # Optional, default is ','
```
---
### Rules (see [`rules.py`](anonymize/models/rules.py))

- The rules will validate the column name and the method, and then apply the method to the column
- If the column is not found in the source, it will be ignored.
- The if the column is not found in rules list, it will be kept as is.

#### Hash
Available algorithms are the ones in [`hashlib`](https://docs.python.org/3/library/hashlib.html) module.
```yaml
rules:
  - column: credit_card
    method: hash
    algorithm: md5
    salt: my_very_secret_salt
```

#### Fake
```yaml
rules:
  - column: name
    method: fake
    faker_type: firstname # or email
```

#### Mask right (last n characters)
```yaml
rules:
  - column: email
    method: mask_right
    n_chars: 5
    mask_char: x
```

#### Mask left (first n characters)
```yaml
rules:
  - column: birthdate
    method: mask_left
    n_chars: 4
    mask_char: "*"
```

## Contributing

- :fork_and_knife: Fork the repository
- :arrow_down: Install dev dependencies: `poetry install --with=dev` or `pip install -r requirements-dev.txt`
- :deciduous_tree: Create a branch `git checkout -b feature/my-feature`
- :wrench: Make your changes
- :white_check_mark: Run formatting, linting and tests `poe all` (see [`pyproject.toml`](pyproject.toml))
- :arrows_clockwise: Create a pull request

## Next steps/Improvements
- [ ] Add database output
- [ ] Validation (especially for database sources)
- [ ] More rules (rounding, etc.)
