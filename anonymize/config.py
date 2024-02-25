import yaml


def load_config(path: str):
    with open(path, "r") as file:
        return yaml.safe_load(file)


def validate_config(path: str):
    ...
