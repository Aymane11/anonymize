from typing import Callable
from anonymize import load_config, Config

from loguru import logger
from anonymize.transformations import (
    hash_transform,
    fake_transform,
    mask_right_transform,
    mask_left_transform,
)
import argparse


def main(config: Config):
    data = config.source.read_data()

    rule_mapping: dict[str, Callable] = {
        "hash": hash_transform,
        "fake": fake_transform,
        "mask_right": mask_right_transform,
        "mask_left": mask_left_transform,
    }

    # Apply transformations
    for rule in config.rules:
        if rule.column not in data.columns:
            logger.warning(
                f"Column {rule.column} not found in the dataset. Skipping transformation."
            )
            continue
        method = rule.method
        data = rule_mapping[method](data, **rule.model_dump())

    config.output.write_data(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        usage="python -m anonymize --config config.yml",
        description="Anonymize data based on the provided configuration.",
    )
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--config",
        "-c",
        type=str,
        help="Path to the yaml configuration file",
        default="config.yml",
        required=True,
    )

    args = arg_parser.parse_args()
    config = load_config(args.config)
    main(config)
