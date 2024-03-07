import sys
from anonymize import load_config, Config

from loguru import logger
import argparse
import polars as pl


def main(config: Config):
    dfs: list[pl.LazyFrame] = []
    for data in config.source:
        if len(dfs) > 0:
            logger.remove()
        for rule in config.rules:
            if rule.column not in data.columns:
                logger.warning(f"Column {rule.column} not found in the dataset. Skipping.")
                continue
            data = rule.apply(data)
        dfs.append(data)

    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
    )
    config.output.write_data(pl.concat(dfs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
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
