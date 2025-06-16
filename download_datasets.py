"""This script downloads datasets from Kaggle using kagglehub and prepares them for testing."""

import argparse
import os
import yaml
import pandas as pd
import kagglehub

from common.logger import get_logger

logger = get_logger("download_datasets")


def _download_dataset():
    try:
        logger.info("Downloading dataset with kagglehub...")
        path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
        logger.info("Dataset downloaded at: %s", path)
        return path
    except Exception as e:
        logger.error("Failed to download dataset: %s", e)
        return None


def _load_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error("Failed to load config file: %s", e)
        return {}


def prepare_data(config_path=None):
    """Prepare datasets for testing by downloading and processing them."""
    output_dir = "./data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info("Created data directory.")

    dataset_path = _download_dataset()
    if not dataset_path:
        logger.error("Dataset download failed. Cannot prepare test data.")
        return

    config = _load_config(config_path) if config_path else {}

    input_files = {
        "movies_metadata.csv": "movies_metadata.csv",
        "credits.csv": "credits.csv",
        "ratings.csv": "ratings.csv",
    }

    for name, filename in input_files.items():
        full_path = os.path.join(dataset_path, filename)
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(full_path):
            logger.info("Processing %s...", filename)
            try:
                df = pd.read_csv(full_path, low_memory=False)
                pct = config.get(filename)

                if pct is not None:
                    if 0 < pct <= 100:
                        sample_size = int(len(df) * (pct / 100))
                        df = df.head(sample_size)
                        logger.info(
                            "Trimming %s to %d%% (%d rows)", filename, pct, sample_size
                        )
                    else:
                        logger.warning(
                            "Invalid percentage for %s: %d. Skipping trim.",
                            filename,
                            pct,
                        )
                else:
                    logger.info(
                        "No percentage specified for %s, keeping full dataset.",
                        filename,
                    )

                df.to_csv(output_path, index=False)
                logger.info("Saved %s to %s", filename, output_path)
            except Exception as e:
                logger.error("Failed to process %s: %s", filename, e)
        else:
            logger.warning("%s not found in downloaded dataset.", filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare datasets from Kaggle.")
    parser.add_argument(
        "-test",
        type=str,
        help="Path to YAML test config file with percentages for each dataset.",
    )
    args = parser.parse_args()

    prepare_data(config_path=args.test)
