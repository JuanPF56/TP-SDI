import os
import yaml
import logging
import argparse
import pandas as pd
import kagglehub

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_dataset():
    try:
        logger.info("Downloading dataset with kagglehub...")
        path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
        logger.info(f"Dataset downloaded at: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None


def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return {}


def prepare_data(config_path=None):
    output_dir = "./data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info("Created data directory.")

    dataset_path = download_dataset()
    if not dataset_path:
        logger.error("Dataset download failed. Cannot prepare test data.")
        return

    config = load_config(config_path) if config_path else {}

    input_files = {
        "movies_metadata.csv": "movies_metadata.csv",
        "credits.csv": "credits.csv",
        "ratings.csv": "ratings.csv",
    }

    for name, filename in input_files.items():
        full_path = os.path.join(dataset_path, filename)
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(full_path):
            logger.info(f"Processing {filename}...")
            try:
                df = pd.read_csv(full_path, low_memory=False)
                pct = config.get(filename)

                if pct is not None:
                    if 0 < pct <= 100:
                        sample_size = int(len(df) * (pct / 100))
                        df = df.head(sample_size)
                        logger.info(
                            f"Trimming {filename} to {pct}% ({sample_size} rows)"
                        )
                    else:
                        logger.warning(
                            f"Invalid percentage for {filename}: {pct}. Skipping trim."
                        )
                else:
                    logger.info(
                        f"No percentage specified for {filename}, keeping full dataset."
                    )

                df.to_csv(output_path, index=False)
                logger.info(f"Saved {filename} to {output_path}")
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
        else:
            logger.warning(f"{filename} not found in downloaded dataset.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare datasets from Kaggle.")
    parser.add_argument(
        "-test",
        type=str,
        help="Path to YAML test config file with percentages for each dataset.",
    )
    args = parser.parse_args()

    prepare_data(config_path=args.test)
