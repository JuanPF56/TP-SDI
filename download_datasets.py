import os
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

def prepare_data(test_lines=None):
    output_dir = "./datasets_for_test"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info("Created datasets_for_test directory.")

    dataset_path = download_dataset()
    if not dataset_path:
        logger.error("Dataset download failed. Cannot prepare test data.")
        return

    input_files = {
        "movies_metadata.csv": "movies_metadata.csv",
        "credits.csv": "credits.csv",
        "ratings.csv": "ratings.csv"
    }

    for name, filename in input_files.items():
        full_path = os.path.join(dataset_path, filename)
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(full_path):
            logger.info(f"Processing {filename}...")
            try:
                df = pd.read_csv(full_path, low_memory=False)
                if test_lines:
                    df = df.head(test_lines)
                    logger.info(f"Trimming {filename} to first {test_lines} rows")
                else:
                    logger.info(f"Keeping full {filename}")
                df.to_csv(output_path, index=False)
                logger.info(f"Saved {filename} to {output_path}")
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
        else:
            logger.warning(f"{filename} not found in downloaded dataset.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare datasets from Kaggle.")
    parser.add_argument('--test', type=int, help="Trim datasets to first N rows (optional)")
    args = parser.parse_args()

    prepare_data(test_lines=args.test)
