import configparser
import os
import time

from client import Client

from common.logger import get_logger

logger = get_logger("Client")


def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config


def main():
    start_time = time.time()

    config = load_config()
    logger.info("Client node is online")
    logger.info("Configuration loaded successfully")

    use_test_dataset = (
        os.getenv("USE_TEST_DATASET", "0") == "1"
    )  # "1" for test dataset, "0" for full dataset
    logger.info(f"Using test dataset: {True if use_test_dataset else False}")

    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    client = Client(
        config["DEFAULT"]["PROXY_HOST"],
        int(config["DEFAULT"]["PROXY_PORT"]),
        int(config["DEFAULT"]["MAX_BATCH_SIZE"]),
    )

    client.run(use_test_dataset)

    end_time = time.time()
    elapsed = int(end_time - start_time)
    minutes, seconds = divmod(elapsed, 60)
    logger.info("Total time spent: %dm %ds", minutes, seconds)


if __name__ == "__main__":
    main()
