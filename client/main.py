import configparser
import os
from common.logger import get_logger

from client import Client

logger = get_logger("Client")

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Client node is online")
    logger.info("Configuration loaded successfully")

    use_test_dataset = os.getenv("USE_TEST_DATASET", "0") == "1"  # "1" for test dataset, "0" for full dataset

    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    client = Client(config["DEFAULT"]["GATEWAY_HOST"], int(config["DEFAULT"]["GATEWAY_PORT"]),
                    int(config["DEFAULT"]["MAX_BATCH_SIZE"]))
    client.run(use_test_dataset)

if __name__ == "__main__":
    main()
