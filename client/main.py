import configparser
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
    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    client = Client(config["DEFAULT"]["GATEWAY_HOST"], int(config["DEFAULT"]["GATEWAY_PORT"]),
                    int(config["DEFAULT"]["MAX_BATCH_SIZE"]))
    client.run()

if __name__ == "__main__":
    main()
