from configparser import ConfigParser
from common.logger import get_logger

from gateway import Gateway

logger = get_logger("Gateway")

def load_config():
    config = ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Gateway node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    gateway = Gateway(int(config["DEFAULT"]["GATEWAY_PORT"]), int(config["DEFAULT"]["LISTEN_BACKLOG"]),
                      int(config["DEFAULT"]["DATASETS_EXPECTED"]))
    logger.info("Gateway started successfully")

    gateway.run()

if __name__ == "__main__":
    main()
