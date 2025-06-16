"""
Gateway Node Main Script
This script initializes the gateway node, loads the configuration
and starts the gateway service.
"""

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
        logger.info("%s: %s", key, value)

    gateway = Gateway(config)
    logger.info("Gateway started successfully")
    gateway.run()


if __name__ == "__main__":
    main()
