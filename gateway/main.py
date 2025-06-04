"""
Gateway Node Main Script
This script initializes the gateway node, loads the configuration,
cleans the resultados folder, and starts the gateway service.
"""

import os
import shutil
from configparser import ConfigParser

from common.logger import get_logger

from gateway import Gateway

logger = get_logger("Gateway")


def load_config():
    config = ConfigParser()
    config.read("config.ini")
    return config


def clean_resultados_folder():
    resultados_path = "/app/resultados"
    if os.path.exists(resultados_path):
        for filename in os.listdir(resultados_path):
            file_path = os.path.join(resultados_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # eliminar archivo o link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # eliminar carpeta
                logger.info("Deleted: %s", file_path)
            except Exception as e:
                logger.error("Failed to delete %s. Reason: %s", file_path, e)
    else:
        logger.warning("Result directory does not exist: %s", resultados_path)


def main():
    config = load_config()
    logger.info("Gateway node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info("%s: %s", key, value)

    clean_resultados_folder()

    gateway = Gateway(config)
    logger.info("Gateway started successfully")
    gateway.run()


if __name__ == "__main__":
    main()
