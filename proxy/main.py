"""
Proxy server that forwards requests to multiple gateways.
This script listens for incoming connections on port 9000 and forwards
requests to a list of defined gateways. If a gateway is unavailable,
it tries the next one in the list. The script uses threading to handle
multiple clients concurrently.
"""

from configparser import ConfigParser
from proxy import Proxy

from common.logger import get_logger

logger = get_logger("Proxy")


def load_config():
    config = ConfigParser()
    config.read("config.ini")
    return config


def main():
    """
    Main function to start the proxy server.
    It reads the GATEWAYS environment variable to determine which gateways
    to forward requests to. If no gateways are configured, it logs an error
    and exits. The server listens on the port specified by the PROXY_PORT
    environment variable, defaulting to 9000 if not set.
    """
    config = load_config()
    logger.info("Proxy node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info("%s: %s", key, value)

    proxy = Proxy(config)
    logger.info("Proxy started successfully")
    proxy.run()


if __name__ == "__main__":
    main()
