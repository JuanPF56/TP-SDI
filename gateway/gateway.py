import configparser
from common.logger import get_logger

logger = get_logger("Gateway")

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Gateway node is online")
    logger.info(f"Config: {config["DEFAULT"]}")

if __name__ == "__main__":
    main()
