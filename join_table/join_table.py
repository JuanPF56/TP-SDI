import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Join Table node is online")
    for section in config.sections():
        for key, value in config.items(section):
            logger.info(f"{section}.{key} = {value}")

if __name__ == "__main__":
    main()
