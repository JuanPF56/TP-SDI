import configparser
from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Year")

class YearFilter(FilterBase):
    def process(self):
        logger.info("Node is online")
        logger.info("Loaded config:")
        for section in self.config.sections():
            for key, value in self.config.items(section):
                logger.info(f"{section}.{key} = {value}")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    YearFilter(config).process()
