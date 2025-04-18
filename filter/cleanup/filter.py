import configparser
import json
import pika
import time

from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Cleanup")

class CleanupFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)

        self.source_queues = [
            self.config["DEFAULT"].get("movies_raw_queue", "movies_raw"),
            self.config["DEFAULT"].get("ratings_raw_queue", "ratings_raw"),
            self.config["DEFAULT"].get("credits_raw_queue", "credits_raw"),
        ]

        self.target_queues = {
            self.source_queues[0]: self.config["DEFAULT"].get("movies_clean_queue", "movies_clean"),
            self.source_queues[1]: self.config["DEFAULT"].get("ratings_clean_queue", "ratings_clean"),
            self.source_queues[2]: self.config["DEFAULT"].get("credits_clean_queue", "credits_clean"),
        }

        self.rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        self.connection = None
        self.channel = None
        
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    filter_instance = CleanupFilter(config)
    filter_instance.process()