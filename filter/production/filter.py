import configparser
import json
import pika

from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Production")

class ProductionFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.config = config

    def process(self):
        logger.info("Node is online")
        logger.info("Configuration loaded successfully")
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"{key}: {value}")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_clean_queue", "movies_clean")
        output_queues = {
            "movies_argentina": self.config["DEFAULT"].get("movies_argentina_queue", "movies_argentina"),
            "movies_solo": self.config["DEFAULT"].get("movies_solo_queue", "movies_solo"),
            "movies_arg_spain": self.config["DEFAULT"].get("movies_arg_spain_queue", "movies_arg_spain")
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        channel.queue_declare(queue=input_queue)
        for queue in output_queues.values():
            channel.queue_declare(queue=queue)


        def callback(ch, method, properties, body):
            pass


        logger.info(f"Waiting for messages from '{input_queue}'...")
        channel.basic_consume(queue=input_queue, on_message_callback=callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            channel.stop_consuming()
        finally:
            connection.close()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    production_filter = ProductionFilter(config)
    production_filter.process()
