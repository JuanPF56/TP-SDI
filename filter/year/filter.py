import configparser
import json
import pika
from datetime import datetime

from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Year")

class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.config = config

    def process(self):
        """
        Reads from:
        - movies_argentina
        - movies_arg_spain

        Writes to:
        - movies_arg_post_2000: Argentine-only movies after 2000
        - movies_arg_spain_2000s: Argentina+Spain movies between 2000-2009
        """
        logger.info("Node is online")
        logger.info("Configuration loaded successfully")
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"{key}: {value}")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")

        input_queues = {
            "argentina": self.config["DEFAULT"].get("movies_argentina_queue", "movies_argentina"),
            "arg_spain": self.config["DEFAULT"].get("movies_arg_spain_queue", "movies_arg_spain")
        }

        output_queues = {
            "arg_post_2000": self.config["DEFAULT"].get("movies_arg_post_2000_queue", "movies_arg_post_2000"),
            "arg_spain_2000s": self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        for queue in list(input_queues.values()) + list(output_queues.values()):
            channel.queue_declare(queue=queue)

        def callback(ch, method, properties, body):
            movie = json.loads(body)
            title = movie.get("title", "Unknown title")
            date_str = movie.get("release_date", "")
            release_year = self.extract_year(date_str)

            logger.info(f"Processing '{title}' released in {release_year} from queue '{method.routing_key}'")

            if method.routing_key == input_queues["argentina"]:
                if release_year and release_year > 2000:
                    channel.basic_publish(exchange='', routing_key=output_queues["arg_post_2000"], body=body)
                    logger.info(f"✔ Sent to {output_queues['arg_post_2000']}")
            elif method.routing_key == input_queues["arg_spain"]:
                if release_year and 2000 <= release_year <= 2009:
                    channel.basic_publish(exchange='', routing_key=output_queues["arg_spain_2000s"], body=body)
                    logger.info(f"✔ Sent to {output_queues['arg_spain_2000s']}")
            else:
                logger.warning("⚠ Unknown source queue")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        for queue in input_queues.values():
            logger.info(f"Waiting for messages from '{queue}'...")
            channel.basic_consume(queue=queue, on_message_callback=callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            channel.stop_consuming()
        finally:
            connection.close()

    def extract_year(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").year
        except Exception as e:
            logger.warning(f"⚠ Invalid release_date '{date_str}': {e}")
            return None

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    year_filter = YearFilter(config)
    year_filter.process()
