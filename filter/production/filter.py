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
        """
        Main processing function for the ProductionFilter.
        This function connects to RabbitMQ, consumes messages from the input queue,
        processes the messages, and publishes them to the appropriate output queues.

        It filters movies based on their production countries and sends them to the respective queues.

        Reads from the clean queue and sends to the filtered queues: 
        - movies_argentina: for movies produced in Argentina
        - movies_solo: for movies produced in only one country
        - movies_arg_spain: for movies produced in both Argentina and Spain
        """
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
            """
            Callback function to process messages from the input queue.
            This function filters the movies based on their production countries and
            sends them to the appropriate output queues.
            """
            movie = json.loads(body)
            country_dicts = movie.get("production_countries", [])
            country_names = [c.get("name") for c in country_dicts if "name" in c]

            logger.info(f"Processing movie: {movie.get('title')}")
            logger.info(f"Production countries: {country_names}")

            if "Argentina" in country_names:
                channel.basic_publish(exchange='', routing_key=output_queues["movies_argentina"], body=body)
                logger.info(f"Sent to {output_queues['movies_argentina']}")

            if len(country_names) == 1:
                channel.basic_publish(exchange='', routing_key=output_queues["movies_solo"], body=body)
                logger.info(f"Sent to {output_queues['movies_solo']}")

            if "Argentina" in country_names and "Spain" in country_names:
                channel.basic_publish(exchange='', routing_key=output_queues["movies_arg_spain"], body=body)
                logger.info(f"Sent to {output_queues['movies_arg_spain']}")

            ch.basic_ack(delivery_tag=method.delivery_tag)


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
