import configparser
import json
import pika

from collections import defaultdict
from common.logger import get_logger

logger = get_logger("Query-Top5-Solo-Country-Budgets")


class SoloCountryBudgetQuery:
    def __init__(self, config):
        self.config = config
        self.budget_by_country = defaultdict(int)

    def process(self):
        """
        Reads from:
        - movies_solo queue (already filtered for productions with a single country)

        Collects:
        - country
        - total budget

        At the end, logs and stores top 5 countries that invested the most (without co-producing).

        NEXT: When receiving the end of stream flag, publish the results to a results queue.
        """
        logger.info("Node is online")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_solo_queue", "movies_solo")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        channel.queue_declare(queue=input_queue)

        def callback(ch, method, properties, body):
            try:
                movie = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            production_countries = movie.get("production_countries", [])
            country = production_countries[0]["name"]
            budget = movie.get("budget", 0)
            self.budget_by_country[country] += budget
            logger.info(f"Added ${budget} to {country}. Total: {self.budget_by_country[country]}")

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
    query = SoloCountryBudgetQuery(config)
    query.process()