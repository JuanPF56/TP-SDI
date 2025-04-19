import configparser
import json
import pika
from common.logger import get_logger

logger = get_logger("SentimentStats")


class SentimentStats:
    def __init__(self, config):
        self.config = config
        self.positive_rates = []
        self.negative_rates = []
        self.received_eos = {"positive": False, "negative": False}

    def _connect(self):
        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        self.channel = self.connection.channel()

        self.positive_queue = self.config["DEFAULT"].get("positive_movies_queue", "positive_movies")
        self.negative_queue = self.config["DEFAULT"].get("negative_movies_queue", "negative_movies")

        self.channel.queue_declare(queue=self.positive_queue)
        self.channel.queue_declare(queue=self.negative_queue)

    def _callback_factory(self, sentiment):
        def callback(ch, method, properties, body):
            pass
    def run(self):
        self._connect()

        logger.info("📡 Waiting for messages on both queues...")

        self.channel.basic_consume(queue=self.positive_queue, on_message_callback=self._callback_factory("positive"))
        self.channel.basic_consume(queue=self.negative_queue, on_message_callback=self._callback_factory("negative"))

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("✋ Interrupted. Exiting gracefully.")
            self.channel.stop_consuming()
        finally:
            self.connection.close()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    stats = SentimentStats(config)
    stats.run()
