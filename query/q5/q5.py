import configparser
import json
import pika
from common.logger import get_logger

logger = get_logger("SentimentStats")
EOS_TYPE = "EOS"  # Type of message indicating end of stream
class SentimentStats:
    """
    Promedio de la tasa ingreso/presupuesto de películas con overview de sentimiento positivo vs. sentimiento negativo
    """
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

    def _calculate_and_publish_results(self):
        """
        Calculate the average rates and publish the results.
        """
        if self.positive_rates:
            avg_positive = sum(self.positive_rates) / len(self.positive_rates)
        else:
            avg_positive = 0

        if self.negative_rates:
            avg_negative = sum(self.negative_rates) / len(self.negative_rates)
        else:
            avg_negative = 0

        results = {
            "query": "Q5",
            "results": {
                "average_positive_rate": avg_positive,
                "average_negative_rate": avg_negative
            }
        }
        logger.info("RESULTS:" + str(results))
        # Publish results to a results queue (not implemented here)

    def _callback_factory(self, sentiment):
        def callback(ch, method, properties, body):
            try:
                msg_type = properties.type if properties and properties.type else "UNKNOWN"

                if msg_type == EOS_TYPE:
                    logger.info("End of stream received")
                    self._calculate_and_publish_results()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return


                movie = json.loads(body)
            except json.JSONDecodeError:
                logger.warning(f"❌ Invalid JSON in {sentiment} queue. Skipping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            budget = movie.get("budget", 0)
            revenue = movie.get("revenue", 0)
            if budget > 0:
                rate = revenue / budget
                if sentiment == "positive":
                    self.positive_rates.append(rate)
                else:
                    self.negative_rates.append(rate)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        return callback


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
