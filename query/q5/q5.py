import configparser
import json
import os
import pika
from common.logger import get_logger
from common.mom import RabbitMQProcessor

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
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1")) * 2 # Two queues to await EOS from
        self.received_eos = {
            "positive": {},
            "negative": {}
        }
        self.source_queues = [
            self.config["DEFAULT"].get("movies_positive_queue", "positive_movies"),
            self.config["DEFAULT"].get("movies_negative_queue", "negative_movies")
        ]
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results")
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue
        )   

    def _calculate_and_publish_results(self):
        """
        Calculate the average rates and publish the results.
        """
        if self.received_eos["positive"] and self.received_eos["negative"]:
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

            self.rabbitmq_processor.publish(
                queue=self.config["DEFAULT"]["results_queue"],
                message=results
            )

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if input_queue == self.source_queues[0]:
                sentiment = "positive"
            elif input_queue == self.source_queues[1]:
                sentiment = "negative"

            if msg_type == EOS_TYPE:
                try:
                    data = json.loads(body)
                    node_id = data.get("node_id")
                except json.JSONDecodeError:
                    logger.error("Failed to decode EOS message")
                    self.rabbitmq_processor.acknowledge(method)  # Make sure to acknowledge
                    return
                    
                self.received_eos[sentiment][node_id] = True
                logger.info(f"EOS received for node {node_id} in {sentiment} queue.")
                eos_received = len(self.received_eos.get("positive", {})) + \
                    len(self.received_eos.get("negative", {}))
                all_eos_received = all(self.received_eos[sent].get(node) for sent in \
                                        ["positive","negative"] for node in self.received_eos[sent])
                if eos_received == int(self.eos_to_await) and all_eos_received:
                    logger.info("All nodes have sent EOS.")
                    self._calculate_and_publish_results()
                self.rabbitmq_processor.acknowledge(method)
                return

            movies_batch = json.loads(body)
        except json.JSONDecodeError:
            logger.warning(f"❌ Invalid JSON in {sentiment} queue. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        for movie in movies_batch:
            if (movie.get("budget") > 0):
                if sentiment == "positive":
                    rate = movie.get("revenue") / movie.get("budget")
                    self.positive_rates.append(rate)
                elif sentiment == "negative":
                    rate = movie.get("revenue") / movie.get("budget") 
                    self.negative_rates.append(rate)

        self.rabbitmq_processor.acknowledge(method)


    def run(self):
        logger.info("Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    stats = SentimentStats(config)
    stats.run()
