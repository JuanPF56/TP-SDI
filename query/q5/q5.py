import configparser
import json
import os
import pika
from collections import defaultdict

from common.logger import get_logger
from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientManager
from common.client_state_manager import ClientState
logger = get_logger("SentimentStats")
EOS_TYPE = "EOS"  # Type of message indicating end of stream
class SentimentStats:
    """
    Promedio de la tasa ingreso/presupuesto de películas con overview de sentimiento positivo vs. sentimiento negativo
    """
    def __init__(self, config):
        self.config = config
        self.positive_rates = defaultdict(list)
        self.negative_rates = defaultdict(list)
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.source_queues = [
            self.config["DEFAULT"].get("movies_positive_queue", "positive_movies"),
            self.config["DEFAULT"].get("movies_negative_queue", "negative_movies")
        ]
        self.sentiment_results = defaultdict(lambda: defaultdict(int))

        self.target_queue = self.config["DEFAULT"].get("results_queue", "results")
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue
        )
        self.client_manager = ClientManager(
            expected_queues=self.source_queues,
            nodes_to_await=self.eos_to_await,
        )  

    def _calculate_and_publish_results(self, client_id, request_number, client_state: ClientState):
        """
        Calculate the average rates and publish the results.
        """
        if client_state.has_received_all_eos(self.source_queues):
            positives = self.positive_rates[(client_id, request_number)]
            negatives = self.negative_rates[(client_id, request_number)]

            avg_positive = sum(positives) / len(positives) if positives else 0
            avg_negative = sum(negatives) / len(negatives) if negatives else 0

            results = {
                "client_id": client_id,
                "request_number": request_number,
                "query": "Q5",
                "results": {
                    "average_positive_rate": avg_positive,
                    "average_negative_rate": avg_negative
                }
            }
            logger.info("RESULTS:" + str(results))
            # Publish results to a results queue (not implemented here)

            self.rabbitmq_processor.publish(
                target=self.config["DEFAULT"]["results_queue"],
                message=results
            )
            del self.positive_rates[(client_id, request_number)]
            del self.negative_rates[(client_id, request_number)]
            del self.sentiment_results[(client_id, request_number)]
            self.client_manager.remove_client(client_id, request_number)


    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id, request_number = headers.get("client_id"), headers.get("request_number")

            if not client_id or not request_number:
                logger.error("Missing client_id or request_number in headers")
                self.rabbitmq_processor.acknowledge(method)
                return
        
            client_state = self.client_manager.add_client(client_id, request_number)


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
                    
                client_state.mark_eos(input_queue, node_id)
                logger.info(f"EOS received for node {node_id} in {sentiment} queue.")
                if client_state.has_received_all_eos(self.source_queues):
                    logger.info("All nodes have sent EOS.")
                    self._calculate_and_publish_results(client_id, request_number, client_state)
                self.rabbitmq_processor.acknowledge(method)
                return

            movies_batch = json.loads(body)
        except json.JSONDecodeError:
            logger.warning(f"❌ Invalid JSON in {sentiment} queue. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        for movie in movies_batch:
            if (movie.get("budget") > 0):
                rate = movie.get("revenue") / movie.get("budget")
                if sentiment == "positive":
                    self.positive_rates[(client_id, request_number)].append(rate)
                elif sentiment == "negative":
                    self.negative_rates[(client_id, request_number)].append(rate)

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
