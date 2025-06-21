import configparser
import json
from collections import defaultdict

from common.client_state_manager import ClientState
from common.duplicate_handler import DuplicateHandler
from common.query_base import QueryBase, EOS_TYPE
from common.logger import get_logger

logger = get_logger("SentimentStats")


class SentimentStats(QueryBase):
    """
    Promedio de la tasa ingreso/presupuesto de películas con overview de sentimiento positivo vs. sentimiento negativo
    """

    def __init__(self, config):
        self.source_queues = [
            config["DEFAULT"].get("movies_positive_queue", "positive_movies"),
            config["DEFAULT"].get("movies_negative_queue", "negative_movies"),
        ]
        super().__init__(config, self.source_queues, logger_name="q5")

        self.duplicate_handler = DuplicateHandler()

        self.positive_rates = defaultdict(list)
        self.negative_rates = defaultdict(list)

    def _calculate_and_publish_results(self, client_id, client_state: ClientState):
        """
        Calculate the average rates and publish the results.
        """
        if client_state.has_received_all_eos(self.source_queues):
            positives = self.positive_rates[client_id]
            negatives = self.negative_rates[client_id]

            avg_positive = sum(positives) / len(positives) if positives else 0
            avg_negative = sum(negatives) / len(negatives) if negatives else 0

            results = {
                "client_id": client_id,
                "query": "Q5",
                "results": {
                    "average_positive_rate": avg_positive,
                    "average_negative_rate": avg_negative,
                },
            }
            logger.info("RESULTS: %s", results)

            self.rabbitmq_processor.publish(
                target=self.config["DEFAULT"]["results_queue"], message=results
            )

            logger.debug("LRU: Results published for client %s in positive queue: %s",
                        client_id, self.duplicate_handler.get_cache(client_id, self.source_queues[0]))
            logger.debug("LRU: Results published for client %s in negative queue: %s",
                        client_id, self.duplicate_handler.get_cache(client_id, self.source_queues[1]))

            del self.positive_rates[client_id]
            del self.negative_rates[client_id]
            self.client_manager.remove_client(client_id)

    def process_movie(self, movie, client_id, sentiment):
        """
        Process a single movie to compute revenue/budget rate.
        """
        budget = movie.get("budget", 0)
        if budget <= 0:
            return

        revenue = movie.get("revenue", 0)
        rate = revenue / budget

        if sentiment == "positive":
            self.positive_rates[client_id].append(rate)
        elif sentiment == "negative":
            self.negative_rates[client_id].append(rate)

        logger.debug(
            "Processed movie for client %s with sentiment %s: rate=%.3f",
            client_id,
            sentiment,
            rate,
        )

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id")
            message_id = headers.get("message_id")

            if client_id is None:
                logger.error("Missing client_id in headers")
                self.rabbitmq_processor.acknowledge(method)
                return

            client_state = self.client_manager.add_client(client_id)

            sentiment = "unknown"
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
                    self.rabbitmq_processor.acknowledge(method)
                    return

                client_state.mark_eos(input_queue, node_id)
                logger.info("EOS received for node %s in %s queue.", node_id, sentiment)

                if client_state.has_received_all_eos(self.source_queues):
                    logger.info("All nodes have sent EOS.")
                    self._calculate_and_publish_results(client_id, client_state)

                self.rabbitmq_processor.acknowledge(method)
                return
            
            if message_id is None:
                logger.error("Missing message_id in headers")
                self.rabbitmq_processor.acknowledge(method)
                return
            
            if self.duplicate_handler.is_duplicate(client_id, input_queue, message_id):
                logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
                self.rabbitmq_processor.acknowledge(method)
                return

            movie = json.loads(body)

            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("❌ Unexpected movie format: %s, skipping.", type(movie))
                self.rabbitmq_processor.acknowledge(method)
                return

            for single_movie in movie:
                self.process_movie(single_movie, client_id, sentiment)

            self.duplicate_handler.add(client_id, input_queue, message_id)
        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return
        
        except Exception as e:
            logger.error("❌ Error processing message: %s", e)
            self.rabbitmq_processor.acknowledge(method)
            return
        
        self.rabbitmq_processor.acknowledge(method)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = SentimentStats(config)
    query.process()
