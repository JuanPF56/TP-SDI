import configparser
import json
import os
from collections import defaultdict
from common.logger import get_logger
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"

logger = get_logger("Query-Arg-Spain-Genres")


class ArgSpainGenreQuery:
    """
    Películas y sus géneros de los años 00' con producción Argentina y Española.
    """

    def __init__(self, config):
        self.config = config

        self.source_queue = self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results_queue")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))

        self.results_by_request = defaultdict(list)
        self.eos_flags_by_request = defaultdict(set)

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )

    def _calculate_and_publish_results(self, client_id, request_number):
        key = (client_id, request_number)
        raw_results = self.results_by_request[key]

        results_dict = {title: genres for title, genres in raw_results}

        results = {
            "client_id": client_id,
            "request_number": request_number,
            "query": "Q1",
            "results": results_dict
        }

        logger.info(f"RESULTS for {key}: {results}")

        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"],
            message=results
        )

        # Limpieza de datos para liberar memoria
        del self.results_by_request[key]
        del self.eos_flags_by_request[key]

    def process_batch(self, movies_batch, client_id, request_number):
        """
        Process a batch of movies.
        """
        logger.debug(f"Processing batch of {len(movies_batch)} movies...")

        key = (client_id, request_number)

        for movie in movies_batch:
            title = movie.get("original_title")
            genres = [g.get("name") for g in movie.get("genres", []) if g.get("name")]
            self.results_by_request[key].append((title, genres))

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        headers = properties.headers or {}

        client_id = headers.get("client_id")
        request_number = headers.get("request_number")

        if not client_id or request_number is None:
            logger.warning("❌ Missing client_id or request_number in headers. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        key = (client_id, request_number)

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                return

            if node_id in self.eos_flags_by_request[key]:
                logger.warning(f"Duplicated EOS from node {node_id} for request {key}")
                return

            self.eos_flags_by_request[key].add(node_id)
            logger.info(f"EOS received from node {node_id} for request {key}")

            if len(self.eos_flags_by_request[key]) == self.eos_to_await:
                logger.info(f"All EOS received for request {key}. Calculating results.")
                self._calculate_and_publish_results(client_id, request_number)

            self.rabbitmq_processor.acknowledge(method)
            return

        # Normal message (batch of movies)
        try:
            movies_batch = json.loads(body)
            if not isinstance(movies_batch, list):
                logger.warning("❌ Expected a list (batch) of movies, skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            self.process_batch(movies_batch, client_id, request_number)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.rabbitmq_processor.acknowledge(method)

    def process(self):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after processing a batch.
        """
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
    query = ArgSpainGenreQuery(config)
    query.process()
