import configparser
import json
from collections import defaultdict

from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger
logger = get_logger("Query-Arg-Spain-Genres")

class ArgSpainGenreQuery(QueryBase):
    """
    Películas y sus géneros de los años 00' con producción Argentina y Española.
    """
    def __init__(self, config):
        source_queue = config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        super().__init__(config, source_queue, logger_name="q1")

        self.results_by_request = defaultdict(list)


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
        self.client_manager.remove_client(client_id, request_number)

    def process_batch(self, movies_batch, client_id, request_number):
        """
        Process a batch of movies.
        """

        key = (client_id, request_number)
        
        logger.debug(f"Processing batch of {len(movies_batch)} movies for {key}")

        for movie in movies_batch:
            title = movie.get("original_title")
            genres = [g.get("name") for g in movie.get("genres", []) if g.get("name")]
            self.results_by_request[key].append((title, genres))

    def callback(self, ch, method, properties, body, input_queue):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after processing a batch.
        """
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        headers = getattr(properties, "headers", {}) or {}
        client_id, request_number = headers.get("client_id"), headers.get("request_number")

        if not client_id or not request_number:
            logger.error("Missing client_id or request_number in headers")
            self.rabbitmq_processor.acknowledge(method)
            return
    
        client_state = self.client_manager.add_client(client_id, request_number)

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                self.rabbitmq_processor.acknowledge(method)
                return

            if client_state.has_queue_received_eos(input_queue):
                logger.warning(f"Duplicated EOS from node {node_id} for request {client_id} and request number {request_number}")   
                self.rabbitmq_processor.acknowledge(method)
                return

            client_state.mark_eos(input_queue, node_id)
            logger.info(f"EOS received from node {node_id} for request {client_id} and request number {request_number}")

            if client_state.has_received_all_eos(input_queue):
                logger.info(f"All EOS received for request {client_id} and request number {request_number}")
                self._calculate_and_publish_results(client_state.client_id, client_state.request_id)

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

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgSpainGenreQuery(config)
    query.process()
