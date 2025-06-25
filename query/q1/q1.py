import configparser
import json
from collections import defaultdict
import os

from common.duplicate_handler import DuplicateHandler
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Query-Arg-Spain-Genres")


class ArgSpainGenreQuery(QueryBase):
    """
    Películas y sus géneros de los años 00' con producción Argentina y Española.
    """

    def __init__(self, config):
        self.source_queue = config["DEFAULT"].get(
            "movies_arg_spain_2000s_queue", "movies_arg_spain_2000s"
        )
        super().__init__(config, self.source_queue, logger_name="q1")

        self.results_by_request = defaultdict(list)

    def _calculate_and_publish_results(self, client_id):
        key = client_id
        raw_results = self.results_by_request[key]

        results_dict = {title: genres for title, genres in raw_results}

        results = {
            "client_id": client_id,
            "query": "Q1",
            "results": results_dict,
        }

        logger.info("RESULTS for %s: %s", key, results)

        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"], message=results
        )

        logger.debug("LRU: Results published for client %s, %s", client_id, self.duplicate_handler.get_cache(client_id, self.source_queue))

    def process_movie(self, movie, client_id):
        """
        Process a single movie.
        """

        key = client_id

        title = movie.get("original_title")
        genres = [g.get("name") for g in movie.get("genres", []) if g.get("name")]
        self.results_by_request[key].append((title, genres))

        logger.debug("Processed movie '%s' for %s", title, key)

    def callback(self, ch, method, properties, body, input_queue):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after EOS message.
        """
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        headers = getattr(properties, "headers", {}) or {}
        client_id = headers.get("client_id")
        message_id = headers.get("message_id")

        if client_id is None:
            logger.error("Missing client_id in headers")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.client_manager.add_client(client_id)

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                self.rabbitmq_processor.acknowledge(method)
                return

            if not self.client_manager.has_queue_received_eos_from_node(client_id, input_queue, node_id):
                self.client_manager.mark_eos(client_id, input_queue, node_id)
                self.write_eos_to_file(client_id)
                logger.info("EOS received from node %s for client %s", node_id, client_id)
                if self.client_manager.has_received_all_eos(client_id, input_queue):
                    logger.info("All EOS received for client %s", client_id)
                    self._calculate_and_publish_results(client_id)
            else:
                logger.warning(
                    "Duplicated EOS from node %s for client %s", node_id, client_id
                )           

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

        # Normal message (single movie)
        try:
            movie = json.loads(body)

            # Normalize to list
            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("❌ Unexpected movie format: %s, skipping.", type(movie))
                self.rabbitmq_processor.acknowledge(method)
                return

            for single_movie in movie:
                self.process_movie(single_movie, client_id)

            self._write_data_to_file(client_id, self.results_by_request[client_id], "partial_results")

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.duplicate_handler.add(client_id, input_queue, message_id)
        self.rabbitmq_processor.acknowledge(method)

    def update_data(self, client_id, key, data):
        if key == "partial_results":
            if client_id not in self.results_by_request:
                self.results_by_request[client_id] = []
            self.results_by_request[client_id].extend(data)
            logger.info("Data updated for client %s, key %s", client_id, key)
        else:
            logger.warning("Unknown key for update_data: %s", key)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgSpainGenreQuery(config)
    query.process()
