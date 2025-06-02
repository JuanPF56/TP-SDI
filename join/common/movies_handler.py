import json
import multiprocessing

from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MoviesHandler")

EOS_TYPE = "EOS"


class MoviesHandler(multiprocessing.Process):
    def __init__(self, config, manager, ready_event, node_id, year_nodes_to_await):
        """
        Initialize the MoviesHandler class with the given configuration and manager.
        """
        super().__init__(target=self.run)
        self.config = config
        self.rabbitmq_processor = RabbitMQProcessor(
            config,
            [],
            [],
            config["DEFAULT"].get("rabbitmq_host", "rabbitmq"),
            source_exchange=config["DEFAULT"].get(
                "movies_exchange", "movies_arg_post_2000"
            ),
        )
        self.rabbitmq_processor.connect()
        self.manager = manager
        self.movies = self.manager.dict()

        self.node_id = node_id

        self.year_eos_flags = self.manager.dict()
        self.year_nodes_to_await = year_nodes_to_await
        self.ready = False
        self.movies_table_ready = ready_event

        self.current_client_id = None
        self.current_request_number = None

    def terminate(self):
        self.rabbitmq_processor.stop_consuming()
        self.rabbitmq_processor.close()
        super().terminate()

    def run(self):
        """
        Start the process to receive the movies from the broadcast exchange.
        It will consume messages from the queue and populate the movies table for each client.
        Once all nodes for the client have sent EOS messages, it will notify that
        at least one table is ready.
        """

        def callback(ch, method, properties, body, queue_name):
            try:
                msg_type = (
                    properties.type if properties and properties.type else "UNKNOWN"
                )
                headers = getattr(properties, "headers", {}) or {}
                self.current_client_id = headers.get("client_id")
                id_tuple = self.current_client_id

                if not self.current_client_id:
                    logger.error("Missing client_id in headers")
                    self.rabbitmq_processor.acknowledge(method)
                    return
                if msg_type == EOS_TYPE:
                    try:
                        data = json.loads(body)
                        node_id = data.get("node_id")
                    except json.JSONDecodeError:
                        logger.error("Failed to decode EOS message")
                        return
                    if id_tuple not in self.year_eos_flags:
                        self.year_eos_flags[id_tuple] = self.manager.dict()
                    if node_id not in self.year_eos_flags[id_tuple]:
                        self.year_eos_flags[id_tuple][node_id] = True
                        logger.debug("EOS received for node %s.", node_id)
                    if not self.ready and self.client_ready(self.current_client_id):
                        self.ready = True
                        logger.debug(
                            "One table is ready for at least 1 client. Notifying..."
                        )
                        self.movies_table_ready.set()
                else:
                    try:
                        data = json.loads(body)
                        movies_data = data
                    except json.JSONDecodeError:
                        logger.error("Error decoding JSON: %s", body)
                        return
                    logger.debug("Received message: %s", movies_data)
                    if id_tuple not in self.movies:
                        self.movies[id_tuple] = self.manager.list()
                    for movie in movies_data:
                        new_movie = {
                            "id": str(movie["id"]),
                            "original_title": movie["original_title"],
                        }
                        self.movies[id_tuple].append(new_movie)
                    logger.debug(
                        "Movies table updated for client %s, request %s: %s",
                        self.current_client_id,
                        self.current_request_number,
                        self.movies[id_tuple],
                    )
            except Exception as e:
                logger.error("Error processing message: %s", e)
            finally:
                self.rabbitmq_processor.acknowledge(method)

        self.rabbitmq_processor.consume(callback)
        self.rabbitmq_processor.close()

    def client_ready(self, client_id):
        """
        Check if all nodes have sent EOS messages for the given client ID.
        """
        if client_id not in self.year_eos_flags:
            return False
        return len(self.year_eos_flags[client_id]) == int(self.year_nodes_to_await)

    def get_movies_table(self, client_id):
        """
        Get the movies table for the given client ID.
        """
        id_tuple = client_id
        if id_tuple not in self.movies:
            return None
        return list(self.movies[id_tuple])

    def remove_movies_table(self, client_id):
        """
        Remove the movies table for the given client ID.
        """
        id_tuple = client_id
        if id_tuple in self.movies:
            del self.movies[id_tuple]
            logger.debug("Movies table removed for client %s", client_id)
        else:
            logger.error("No movies table found for client %s", client_id)
