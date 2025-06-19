import json
import multiprocessing
import signal

from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MoviesHandler")

EOS_TYPE = "EOS"


class MoviesHandler(multiprocessing.Process):
    def __init__(self, config, manager, ready_event, node_id, node_name, year_nodes_to_await):
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
        self.rabbitmq_processor.connect(node_name=node_name+f"_{self.node_id}")
        self.manager = manager
        self.movies = self.manager.dict()

        self.node_id = node_id
        self.stopped = False

        self.year_eos_flags = self.manager.dict()
        self.year_nodes_to_await = year_nodes_to_await
        self.ready = False
        self.movies_table_ready = ready_event

        self.current_client_id = None
        self.current_request_number = None

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)
        signal.signal(signal.SIGINT, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        self.log_info("SIGTERM signal received. Closing connection...")
        try:
            self._close_connection()
        except Exception as e:
            self.log_info(f"Error closing connection: {e}")

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
                        decoded = json.loads(body)
                        if isinstance(decoded, list):
                            logger.debug(f"Received list: {decoded}")
                            movies_data = decoded
                        elif isinstance(decoded, dict):
                            logger.debug(f"Received dict: {decoded}")
                            movies_data = [decoded]
                        else:
                            logger.warning(f"Unexpected JSON format: {decoded}")
                            return
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
                        if new_movie not in self.movies[id_tuple]:
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

        try:
            self.rabbitmq_processor.consume(callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            self._close_connection()

    def _close_connection(self):
        if not self.stopped:
            try:
                logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.stop_consuming()
                self.rabbitmq_processor.close()
                logger.info("Connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.stopped = True

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

    def recover_movies_table(self, node_id):
        """
        Recover the movies tables for the given node ID.
        This method will be called when a node requests recovery.
        """
        for client_id in self.movies:
            movies_table = self.get_movies_table(client_id)
            if movies_table:
                self.rabbitmq_processor.publish(
                    target=f"movies_exchange_node_{self.node_name + f'_{node_id}'}",
                    message={movies_table},
                    headers={"client_id": client_id},
                )
            year_eos_flags = self.year_eos_flags.get(client_id, {})
            if year_eos_flags:
                for node in year_eos_flags:
                    self.rabbitmq_processor.publish(
                        target=f"movies_exchange_node_{self.node_name + f'_{node_id}'}",
                        message={"node_id": node},
                        msg_type=EOS_TYPE,
                        headers={"client_id": client_id},
                    )
