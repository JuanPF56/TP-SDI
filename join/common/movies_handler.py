import json
import multiprocessing
import os
import signal
import tempfile

from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MoviesHandler")

EOS_TYPE = "EOS"


class MoviesHandler(multiprocessing.Process):
    def __init__(self, config, manager, ready_event, node_id, node_name, year_nodes_to_await, is_leader):
        """
        Initialize the MoviesHandler class with the given configuration and manager.
        """
        super().__init__(target=self.run)
        self.config = config
        self.source_exchange = config["DEFAULT"].get(
            "movies_exchange", "movies_arg_post_2000"
        )
        self.rabbitmq_processor = RabbitMQProcessor(
            config,
            [],
            [],
            config["DEFAULT"].get("rabbitmq_host", "rabbitmq"),
            source_exchange=self.source_exchange,
        )
        self.node_id = node_id
        self.node_name = node_name
        self.stopped = False
        self.rabbitmq_processor.connect(node_name=node_name)
        self.manager = manager
        self.movies = self.manager.dict()

        self.is_leader = is_leader

        self.year_eos_flags = self.manager.dict()
        self.year_nodes_to_await = year_nodes_to_await
        self.ready = False
        self.movies_table_ready = ready_event

        self.done_recovering = multiprocessing.Event()

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
                        #self.write_storage("moveos", self.year_eos_flags[id_tuple], self.current_client_id)
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
                    #if self.is_leader:
                        #self.done_recovering.wait()
                        #self.write_storage(
                        #    "movies",
                        #    self.movies[id_tuple],
                        #    self.current_client_id,
                        #)
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
        if self.movies[id_tuple] is None or len(self.movies[id_tuple]) == 0:
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
        Recover the movies tables and EOS flags for the given node ID.
        Sends the list of movies and EOS messages for each client.
        """

        rabbit = RabbitMQProcessor(
            config=self.config,
            source_queues=[],
            target_queues=[],
            target_exchange=self.source_exchange,
        )
        if not rabbit.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return

        all_movies = self.get_movies_tables()
        all_eos_flags = self.get_year_eos_flags()

        for client_id, movie_list in all_movies.items():
            logger.info("Recovering movies for client %s, sending to exchange %s", client_id, self.source_exchange)
            # Send the movie list
            rabbit.publish(
                target=self.source_exchange,
                message=movie_list,
                exchange=True,
                headers={"client_id": client_id},
            )

            # Send EOS messages if present
            client_eos_flags = all_eos_flags.get(client_id, {})
            for node in client_eos_flags:
                rabbit.publish(
                    target=self.source_exchange,
                    message={"node_id": node},
                    exchange=True,
                    msg_type=EOS_TYPE,
                    headers={"client_id": client_id},
                    priority=1
                )

        rabbit.close()
    
    def get_movies_tables(self):
        """
        Get a deep copy of all movies tables.
        """
        return {
            client_id: list(movies)
            for client_id, movies in self.movies.items()
            if movies is not None and len(movies) > 0
        }
    
    def get_year_eos_flags(self):
        """
        Get a deep copy of all year EOS flags.
        """
        eos_copy = {}
        for client_id, eos_dict in self.year_eos_flags.items():
            eos_copy[client_id] = dict(eos_dict)
        return eos_copy
    
    def clear_done_recovering(self):
        """
        Reset the done reading event to allow processing of new messages.
        This is typically called when the node is elected as leader.
        """
        self.done_recovering.clear()
        logger.debug("Done reading event reset for MoviesHandler.")

    def write_storage(self, key, data, client_id):
        storage_dir = "./storage"
        os.makedirs(storage_dir, exist_ok=True)
        file_path = os.path.join(storage_dir, f"{key}_{client_id}.json")
        tmp_file = None

        if isinstance(data, multiprocessing.managers.DictProxy):
            serializable_data = dict(data)
        elif isinstance(data, multiprocessing.managers.ListProxy):
            serializable_data = list(data)
        else:
            serializable_data = data

        try:
            with tempfile.NamedTemporaryFile("w", dir=storage_dir, delete=False) as tf:
                json.dump(serializable_data, tf)
                tmp_file = tf.name
            os.replace(tmp_file, file_path)
            logger.debug("Client %s state written to %s", client_id, file_path)
        except Exception as e:
            logger.error(f"Failed to write {key} data for client {client_id}: {e}")
            if tmp_file and os.path.exists(tmp_file):
                os.remove(tmp_file)

    def read_storage(self):
        """
        Load persisted EOS and movies data into memory on startup.
        """
        storage_dir = "./storage"
        if not os.path.exists(storage_dir):
            logger.warning("Storage directory not found.")
            return

        for filename in os.listdir(storage_dir):
            if filename.startswith("moveos_") and filename.endswith(".json"):
                type = "moveos"
            elif filename.startswith("movies_") and filename.endswith(".json"):
                type = "movies"
            else:
                continue
            client_id = filename.split("_")[1][:-5]
            file_path = os.path.join(storage_dir, filename)
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                self.compare_and_update(
                    type,
                    data,
                    client_id
                )
            except Exception as e:
                logger.error(f"Error reading {type} data from {file_path}: {e}")

        for client_id in self.year_eos_flags:
            if not self.ready and self.client_ready(client_id):
                self.ready = True
                logger.debug(
                    "One table is ready for at least 1 client. Notifying..."
                )
                self.movies_table_ready.set()

        self.done_recovering.set()

    def compare_and_update(self, type, data, client_id):
        """
        Compare the data read from storage with the current state and update file if necessary.
        """
        if type == "moveos":
            if client_id not in self.year_eos_flags:
                self.year_eos_flags[client_id] = self.manager.dict()
            current_flags = self.year_eos_flags[client_id]
            updated = False
            for node_id, flag in data.items():
                if node_id not in current_flags or current_flags[node_id] != flag:
                    current_flags[node_id] = flag
                    updated = True
            if updated:
                self.write_storage("moveos", current_flags, client_id)
                logger.debug(f"EOS data for client {client_id} updated.")
        elif type == "movies":
            if client_id not in self.movies:
                self.movies[client_id] = self.manager.list()
            current_movies = self.movies[client_id]
            updated = False
            for movie in data:
                if movie not in current_movies:
                    current_movies.append(movie)
                    updated = True
            if updated:
                self.write_storage("movies", list(current_movies), client_id)
                logger.debug(f"Movies data for client {client_id} updated.")
