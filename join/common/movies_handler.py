import json
import multiprocessing
import os
import signal
import fcntl

from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MoviesHandler")

EOS_TYPE = "EOS"


class MoviesHandler(multiprocessing.Process):
    def __init__(self, config, manager, node_id, node_name, year_nodes_to_await,
                 movies_handler_ready_event, clients_ready_events):
        """
        Initialize the MoviesHandler class with the given configuration and manager.
        """
        super().__init__()
        self.config = config
        self.node_id = node_id
        self.node_name = node_name
        self.source_queue = config["DEFAULT"].get(
            "movies_table_queue", "movies_arg_post_2000"
        ) + "_node_" + str(self.node_name)
        self.shard_start = int(os.getenv("SHARD_RANGE_START", "0"))
        self.shard_end = int(os.getenv("SHARD_RANGE_END", "0"))

        self.rabbitmq_processor = RabbitMQProcessor(
            config,
            [self.source_queue],
            [],
            config["DEFAULT"].get("rabbitmq_host", "rabbitmq"),
        )
        self.stopped = False
        self.rabbitmq_processor.connect(node_name=node_name)
        self.manager = manager
        self.movies = self.manager.dict()
        self.movies_handler_ready_event = movies_handler_ready_event
        self.clients_ready_events = clients_ready_events

        self.year_eos_flags = self.manager.dict()
        self.year_nodes_to_await = year_nodes_to_await

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
        It will first load the memory from disk and then start consuming messages
        from the RabbitMQ queue.
        It will consume messages from the queue and populate the movies table for each client.
        Once all nodes for the client have sent EOS messages, it will notify that
        at least one table is ready.
        """
        self.read_storage()  # Load memory from disk if available

        def callback(ch, method, properties, body, queue_name):
            try:
                msg_type = (
                    properties.type if properties and properties.type else "UNKNOWN"
                )
                headers = getattr(properties, "headers", {}) or {}
                current_client_id = headers.get("client_id")

                if not current_client_id:
                    logger.error("Missing client_id in headers")
                    self.rabbitmq_processor.acknowledge(method)
                    return

                if msg_type == EOS_TYPE:
                    try:
                        data = json.loads(body)
                        node_id = str(data.get("node_id"))
                    except json.JSONDecodeError:
                        logger.error("Failed to decode EOS message")
                        return
                    logger.info("EOS received for node %s in queue %s for client %s, eos flags: %s",
                                node_id, queue_name, current_client_id, self.year_eos_flags.get(current_client_id, {}))
                    if current_client_id not in self.year_eos_flags.keys():
                        logger.info("Creating new EOS flags for client %s", current_client_id)
                        self.year_eos_flags[current_client_id] = self.manager.dict()
                    if node_id in self.year_eos_flags[current_client_id].keys():
                        logger.warning(
                            "Duplicate EOS from node %s for client %s. Ignored.",
                            node_id,
                            current_client_id,
                        )
                    else:
                        self.year_eos_flags[current_client_id][node_id] = True
                        self.write_storage("moveos", self.year_eos_flags[current_client_id], current_client_id, self.node_id)
                        logger.debug("EOS received for node %s.", node_id)
                    logger.info("EOS flags for client %s: %s", current_client_id, self.year_eos_flags[current_client_id])   
                    if self.client_ready(current_client_id):
                        logger.info("Client %s is ready with EOS flags: %s",
                                    current_client_id, self.year_eos_flags[current_client_id])
                        if not self.movies_handler_ready_event.is_set():
                            self.movies_handler_ready_event.set()
                            logger.info("MoviesHandler is now ready for client %s", current_client_id)
                        if current_client_id not in self.clients_ready_events:
                            self.clients_ready_events[current_client_id] = multiprocessing.Event()
                        self.clients_ready_events[current_client_id].set()
                        logger.info("Client %s is now ready and event set.", current_client_id)
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

                    if current_client_id not in self.movies:
                        self.movies[current_client_id] = self.manager.list()

                    filtered_movies = [
                        movie for movie in movies_data
                        if self.shard_start <= int(movie["id"]) <= self.shard_end
                    ]

                    logger.debug(
                        "Filtered movies for client %s: %s",
                        current_client_id,
                        filtered_movies,
                    )
                    

                    if not filtered_movies:
                        logger.debug("No movies in range [%s-%s] for node %s",
                                    self.shard_start, self.shard_end, self.node_name)
                        return

                    for movie in filtered_movies:
                        if movie not in self.movies[current_client_id]:
                            self.movies[current_client_id].append(movie)

                    self.write_storage(
                        "movies",
                        self.movies[current_client_id],
                        current_client_id,
                        self.node_id,
                    )

                    logger.debug(
                        "Movies table updated for client %s: %s",
                        current_client_id,
                        self.movies[current_client_id],
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
        logger.debug("Checking if client %s is ready with EOS flags: %s",
                    client_id, self.year_eos_flags.get(client_id, {}))
        if client_id not in self.year_eos_flags:
            return False
        logger.debug("EOS flags for client %s: %s", client_id, self.year_eos_flags[client_id])
        logger.debug("Year nodes to await for client %s: %s", client_id, self.year_nodes_to_await)
        logger.debug("EOS flags count for client %s: %d", client_id, len(self.year_eos_flags[client_id]))
        return len(self.year_eos_flags[client_id]) >= int(self.year_nodes_to_await)
    
    def get_movies_table(self, client_id):
        """
        Get the movies table for the given client ID.
        """
        if client_id not in self.movies:
            return None
        if self.movies[client_id] is None or len(self.movies[client_id]) == 0:
            return None
        return list(self.movies[client_id])

    def remove_movies_table(self, client_id):
        """
        Remove the movies table for the given client ID.
        """
        if client_id in self.movies:
            del self.movies[client_id]
            logger.debug("Movies table removed for client %s", client_id)
        else:
            logger.error("No movies table found for client %s", client_id)

    def wait_for_client(self, client_id):
        """
        Wait for the client to be ready by checking the EOS flags.
        """
        if client_id not in self.clients_ready_events:
            self.clients_ready_events[client_id] = multiprocessing.Event()
        
        self.clients_ready_events[client_id].wait()
        logger.info("Client %s is ready", client_id)

    def write_storage(self, key, data, client_id, node_id):
        # Get the "./storage/{node_id}" directory
        storage_dir = "./storage"
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            logger.info("Created storage directory: %s", storage_dir)
        storage_dir = os.path.join(storage_dir, str(self.node_id))
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            logger.info("Created storage directory for node %s: %s", self.node_id, storage_dir)
        logger.debug("Writing %s data for client %s to storage directory: %s", key, client_id, storage_dir)       
        
        file_path = os.path.join(storage_dir, f"{key}_{client_id}_{node_id}.json")
        tmp_file = os.path.join(storage_dir, f".tmp_{key}_{client_id}_{node_id}_{int(os.getpid())}.json")

        if isinstance(data, multiprocessing.managers.DictProxy):
            serializable_data = dict(data)
        elif isinstance(data, multiprocessing.managers.ListProxy):
            serializable_data = list(data)
        else:
            serializable_data = data

        if key == "moveos":
            logger.info("Writing %s data for client %s to %s", key, client_id, file_path)
            logger.info("Data to write: %s", serializable_data)

        try:
            with open(tmp_file, "w") as tf:
                fcntl.flock(tf.fileno(), fcntl.LOCK_EX)
                json.dump(serializable_data, tf)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(tmp_file, file_path)
            logger.debug(f"Successfully wrote {key} data for client {client_id} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to write {key} data for client {client_id}: {e}")
            if tmp_file and os.path.exists(tmp_file):
                os.remove(tmp_file)

    def read_storage(self):
        """
        Load persisted EOS and movies data into memory on startup.
        """
        storage_dir = "./storage" + os.path.sep + str(self.node_id)
        if not os.path.exists(storage_dir):
            logger.error("Storage directory for node %s does not exist: %s", self.node_id, storage_dir)
            return
    
        logger.info("Loading persisted data from storage directory: %s", storage_dir)

        for filename in os.listdir(storage_dir):
            if filename.endswith(".flag"):
                continue
            if "tmp" in filename:
                logger.debug("Skipping temporary file: %s", filename)
                continue
            parts = filename[:-5].split("_")
            if len(parts) != 3:
                logger.warning("Invalid filename format in storage: %s", filename)
                continue
            
            key_type, client_id, node_id = parts

            if node_id != str(self.node_id):
                logger.info("Skipping file %s for node %s", filename, node_id)
                continue

            file_path = os.path.join(storage_dir, filename)
            try:
                with open(file_path, "r") as f: 
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    data = json.load(f)
                    self.update(key_type, data, client_id, node_id)
                logger.info("Loaded %s data for client %s from %s", key_type, client_id, file_path)
            except Exception as e:
                logger.error(f"Error reading {key_type} data from {file_path}: {e}")

        if self.at_least_one_table_ready():
            self.movies_handler_ready_event.set()
            logger.info("MoviesHandler is ready with at least one movies table populated.")
        

    def at_least_one_table_ready(self):
        """
        Check if at least one movies table is ready.
        """
        for client_id in self.movies.keys():
            if self.client_ready(client_id):
                logger.info("At least one movies table is ready for client %s", client_id)
                return True
        logger.info("No movies tables are ready yet.")
        return False

    def update(self, key_type, data, client_id, node_id):
        """
        Update the movies table or EOS flags based on the key type.
        """
        if key_type == "moveos":
            self.update_eos_flags(data, client_id, node_id)
        if key_type == "movies":
            self.update_movies_table(data, client_id, node_id)

    def update_eos_flags(self, data, client_id, node_id):
        """
        Update the EOS flags for the given client ID and node ID.
        """
        if client_id not in self.year_eos_flags:
            self.year_eos_flags[client_id] = self.manager.dict()
        for n_id, flag in data.items():
            if n_id not in self.year_eos_flags[client_id]:
                self.year_eos_flags[client_id][n_id] = flag
        logger.info("EOS flags updated for client %s: %s", client_id, self.year_eos_flags[client_id])

    def update_movies_table(self, data, client_id, node_id):
        """
        Update the movies table for the given client ID.
        """
        if client_id not in self.movies:
            self.movies[client_id] = self.manager.list()
        for movie in data:
            if movie not in self.movies[client_id]:
                self.movies[client_id].append(movie)
        logger.debug("Movies table updated for client %s: %s", client_id, self.movies[client_id])

        
