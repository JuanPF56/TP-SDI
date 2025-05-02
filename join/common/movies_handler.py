import json
import multiprocessing
import signal

from common import logger
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"

class MoviesHandler(multiprocessing.Process):
    def __init__(self, config, manager, ready_event, node_id, year_nodes_to_await):
        """
        Initialize the MoviesHandler class with the given configuration and manager.
        """
        super().__init__(target=self.run)
        self.config = config
        self.rabbitmq_processor = RabbitMQProcessor(
            config, [], [],
            config["DEFAULT"].get("rabbitmq_host", "rabbitmq"),
            source_exchange=config["DEFAULT"].get("movies_exchange", "movies_arg_post_2000")
        )
        self.rabbitmq_processor.connect()
        self.manager = manager
        self.movies = self.manager.dict()

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)

        self.node_id = node_id
        # TODO: Handle for each client
        self.year_eos_flags = self.manager.dict()
        self.year_nodes_to_await = year_nodes_to_await
        self.ready = False
        self.movies_table_ready = ready_event

    def __handleSigterm(self, signum, frame):
        self.rabbitmq_processor.stop_consuming()
        self.rabbitmq_processor.close()
    
    def run(self):
        """
        Start the process to receive the movies from the broadcast exchange.
        This method will create a new queue and bind it to the exchange.
        It will then consume messages from the queue and populate the movies table.
        Once all nodes have sent EOS messages, it will notify that the movies table is ready.
        """

        def callback(ch, method, properties, body, queue_name):
            try:
                msg_type = properties.type if properties and properties.type else "UNKNOWN"
                if msg_type == EOS_TYPE:
                    try:
                        data = json.loads(body)
                        node_id = data.get("node_id")
                        client_id = 1 # TODO: Handle for each client
                    except json.JSONDecodeError:
                        self.log_error("Failed to decode EOS message")
                        return
                    if client_id not in self.year_eos_flags:
                        self.year_eos_flags[client_id] = {}
                    if node_id not in self.year_eos_flags[client_id]:
                        self.year_eos_flags[client_id][node_id] = True
                        self.log_debug(f"EOS received for node {node_id}.")
                    if not self.ready and self.client_ready(client_id):
                        self.ready = True
                        self.log_info("One table is ready for at least 1 client. Notifying...")
                        self.movies_table_ready.set()            
                else:
                    try:
                        data = json.loads(body)
                        # TODO: Handle for each client
                        client_id = 1
                        movies_data = data
                    except json.JSONDecodeError:
                        self.log_error(f"Error decoding JSON: {body}")
                        return
                    logger.debug(f"Received message: {movies_data}")
                    if client_id not in self.movies:
                        self.movies[client_id] = self.manager.list()
                    for movie in movies_data:
                        new_movie = {
                            "id" : str(movie["id"]),
                            "original_title": movie["original_title"],
                        }
                        self.movies[client_id].append(new_movie)
                    logger.debug(f"Movies table updated for client {client_id}: {self.movies[client_id]}")
            except Exception as e:
                self.log_error(f"Error processing message: {e}")
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
        if client_id not in self.movies:
            return None
        return list(self.movies[client_id])
    
    def remove_movies_table(self, client_id):
        """
        Remove the movies table for the given client ID.
        """
        if client_id in self.movies:
            del self.movies[client_id]
            logger.debug(f"Movies table removed for client {client_id}.")
        else:
            logger.error(f"No movies table found for client {client_id}.")

    