import json
import os
import pika
import multiprocessing
import signal

from common.mom import RabbitMQProcessor
from join.common.movies_handler import MoviesHandler

EOS_TYPE = "EOS"

class JoinBase:
    def __init__(self, config):
        """
        Initialize the JoinBase class with the given configuration.
        """
        self.config = config
        
        # Get the clean batch queue name from the config
        self.input_queue = self.config["DEFAULT"].get("input_queue", "input_queue")
        self.output_queue = self.config["DEFAULT"].get("output_queue", "output_queue")

        # Get the environment variables for node ID and nodes to await
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))

        self._eos_flags = {}

        # Initialize the RabbitMQProcessor
        self.rabbitmq_processor = RabbitMQProcessor(config, self.input_queue, self.output_queue)
        self.rabbitmq_processor.connect()

        # Create a movie handler process to receive the movies tables
        self.manager = multiprocessing.Manager()
        self.movies_handler_ready = self.manager.Event()

        self.movies_handler = MoviesHandler(
            config=self.config,
            manager=self.manager,
            ready_event=self.movies_table_ready,
            node_id=self.node_id,
            year_nodes_to_await= int(os.getenv("YEAR_NODES_TO_AWAIT", "1"))
        )

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)


    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            self.rabbitmq_processor.stop_consuming()
            self.rabbitmq_processor.close()
        except Exception as e:
            self.log_info(f"Error closing connection: {e}")
        finally:
            self.manager.shutdown()
            os.kill(self.movies_handler.pid, signal.SIGTERM)
            self.movies_handler.join()

    def process(self):
        # Start the process to receive the movies table
        self.movies_handler.start()

        # Start the loop to receive the batches
        self.receive_batch()

        self.movies_handler.join()

    def receive_batch(self):
        """
        Start the process to receive batches from the input queue.
        This method will wait for the movies table to be ready before 
        starting to consume messages.
        """
        # Wait for the movies table to be ready
        self.movies_handler_ready.wait()

        self.log_info("Movies table is ready for at least 1 client. Starting to receive batches...")

        self.rabbitmq_processor.consume(self.process_batch)


    def process_batch(self, ch, method, properties, body, input_queue):
        """
        Process the incoming batch of messages.
        This method handles the end-of-stream (EOS) messages and performs the join operation.
        The join operation is defined in the perform_join method, which should be overridden
        by subclasses.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            if msg_type == "EOS":
                try:
                    data = json.loads(body)
                    node_id = data.get("node_id")
                    count = data.get("count")
                    client_id = 1 # TODO: Handle for each client
                except json.JSONDecodeError:
                    self.log_debug("Failed to decode EOS message")
                    return
                self.log_debug(f"EOS message received: {data}")
                if node_id not in self._eos_flags:
                    count += 1
                    self._eos_flags[node_id] = True
                    self.log_debug(f"EOS received for node {node_id}.")
                if len(self._eos_flags) == int(self.eos_to_await):
                    self.log_info("All nodes have sent EOS. Sending EOS to output queue.")
                    self.rabbitmq_processor.publish(
                        target=self.output_queue,
                        message={"node_id": self.node_id, "count": 0},
                        msg_type=msg_type
                    )
                self.log_debug(f"EOS count for node {node_id}: {count}")
                self.log_debug(f"Nodes of type: {self.nodes_of_type}")
                # If this isn't the last node, put the EOS message back to the queue for other nodes
                if count < self.nodes_of_type:
                    self.log_debug(f"Sending EOS back to input queue for node {node_id}.")
                    # Put the EOS message back to the queue for other nodes
                    self.rabbitmq_processor.publish(
                        target=input_queue,
                        message={"node_id": node_id, "count": count},
                        msg_type=msg_type
                    )
            # Load the data from the incoming message
            try:
                decoded = json.loads(body)
                client_id = 1 # TODO: Handle for each client
                if isinstance(decoded, list):
                    self.log_debug(f"Received list: {decoded}")
                    data = decoded
                elif isinstance(decoded, dict):
                    self.log_debug(f"Received dict: {decoded}")
                    data = [decoded]
                else:
                    self.log_warning(f"Unexpected JSON format: {decoded}")
                    return
            except json.JSONDecodeError as e:
                self.log_error(f"Error decoding JSON: {e}")
                return
            
            if not self.movies_handler.client_ready(client_id):
                # Put the message back to the queue for other nodes
                self.log_debug(f"Movies table not ready for client {client_id}. Putting message back to queue.")
                self.rabbitmq_processor.publish(
                    target=input_queue,
                    message=body,
                    msg_type=msg_type
                )
            else:
                self.log_debug(f"Movies table ready for client {client_id}. Processing batch.")

                # Get the movies table for the client
                movies_table = self.movies_handler.get_movies_table(client_id)

                # Build a set of movie IDs for fast lookup
                movies_by_id = {movie["id"]: movie for movie in movies_table}
                joined_data = self.perform_join(data, movies_by_id)            

                if not joined_data:
                    self.log_debug("No matching movies found in the movies table.")
                    return
                
                self.rabbitmq_processor.publish(
                    target=self.output_queue,
                    message=joined_data,
                    msg_type=msg_type
                )
        except pika.exceptions.StreamLostError as e:
            self.log_info(f"Stream lost, reconnecting: {e}")
            self.rabbitmq_processor.stop_consuming()
            self.rabbitmq_processor.close()
            self.rabbitmq_processor.connect()
            self.receive_batch()
        except Exception as e:
            self.log_error(f"[ERROR] Unexpected error in process_batch: {e}")
        finally:
            self.rabbitmq_processor.acknowledge(method)
    
    def perform_join(self, data, movies_by_id):
        """
        Perform the join operation between the incoming data and the movies table.
        This method should be overridden by subclasses to implement specific join logic.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def log_info(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_error(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_debug(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_warning(self, message):
        raise NotImplementedError("Subclasses must implement this method")