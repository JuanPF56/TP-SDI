import json
import os
import pika
import multiprocessing
import signal

from common.client_state import ClientState
from common.client_state_manager import ClientManager
from common.eos_handling import handle_eos
from common.mom import RabbitMQProcessor
from common.movies_handler import MoviesHandler

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
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.stopped = False

        self.client_manager = ClientManager(
            expected_queues=self.input_queue,
            nodes_to_await=self.eos_to_await,
        )

        # Initialize the RabbitMQProcessor
        self.rabbitmq_processor = RabbitMQProcessor(
            config, self.input_queue, self.output_queue
        )
        if not self.rabbitmq_processor.connect():
            self.log_error("Error connecting to RabbitMQ. Exiting...")
            return

        # Create a movie handler process to receive the movies tables
        self.manager = multiprocessing.Manager()
        self.movies_handler_ready = self.manager.Event()

        self.movies_handler = MoviesHandler(
            config=self.config,
            manager=self.manager,
            ready_event=self.movies_handler_ready,
            node_id=self.node_id,
            year_nodes_to_await=int(os.getenv("YEAR_NODES_TO_AWAIT", "1")),
        )

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        self.log_info("SIGTERM signal received. Closing connection...")
        try:
            self._close_connection()
        except Exception as e:
            self.log_info(f"Error closing connection: {e}")

    def process(self):
        # Start the process to receive the movies table
        self.movies_handler.start()

        # Start the loop to receive the batches
        self.receive_batch()


    def receive_batch(self):
        """
        Start the process to receive batches from the input queue.
        This method will wait for the movies table to be ready before
        starting to consume messages.
        """
        try:
            # Wait for the movies table to be ready
            self.movies_handler_ready.wait()

            self.log_info(
                "Movies table is ready for at least 1 client. Starting to receive batches..."
            )

            self.rabbitmq_processor.consume(self.process_batch)
        except KeyboardInterrupt:
            self.log_info("Shutting down gracefully...")
        except Exception as e:
            self.log_error(f"Error during consumption: {e}")
        finally:
            self._close_connection()

    def _close_connection(self):
        if not self.stopped:
            self.log_info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.stop_consuming()
            self.rabbitmq_processor.close()
            self.log_info("Connection closed.")
            os.kill(self.movies_handler.pid, signal.SIGINT)
            self.movies_handler.join()
            self.manager.shutdown()
            self.stopped = True

    def _handle_eos(self, queue_name, body, method, headers, client_state):
        self.log_debug(f"Received EOS from {queue_name}")
        handle_eos(
            body,
            self.node_id,
            queue_name,
            self.input_queue,
            headers,
            self.nodes_of_type,
            self.rabbitmq_processor,
            client_state,
            target_queues=self.output_queue,
        )
        self._free_resources(client_state)

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.input_queue):
            self.client_manager.remove_client(client_state.client_id)
            self.movies_handler.remove_movies_table(client_state.client_id)

    def process_batch(self, ch, method, properties, body, input_queue):
        """
        Process the incoming batch of messages.
        This method handles the end-of-stream (EOS) messages and performs the join operation.
        The join operation is defined in the perform_join method, which should be overridden
        by subclasses.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            headers = getattr(properties, "headers", {}) or {}
            current_client_id = headers.get("client_id")

            if not current_client_id:
                self.log_error("Missing client_id in headers")
                return

            client_state = self.client_manager.add_client(
                current_client_id, msg_type == EOS_TYPE
            )

            if msg_type == EOS_TYPE:
                self._handle_eos(input_queue, body, method, headers, client_state)
                return

            # Load the data from the incoming message
            try:
                decoded = json.loads(body)
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

            if not self.movies_handler.client_ready(current_client_id):
                # Put the message back to the queue for other nodes
                self.log_debug(
                    f"Movies table not ready for: client {current_client_id},"
                    + f"Publishing to input queue {input_queue}."
                )
                self.rabbitmq_processor.publish(
                    target=input_queue,
                    message=decoded,
                    msg_type=msg_type,
                    headers=headers,
                )
            else:
                self.log_debug(
                    f"Movies table ready for client {current_client_id},"
                    + "Processing batch..."
                )

                # Get the movies table for the client
                movies_table = self.movies_handler.get_movies_table(current_client_id)

                if not movies_table:
                    self.log_debug(
                        f"Movies table is empty for client {current_client_id},"
                    )
                    return

                # Build a set of movie IDs for fast lookup
                movies_by_id = {movie["id"]: movie for movie in movies_table}
                joined_data = self.perform_join(data, movies_by_id)

                if not joined_data:
                    self.log_debug("No matching movies found in the movies table.")
                else:
                    self.rabbitmq_processor.publish(
                        target=self.output_queue,
                        message=joined_data,
                        msg_type=msg_type,
                        headers=headers,
                    )
        except pika.exceptions.StreamLostError as e:
            self.log_info(f"Stream lost, reconnecting: {e}")
            self.rabbitmq_processor.reconnect_and_restart(self.process_batch)
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
