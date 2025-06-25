import fcntl
import json
import multiprocessing
import os
import signal

from common.duplicate_handler import DuplicateHandler
from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientManager

from common.logger import get_logger

EOS_TYPE = "EOS"


class QueryBase:
    """
    Clase base para las queries.
    """

    def __init__(self, config, source_queue_key, logger_name):
        self.config = config

        self.source_queues = source_queue_key  # every subclass should set this
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results_queue")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.recovery_mode = os.path.exists(f"./storage/recovery_mode.flag")
        self.q_id = int(self.node_name)[-1] if self.node_name.isdigit() else 0

        self.duplicate_handler = DuplicateHandler(self.q_id)

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue,
        )

        self.manager = multiprocessing.Manager()

        self.client_manager = ClientManager(
            self.source_queues,
            manager=self.manager,
            nodes_to_await=self.eos_to_await,
        )

        self.logger = get_logger(logger_name)

        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            if self.rabbitmq_processor:
                self.logger.info("Stopping message consumption...")
                self.rabbitmq_processor.stop_consuming()
                self.logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.close()
        except Exception as e:
            self.logger.error("Error closing connection: %s", e)

    def process(self):
        self.logger.info("Node is online")

        if self.recovery_mode:
            self.logger.info("Recovery mode is enabled. Attempting to recover state...")
            self.recover_state()
            self.duplicate_handler.read_storage()

        for key, value in self.config["DEFAULT"].items():
            self.logger.info("%s: %s", key, value)

        if not self.rabbitmq_processor.connect():
            self.logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            self.logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        finally:
            self.logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            self.logger.info("Connection closed.")

    def _write_data_to_file(self, client_id, data, key):
        """
        Write the data state of a client to a file.
        """
        storage_dir = f"./storage/{self.q_id}"
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir, exist_ok=True)
        file_path = os.path.join(storage_dir, f"{key}_{client_id}.json")
        temp_file_path = os.path.join(storage_dir, f".tmp_{key}_{client_id}.json")

        try:
            with open(temp_file_path, 'w') as tf:
                fcntl.flock(tf.fileno(), fcntl.LOCK_EX)
                json.dump(data, tf, indent=4)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(temp_file_path, file_path)
            self.logger.debug("Successfully wrote data to %s", file_path)
        except Exception as e:
            self.logger.error("Failed to write data: %s", e)
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.debug("Removed temporary file: %s", temp_file_path)

    def write_eos_to_file(self, client_id):
        """
        Write the EOS state of a client to a file.
        """
        client_eos = self.client_manager.get_client_eos(client_id)
        if not client_eos:
            self.logger.warning("No EOS data found for client %s", client_id)
            return
        self._write_data_to_file(client_id, client_eos, "eos")

    def recover_state(self):
        storage_dir = "./storage" + os.path.sep + str(self.q_id)
        if not os.path.exists(storage_dir):
            self.logger.info("No previous state found for query %s", self.q_id)
            return
        self.logger.info("Recovering state for query %s from %s", self.q_id, storage_dir)

        for filename in os.listdir(storage_dir):
            if "tmp" in filename:
                continue
            if filename.endswith(".json"):
                parts = filename.split[:-5].split("_")
                client_id = parts[-1]
                key = "_".join(parts[:-1])

                try:
                    with open(os.path.join(storage_dir, filename), "r") as f:
                        data = json.load(f)
                        if key == "eos":
                            self.update_eos_state(client_id, data)
                        else:
                            self.update_data(client_id, key, data)
                    self.logger.info("Recovered state for client %s from %s", client_id, filename)
                except Exception as e:
                    self.logger.error("Failed to recover state from %s: %s", filename, e)
    
    def update_eos_state(self, client_id, data):
        """
        Update the EOS state of a client.
        """
        if not isinstance(data, dict):
            self.logger.error("Invalid EOS data format for client %s: %s", client_id, data)
            return
        self.client_manager.update_eos_state(client_id, data)

    def update_data(self, client_id, key, data):
        """
        Update the data state of a client. To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")        

    def callback(self, ch, method, properties, body, input_queue):
        """
        Callback method to be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
    def _calculate_and_publish_results(self, client_id):
        """
        Calculate and publish results. To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
