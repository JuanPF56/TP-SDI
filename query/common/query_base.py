import os

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

        self.source_queues = source_queue_key # every subclass should set this
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results_queue")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.node_name = os.getenv("NODE_NAME", "unknown")

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue
        )

        self.client_manager = ClientManager(
            expected_queues=self.source_queues,
            nodes_to_await=self.eos_to_await,
        )

        self.logger = get_logger(logger_name)

    def process(self):
        self.logger.info("Node is online")

        for key, value in self.config["DEFAULT"].items():
            self.logger.info(f"{key}: {value}")

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

    def callback(self, ch, method, properties, body, input_queue):
        """
        Callback method to be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
    def _calculate_and_publish_results(self, client_id, request_number):
        """
        Calculate and publish results. To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")