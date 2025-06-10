import os
import json
import signal
import pika

from common.logger import get_logger

logger = get_logger("Filter-Base")


from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"


class FilterBase:
    def __init__(self, config):
        self.config = config
        self.source_queues = []
        self.target_queues = {}
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.rabbitmq_processor = None
        self.client_manager = None
        self.node_name = os.getenv("NODE_NAME", "unknown")

        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def setup(self):
        """
        Método que cada subclase debe implementar para definir:
        - source_queues
        - target_queues
        - inicializar rabbitmq_processor
        """
        raise NotImplementedError()

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            if self.rabbitmq_processor:
                logger.info("Stopping message consumption...")
                self.rabbitmq_processor.stop_consuming()
                logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queues,
        )

    def run_consumer(self):
        logger.info("Node is online")
        logger.info("Configuration loaded successfully")
        for key, value in self.config["DEFAULT"].items():
            logger.info("Config: %s: %s", key, value)

        if not self.rabbitmq_processor.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()

        except Exception as e:
            logger.error("Error during consumption: %s", e)
            self.rabbitmq_processor.stop_consuming()

        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")

    def _get_message_type(self, properties):
        return properties.type if properties and properties.type else "UNKNOWN"

    def _decode_body(self, body, queue_name):
        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            logger.error("JSON decode error in message from %s: %s", queue_name, e)
            return None

    def process(self):
        raise NotImplementedError("Subclasses should implement this.")
