import multiprocessing
import os
import json
import signal
import pika

from common.logger import get_logger
from common.leader_election import LeaderElector
logger = get_logger("Filter-Base")


from common.master import MasterLogic
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"


class FilterBase:
    def __init__(self, config):
        self.config = config
        self.main_source_queues = []
        self.source_queues = []
        self.target_queues = {}
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.election_port = int(os.getenv("ELECTION_PORT", 9001))
        self.peers = os.getenv("PEERS", "")  # del estilo: "filter_cleanup_1:9001,filter_cleanup_2:9002"
        self.node_name = os.getenv("NODE_NAME")
        self.elector = LeaderElector(self.node_id, self.peers, self.election_port, self._election_logic)

        self.rabbitmq_processor = None
        self.client_manager = None

        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def setup(self):
        """
        Método que cada subclase debe implementar para definir:
        - source_queues
        - target_queues
        - inicializar rabbitmq_processor
        """
        raise NotImplementedError()
    
    def _election_logic(self):
        pass

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            if self.rabbitmq_processor:
                logger.info("Stopping message consumption...")
                self.rabbitmq_processor.stop_consuming()
                logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.close()
                self.terminate_subprocesses()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queues,
        )

    def _initialize_master_logic(self):
        """
        Initialize the MasterLogic for load balancing and EOS handling.
        """
        self.manager = multiprocessing.Manager()
        self.master_logic = MasterLogic(
            config=self.config,
            manager=self.manager,
            node_id=self.node_id,
            nodes_of_type=self.nodes_of_type,
            clean_queues=self.main_source_queues,
            client_manager=self.client_manager,
        )
        self.master_logic.start()
        self.elector.start_election()


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
    
    def terminate_subprocesses(self):
        os.kill(self.master_logic.pid, signal.SIGINT)
        self.master_logic.join()
        self.manager.shutdown()