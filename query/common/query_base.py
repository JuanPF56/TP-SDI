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