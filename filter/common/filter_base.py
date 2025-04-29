import os

from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS" 

class FilterBase:
    def __init__(self, config):
        self.config = config
        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))
        self.source_queues = []
        self.target_queues = {}
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.rabbitmq_processor = None
        self._eos_flags = {}

    def setup(self):
        """
        Método que cada subclase debe implementar para definir:
        - source_queues
        - target_queues
        - inicializar rabbitmq_processor
        """
        raise NotImplementedError()

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queues
        )

    def process(self):
        raise NotImplementedError("Subclasses should implement this.")