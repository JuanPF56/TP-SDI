# common/client_state.py
from common.logger import get_logger

logger = get_logger("Client-State")

class ClientState:
    def __init__(self, client_id, request_id, nodes_to_await=1):
        self.client_id = client_id
        self.request_id = request_id
        self.eos_flags = {}  # key: queue_name, value: dict(node_id: bool)
        self.amount_of_eos = nodes_to_await
        
    def mark_eos(self, queue_name, node_id=1):
        if queue_name not in self.eos_flags:
            self.eos_flags[queue_name] = {}
        self.eos_flags[queue_name][node_id] = True
        
    def has_queue_received_eos(self, queue_name):
        """Check if the required amount of EOS messages has been received for a queue"""
        logger.debug(f"Checking if all EOS received for queue {queue_name}")
        eos_received = self.eos_flags.get(queue_name, {})
        logger.debug(f"EOS received: {eos_received}")
        if len(eos_received) == self.amount_of_eos:
            logger.debug(f"All EOS received for queue {queue_name}")
            return True
        else:
            logger.debug(f"Not all EOS received for queue {queue_name}")
            return False
            
    def has_queue_received_eos_from_node(self, queue_name, node_id):
        """Check if an EOS message has been received from a specific node for a queue"""
        logger.debug(f"Checking if EOS received from node {node_id} for queue {queue_name}")
        eos_received = self.eos_flags.get(queue_name, {})
        logger.debug(f"EOS received: {eos_received}")
        if eos_received.get(node_id, False):
            logger.debug(f"EOS received from node {node_id} for queue {queue_name}")
            return True
        else:
            logger.debug(f"EOS not received from node {node_id} for queue {queue_name}")
            return False
            
    def has_received_all_eos(self, queues):
        """Check if all required EOS messages have been received for all specified queues"""
        if not isinstance(queues, list):
            queues = [queues]
        for queue in queues:
            if not self.has_queue_received_eos(queue):
                logger.debug(f"Not all EOS received for queue {queue}")
                return False
        logger.debug("All EOS received for all queues")
        return True
        
    def get_eos_count(self, queue_name):
        """Returns the number of EOS messages received for a specific queue."""
        if queue_name in self.eos_flags:
            return len(self.eos_flags[queue_name])
        return 0
