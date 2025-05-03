# common/client_state.py
from common.logger import get_logger

logger = get_logger("Client-State")

class ClientState:
    def __init__(self, client_id, nodes_to_await=1):
        self.client_id = client_id
        self.eos_flags = {}  # key: queue_name, value: dict(node_id: bool)  
        self.amount_of_eos = nodes_to_await

    def mark_eos(self, queue_name, node_id = 1):
        if queue_name not in self.eos_flags:
            self.eos_flags[queue_name] = {}
        self.eos_flags[queue_name][node_id] = True

    def has_queue_received_eos(self, queue_name): ## chequear if amount of eos is correct
        logger.info(f"Checking if all EOS received for queue {queue_name}")
        eos_received = self.eos_flags.get(queue_name, {})
        logger.info(f"EOS received: {eos_received}")
        if len(eos_received) == self.amount_of_eos:
            logger.info(f"All EOS received for queue {queue_name}")
            return True
    def has_queue_received_eos_from_node(self, queue_name, node_id):
        logger.info(f"Checking if EOS received from node {node_id} for queue {queue_name}")
        eos_received = self.eos_flags.get(queue_name, {})
        logger.info(f"EOS received: {eos_received}")
        if eos_received.get(node_id, False):
            logger.info(f"EOS received from node {node_id} for queue {queue_name}")
            return True
        else:
            logger.info(f"EOS not received from node {node_id} for queue {queue_name}")

        
    def has_received_all_eos(self, queues):
        logger.info(f"Checking if all EOS received for queues {queues}")
        if not isinstance(queues, list):
            queues = [queues]
        for queue in queues:
            if not self.has_queue_received_eos(queue):
                logger.info(f"Not all EOS received for queue {queue}")
                return False
        logger.info("All EOS received for all queues")
        return True    

