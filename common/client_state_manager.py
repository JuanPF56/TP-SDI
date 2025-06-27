# common/client_manager.py
import json
import multiprocessing
from common import logger

logger = logger.get_logger("Client-Manager")

EOS_TYPE = "EOS"

class ClientManager:
    def __init__(self, expected_queues, manager, nodes_to_await=1):

        self.manager = manager  # Shared manager for multiprocessing
        self.clients = self.manager.dict()  # Dictionary to hold client states
        self.expected_queues = expected_queues
        self.nodes_to_await = nodes_to_await

    def add_client(self, client_id):
        """
        Add a new client or retrieve an existing one.
        """
        if client_id not in self.clients:
            self.clients[client_id] = self.manager.dict()
        return self.clients[client_id]

    def remove_client(self, client_id):
        """
        Remove a client from the manager.
        """
        if client_id in self.clients:
            del self.clients[client_id]

    def get_clients(self):
        """
        Get all clients managed by this ClientManager.
        Returns a dictionary of client IDs and their EOS flags.
        """
        return dict({client_id: self._unwrap_dictproxy(eos_flags)
                     for client_id, eos_flags in self.clients.items()})

    def get_eos_flags(self, client_id):
        """
        Get the EOS flags for a specific client.
        """
        if client_id in self.clients:
            return dict(self._unwrap_dictproxy(self.clients[client_id]))
        return {}
    
    def mark_eos(self, client_id, queue_name, node_id=1):
        """
        Mark that an EOS message has been received for a specific client and queue.
        """
        if client_id not in self.clients:
            self.add_client(client_id)
        if queue_name not in self.clients[client_id].keys():
            self.clients[client_id][queue_name] = self.manager.dict()
        self.clients[client_id][queue_name][node_id] = True
        logger.info(
            "EOS marked for client %s, dict: %s", client_id, self._unwrap_dictproxy(self.clients[client_id])
        )
    
    def has_queue_received_eos(self, client_id, queue_name):
        """
        Check if the required amount of EOS messages has been received for a client's queue.
        """
        logger.debug("Current EOS flags: %s", self.clients.get(client_id, {}))
        logger.debug("Checking if all EOS received for queue %s", queue_name)
        eos_received = self.clients.get(client_id, {}).get(queue_name, {})
        logger.debug(
            "EOS received: %s, amount expected: %s", eos_received, self.nodes_to_await
        )
        if len(eos_received) == self.nodes_to_await:
            logger.debug("All EOS received for queue %s", queue_name)
            return True
        else:
            logger.debug("Not all EOS received for queue %s", queue_name)
            return False
        
    def has_queue_received_eos_from_node(self, client_id, queue_name, node_id):
        """
        Check if an EOS message has been received from a specific node for a client's queue.
        """
        eos_received = self.clients.get(client_id, {}).get(queue_name, {})
        logger.info("Checking if EOS received from node %s for queue %s", node_id, queue_name)
        if eos_received.get(node_id, False):
            return True
        else:
            return False
        
    def has_received_all_eos(self, client_id, queues):
        """
        Check if all required EOS messages have been received for all specified queues of a client.
        """
        logger.debug("Checking if all EOS received for queues: %s", queues)
        if not isinstance(queues, list):
            queues = [queues]
        for queue in queues:
            if not self.has_queue_received_eos(client_id, queue):
                logger.debug("Not all EOS received for queue %s", queue)
                return False
        return True
    
    def get_eos_count(self, client_id, queue_name):
        """
        Get the count of EOS messages received for a specific queue of a client.
        """
        eos_received = self.clients.get(client_id, {}).get(queue_name, {})
        return len(eos_received)

    def handle_eos(self, body, node_id, incoming_queue, source_queues, headers,
                       rabbitmq_processor, target_queues=None, target_exchanges=None):
        """
        Handle the end of stream (EOS) message received from a node.
        This method checks if the EOS message is valid, marks it, and checks if all nodes have sent EOS.

        Parameters:
        - body: The body of the EOS message.
        - node_id: The ID of the current node.
        - incoming_queue: The input queue from which the EOS message was received.
        - source_queues: The list of source queues to check for EOS.
        - headers: The headers of the message.
        - rabbitmq_processor: The RabbitMQ processor instance.
        - target_queues: The target queues to send the EOS message to.
        - target_exchanges: The target exchanges to send the EOS message to.
        """
        try:
            data = json.loads(body)
            n_id = data.get("node_id", 1)  # ID of the node that sent the EOS message
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return

        client_id = headers.get("client_id")
        if client_id is None:
            logger.error("Missing client_id in headers")
            return
        
        logger.info("EOS received for node %s in queue %s, eos flags: %s",
                    n_id, incoming_queue, self.get_eos_flags(client_id)
                )

        if not self.has_queue_received_eos_from_node(client_id, incoming_queue, n_id):
            self.mark_eos(client_id, incoming_queue, n_id)
            self.check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                                target_queues, target_exchanges)
        
        logger.info(f"EOS received for node {n_id} from input queue {incoming_queue}")

    def check_eos_flags(self, headers, node_id, source_queues, rabbitmq_processor,
                        target_queues=None, target_exchanges=None):
        """
        Check if all nodes have sent EOS and propagate to output queues.
        """
        logger.info("Checking EOS flags for client_id: %s", headers.get("client_id"))
        client_id = headers.get("client_id")
        if client_id is None:
            logger.error("Missing client_id in headers")
            return
        
        if self.has_received_all_eos(client_id, source_queues):
            logger.info(f"All nodes have sent EOS. Sending EOS to output queues. Dict: {self.get_eos_flags(client_id)}")
            self.send_eos(headers, node_id, rabbitmq_processor, target_queues, target_exchanges)
        else:
            logger.debug("Not all nodes have sent EOS yet. Waiting...")

    def send_eos(self, headers, node_id, rabbitmq_processor,
                 target_queues=None, target_exchanges=None):
        """
        Propagate the end of stream (EOS) to all output queues and exchanges.
        """
        logger.info(f"Sending EOS to output queue and exchange. Target queues: {target_queues}, target exchanges: {target_exchanges}")
        if target_queues is not None:
            if not isinstance(target_queues, list):
                targets = [target_queues]
            else:
                targets = target_queues
            for target_queue in targets:
                rabbitmq_processor.publish(
                    target=target_queue,
                    message={"node_id": node_id},
                    msg_type=EOS_TYPE,
                    headers=headers,
                    priority=1
                )
                logger.info(f"EOS message sent to {target_queue}")
        if target_exchanges is not None:
            if not isinstance(target_exchanges, list):
                targets = [target_exchanges]
            else:
                targets = target_exchanges
            for target_exchange in targets:
                rabbitmq_processor.publish(
                    target=target_exchange,
                    message={"node_id": node_id},
                    msg_type=EOS_TYPE,
                    exchange=True,
                    headers=headers,
                    priority=1
                )
                logger.info(f"EOS message sent to {target_exchange}")

    
    def _unwrap_dictproxy(self, d, level=0):
        indent = "  " * level
        from common.logger import get_logger  # or use your logger
        logger = get_logger("Client-Manager-Unwrap")

        if isinstance(d, multiprocessing.managers.DictProxy):
            logger.debug(f"{indent}Unwrapping DictProxy at level {level} with keys: {list(d.keys())}")
            result = {k: self._unwrap_dictproxy(v, level + 1) for k, v in d.items()}
            logger.debug(f"{indent}Unwrapped to dict at level {level}: {result}")
            return result
        elif isinstance(d, dict):
            logger.debug(f"{indent}Unwrapping dict at level {level} with keys: {list(d.keys())}")
            result = {k: self._unwrap_dictproxy(v, level + 1) for k, v in d.items()}
            logger.debug(f"{indent}Unwrapped dict at level {level}: {result}")
            return result
        else:
            logger.debug(f"{indent}Reached leaf at level {level}: {d} (type: {type(d)})")
            return d

    def update_eos_state(self, client_id, data):
        """
        Update the EOS state for a client based on the provided data.
        This method is used to update the EOS flags for a specific client.
        """
        if client_id not in self.clients:
            self.add_client(client_id)
        for queue_name, node_ids in data.items():
            if queue_name not in self.clients[client_id]:
                self.clients[client_id][queue_name] = self.manager.dict()
            for node_id in node_ids:
                self.clients[client_id][queue_name][node_id] = True
        logger.info(
            "EOS state updated for client %s, dict: %s", client_id, self._unwrap_dictproxy(self.clients[client_id])
        )