# common/client_state.py
import os
from common.logger import get_logger
import json
import tempfile

logger = get_logger("Client-State")


class ClientState:
    def __init__(self, client_id, nodes_to_await=1):
        self.client_id = client_id
        self.eos_flags = {}  # Dictionary to hold EOS flags for each queue
        self.amount_of_eos = nodes_to_await

    def get_eos_flags(self):
        """
        Get the EOS flags for the client.
        """
        return self.eos_flags

    def mark_eos(self, queue_name, node_id=1):
        """
        Mark that an EOS message has been received for a specific queue and node
        """
        if queue_name not in self.eos_flags.keys():
            self.eos_flags[queue_name] = {}
        self.eos_flags[queue_name][node_id] = True
        logger.debug(
            "EOS marked for client %s, dict: %s", self.client_id, self.eos_flags
        )

    def has_queue_received_eos(self, queue_name):
        """
        Check if the required amount of EOS messages has been received for a queue
        """
        logger.debug("Current EOS flags: %s", self.eos_flags)
        logger.debug("Checking if all EOS received for queue %s", queue_name)
        eos_received = self.eos_flags.get(queue_name, {})
        logger.debug(
            "EOS received: %s, amount expected: %s", eos_received, self.amount_of_eos
        )
        if len(eos_received) == self.amount_of_eos:
            logger.debug("All EOS received for queue %s", queue_name)
            return True
        else:
            logger.debug("Not all EOS received for queue %s", queue_name)
            return False

    def has_queue_received_eos_from_node(self, queue_name, node_id):
        """
        Check if an EOS message has been received from a specific node for a queue
        """
        eos_received = self.eos_flags.get(queue_name, {})
        if eos_received.get(node_id, False):
            return True
        else:
            return False

    def has_received_all_eos(self, queues):
        """
        Check if all required EOS messages have been received for all specified queues
        """
        logger.debug("Checking if all EOS received for queues: %s", queues)
        if not isinstance(queues, list):
            queues = [queues]
        for queue in queues:
            if not self.has_queue_received_eos(queue):
                logger.debug("Not all EOS received for queue %s", queue)
                return False
        return True

    def get_eos_count(self, queue_name):
        """
        Returns the number of EOS messages received for a specific queue.
        """
        if queue_name in self.eos_flags:
            return len(self.eos_flags[queue_name])
        return 0
