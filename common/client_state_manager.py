# common/client_manager.py
import multiprocessing
import os
from common import logger
from common.client_state import ClientState
import json
import tempfile

from common.eos_handling import check_eos_flags
from common.mom import RabbitMQProcessor

logger = logger.get_logger("Client-Manager")

class ClientManager:
    def __init__(self, expected_queues, nodes_to_await=1):

        self.clients = {}  # Dictionary to hold client_id to ClientState mapping
        self.expected_queues = expected_queues
        self.nodes_to_await = nodes_to_await

    def add_client(self, client_id, is_eos=False) -> ClientState:
        """
        Add a new client or retrieve an existing one.
        """
        if client_id not in self.clients:
            if is_eos:
                return None
            self.clients[client_id] = ClientState(client_id, self.nodes_to_await)
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
        Returns a dictionary of client_id to ClientState.
        """
        return self.clients.copy()