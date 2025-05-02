# common/client_manager.py
from common.client_state import ClientState

class ClientManager:
    def __init__(self, expected_queues):
        self.clients = {}  # key: client_id, value: ClientState
        self.expected_queues = expected_queues

    def add_client(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientState(client_id)
        return self.clients[client_id]

    def get_client(self, client_id) -> ClientState:
        return self.clients.get(client_id)
    
    def remove_client(self, client_id):
        if client_id in self.clients:
            del self.clients[client_id]