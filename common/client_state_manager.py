# common/client_manager.py
from common.client_state import ClientState

class ClientManager:
    def __init__(self, expected_queues, nodes_to_await=1):
    
        self.clients = {}  # key: (client_id, request_id), value: ClientState
        self.expected_queues = expected_queues
        self.nodes_to_await = nodes_to_await

    def add_client(self, client_id, request_id) -> ClientState:
        key = (client_id, request_id)
        if key not in self.clients:
            self.clients[key] = ClientState(client_id, self.nodes_to_await)
        return self.clients[key]
    
    def remove_client(self, client):
        if client in self.clients:
            del self.clients[client]