# common/client_manager.py
from common.client_state import ClientState

class ClientManager:
    def __init__(self, expected_queues):
        self.clients = {}  # key: client_id, value: ClientState
        self.expected_queues = expected_queues

    def get_or_create(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientState(client_id)
        return self.clients[client_id]

    def mark_eos(self, client_id, queue_name):
        client = self.get_or_create(client_id)
        client.mark_eos(queue_name)

    def client_received_all_eos(self, client_id):
        client = self.get_or_create(client_id)
        return client.all_queues_eos(self.expected_queues)
