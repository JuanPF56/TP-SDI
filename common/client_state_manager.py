# common/client_manager.py
from common.client_state import ClientState


class ClientManager:
    def __init__(self, expected_queues, nodes_to_await=1):

        self.clients = {}  # key: (client_id), value: ClientState
        self.expected_queues = expected_queues
        self.nodes_to_await = nodes_to_await

    def add_client(self, client_id, is_eos=False) -> ClientState:
        """
        Add a new client or return the existing client state.
        If is_eos is True, it will not create a new client state.
        """
        key = client_id
        if key not in self.clients:
            if is_eos:
                return None
            else:
                self.clients[key] = ClientState(client_id, self.nodes_to_await)
        return self.clients[key]

    def remove_client(self, client_id):
        """
        Remove a client from the manager.
        This will delete the client state from the manager.
        """
        key = client_id
        if key in self.clients:
            del self.clients[key]

    def get_clients(self):
        """
        Get all clients managed by this ClientManager.
        Returns a dictionary of client_id to ClientState.
        """
        return self.clients
