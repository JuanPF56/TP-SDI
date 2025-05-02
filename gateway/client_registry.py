from threading import Lock
from typing import Dict, Optional
from connected_client import ConnectedClient

class ClientRegistry:
    def __init__(self):
        self._clients: Dict[str, ConnectedClient] = {}
        self._lock = Lock()

    def add(self, client: ConnectedClient):
        with self._lock:
            self._clients[client.get_client_id()] = client

    def remove(self, client: ConnectedClient):
        with self._lock:
            client_id = client.get_client_id()
            if client_id in self._clients:
                del self._clients[client_id]

    def get_by_uuid(self, uuid_str: str) -> Optional[ConnectedClient]:
        with self._lock:
            return self._clients.get(uuid_str)

    def get_all(self) -> Dict[str, ConnectedClient]:
        with self._lock:
            return dict(self._clients)  # Return a shallow copy

    def count(self) -> int:
        with self._lock:
            return len(self._clients)

    def clear(self):
        with self._lock:
            for client in self._clients.values():
                client._stop_client()
            self._clients.clear()
