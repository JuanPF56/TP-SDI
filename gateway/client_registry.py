from threading import Lock
from typing import List
from connected_client import ConnectedClient

class ClientRegistry:
    def __init__(self):
        self._clients: List[ConnectedClient] = []
        self._lock = Lock()

    def add(self, client):
        with self._lock:
            self._clients.append(client)

    def remove(self, client):
        with self._lock:
            if client in self._clients:
                self._clients.remove(client)

    def get_by_uuid(self, uuid_str: str) -> ConnectedClient | None:
        with self._lock:
            for client in self._clients:
                if client.get_client_id() == uuid_str:
                    return client
            return None
    
    def get_all(self) -> List[ConnectedClient]:
        with self._lock:
            return list(self._clients)  # Return a shallow copy

    def count(self) -> int:
        with self._lock:
            return len(self._clients)

    def clear(self):
        with self._lock:
            for client in self._clients:
                client._stop_client()
            self._clients.clear()
