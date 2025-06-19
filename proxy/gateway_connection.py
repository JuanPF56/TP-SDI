# proxy/connections/gateway_connection.py

import socket
import threading
import time

RETRY_DELAY = 2  # seconds


class GatewayConnection:
    def __init__(self, host, port, encoded_client_id, logger):
        self.host = host
        self.port = port
        self.encoded_client_id = encoded_client_id
        self.logger = logger

        self.lock = threading.Lock()
        self.socket = None
        self._connect()

    def _connect(self):
        while True:
            try:
                self.socket = socket.create_connection(
                    (self.host, self.port), timeout=3
                )
                self.logger.info("Connected to gateway %s:%s", self.host, self.port)
                self.socket.sendall(self.encoded_client_id)
                return
            except Exception as e:
                self.logger.warning(
                    "Failed to connect to gateway %s:%s – %s. Retrying in %s sec.",
                    self.host,
                    self.port,
                    e,
                    RETRY_DELAY,
                )
                time.sleep(RETRY_DELAY)

    def reconnect(self):
        with self.lock:
            try:
                if self.socket:
                    self.socket.close()
            except Exception:
                pass
            self.logger.info(
                "Reconnecting gateway connection to %s:%s...", self.host, self.port
            )
            self._connect()

    def send(self, data: bytes):
        with self.lock:
            try:
                self.socket.sendall(data)
                return
            except Exception as e:
                self.logger.warning(
                    "Send failed to %s:%s – %s. Reconnecting...",
                    self.host,
                    self.port,
                    e,
                )
                self._connect()

    def receive(self, size: int, timeout=None) -> bytes:
        with self.lock:
            self.socket.settimeout(timeout)
            try:
                data = self.socket.recv(size)
                return data
            except Exception as e:
                self.logger.warning(
                    "Receive failed from %s:%s – %s. Reconnecting...",
                    self.host,
                    self.port,
                    e,
                )
                self._connect()
                raise

    def fileno(self):
        return self.socket.fileno()

    def get_socket(self):
        return self.socket
