"""Proxy class for handling requests to multiple gateways."""

import os
import socket
import threading
import struct
import signal
import logging
import time

from gateways_listener import gateways_listener
from clients_listener import clients_listener

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UUID
import common.receiver as receiver
import common.sender as sender
from common.logger import get_logger

logger = get_logger("proxy")
logger.setLevel(logging.DEBUG)

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600


class Proxy:
    """
    Proxy server that forwards requests to multiple gateways.
    This class listens for incoming connections on a specified port and forwards
    requests to a list of defined gateways. If a gateway is unavailable,
    it tries the next one in the list. The class uses threading to handle
    multiple clients concurrently.
    """

    def __init__(self, config):
        self.config = config
        self.gateways = self._get_gateways()
        if not self.gateways:
            logger.error(
                "No gateways configured. Set the GATEWAYS environment variable."
            )
            return
        logger.info("Configured gateways: %s", self.gateways)

        # initialize the proxy socket
        self.port = int(self.config["DEFAULT"].get("PROXY_PORT", "9000"))
        self.proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.proxy_socket.bind(("", self.port))
        self.proxy_socket.listen(int(self.config["DEFAULT"].get("LISTEN_BACKLOG", 5)))
        logger.info("Proxy listening on port %s", self.port)
        self._was_closed = False

        # initialize the gateway connections
        self._gateways_connected = {}
        self._setup_gateways()
        logger.info("Proxy server initialized with gateways: %s", self.gateways)
        if not self._gateways_connected:
            logger.error("No gateways are reachable. Exiting proxy server.")
            return

        # initialize the client dictionary: client_id -> (client_socket, gateway_socket)
        self._connected_clients: dict[str, tuple[socket.socket, socket.socket]] = {}

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _get_gateways(self):
        gateways_str = os.getenv("GATEWAYS", "localhost:8000")
        return [
            (gateway.split(":")[0], int(gateway.split(":")[1]))
            for gateway in gateways_str.split(",")
            if ":" in gateway
        ]

    def _setup_gateways(self):
        """
        Set up the gateways by checking if they are reachable.
        """
        for host, port in self.gateways:
            try:
                socket.create_connection((host, port), timeout=2)
                self._gateways_connected[(host, port)] = True
                logger.info("Gateway %s:%s is reachable", host, port)
            except Exception as e:
                logger.error("Could not connect to gateway %s:%s - %s", host, port, e)

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping proxy...")
        self._stop_proxy()

    def _stop_proxy(self):
        logger.info("Stopping proxy...")
        self._was_closed = True

        # Close all client <-> gateway connections
        for client_id, (client_sock, gateway_sock) in self._connected_clients.items():
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            finally:
                client_sock.close()

            try:
                gateway_sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            finally:
                gateway_sock.close()

            logger.info("Closed connection for client: %s", client_id)

        self._connected_clients.clear()

        # Close the proxy listening socket
        try:
            self.proxy_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        finally:
            self.proxy_socket.close()

        logger.info("Proxy stopped.")

    def run(self):
        """
        Run the proxy server.
        It listens on the specified port and handles incoming client connections.
        """
        threading.Thread(target=gateways_listener, args=(self,), daemon=True).start()
        threading.Thread(target=clients_listener, args=(self,), daemon=True).start()
        while not self._was_closed:
            time.sleep(1)
