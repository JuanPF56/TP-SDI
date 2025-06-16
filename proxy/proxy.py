"""Proxy class for handling requests to multiple gateways."""

import os
import socket
import threading
from common.logger import get_logger

logger = get_logger("proxy")


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

        self.port = int(config.get("PROXY_PORT", "9000"))
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("", self.port))
        server.listen()
        logger.info("Proxy listening on port %s", self.port)

    def _get_gateways(self):
        gateways_str = os.getenv("GATEWAYS", "localhost:8000")
        return [
            (gateway.split(":")[0], int(gateway.split(":")[1]))
            for gateway in gateways_str.split(",")
            if ":" in gateway
        ]

    def _forward(self, source, destination):
        while True:
            try:
                data = source.recv(4096)
                if not data:
                    break
                destination.sendall(data)
            except Exception as e:
                logger.error("Error forwarding data: %s", e)
                break

    def _handle_client(self, client_socket):
        for host, port in self.gateways:
            try:
                gateway_socket = socket.create_connection((host, port), timeout=2)
                logger.info("Connected to %s:%s", host, port)
                threading.Thread(
                    target=self._forward, args=(client_socket, gateway_socket)
                ).start()
                threading.Thread(
                    target=self._forward, args=(gateway_socket, client_socket)
                ).start()
                return
            except Exception as e:
                logger.error("Could not connect to %s:%s - %s", host, port, e)
        client_socket.close()

    def run(self):
        """
        Run the proxy server.
        It listens on the specified port and handles incoming client connections.
        """
        logger.info("Starting proxy server on port %s...", self.port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind(("", self.port))
            server_socket.listen(5)
            logger.info("Proxy server is listening for connections...")

            while True:
                client_socket, addr = server_socket.accept()
                logger.info("Accepted connection from %s", addr)
                threading.Thread(
                    target=self._handle_client, args=(client_socket,)
                ).start()
                logger.info("Started thread to handle client connection.")
