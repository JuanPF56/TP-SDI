"""Proxy class for handling requests to multiple gateways."""

from email import header
import os
import socket
import threading
import struct
import signal
import logging

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UUID
import common.receiver as receiver
import common.sender as sender
from common.logger import get_logger

logger = get_logger("proxy")
logger.setLevel(logging.INFO)

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

    def _forward(self, source: socket.socket, destination: socket.socket):
        try:
            while True:
                # receive the header and payload from the source
                logger.debug("Waiting from source...")
                header = receiver.receive_data(
                    source, SIZE_OF_HEADER, timeout=TIMEOUT_HEADER
                )
                if len(header) != SIZE_OF_HEADER:
                    logger.error(
                        "Incomplete header received from source, expected %d bytes, got %d",
                        SIZE_OF_HEADER,
                        len(header),
                    )
                    break
                if not header:
                    logger.debug("Connection closed while reading header.")
                    break
                (
                    type_of_batch,
                    _encoded_id,
                    _current_batch,
                    _is_last_batch,
                    payload_len,
                ) = struct.unpack(">B36sIBI", header)

                payload = receiver.receive_data(
                    source, payload_len, timeout=TIMEOUT_PAYLOAD
                )
                if len(payload) != payload_len:
                    logger.error(
                        "Incomplete payload received, expected %d bytes, got %d",
                        payload_len,
                        len(payload),
                    )
                    break

                # Forward the header and payload to the destination
                logger.debug(
                    "Forwarding data to destination: %s",
                    destination,
                )
                sender.send(destination, header)
                sender.send(destination, payload)

        except Exception as e:
            logger.error("Error in structured forwarding: %s", e)

    def _handle_client(self, client_socket, addr):
        client_id = f"{addr[0]}:{addr[1]}"
        for host, port in self.gateways:
            try:
                gateway_socket = socket.create_connection((host, port), timeout=None)
                logger.info("Connected to %s:%s for client %s", host, port, client_id)

                self._connected_clients[client_id] = (client_socket, gateway_socket)

                # Receive the client ID from the gateway and send it to the client
                logger.debug("Waiting for client ID from gateway...")
                encoded_client_id = receiver.receive_data(
                    gateway_socket, SIZE_OF_UUID, timeout=TIMEOUT_HEADER
                )
                if not encoded_client_id or len(encoded_client_id) != SIZE_OF_UUID:
                    logger.error(
                        "Invalid client ID received from gateway %s:%s", host, port
                    )
                    gateway_socket.close()
                    continue
                client_id = encoded_client_id.decode("utf-8")
                logger.info("Received client ID from gateway: %s", client_id)
                sender.send(client_socket, encoded_client_id)
                logger.info("Sent client ID %s to client %s", client_id, addr)

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
        while not self._was_closed:
            try:
                client_socket, addr = self.proxy_socket.accept()
                logger.info("Accepted connection from the client %s", addr)
                threading.Thread(
                    target=self._handle_client, args=(client_socket, addr)
                ).start()
                logger.info("Started thread to handle client connection.")

            except Exception as e:
                if self._was_closed:
                    logger.info("Proxy server was closed, stopping accept loop.")
                    break
                logger.error("Error accepting client connection: %s", e)
