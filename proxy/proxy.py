"""Proxy class for handling requests to multiple gateways."""

import json
import threading
import socket
import signal
import logging
import time
from collections import defaultdict

from gateways_listener import GatewaysListener
from clients_listener import ClientsListener
from common.protocol import SIZE_OF_HEADER_RESULTS, unpack_result_header
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
        self._was_closed = False

        # initialize the gateway connections
        self._gateways_connected = {}
        self._gateway_locks = {}
        self._gateway_response_threads = {}

        # initialize the client dictionary: client_id -> (client_socket, gateway_socket, addr)
        self._connected_clients: dict[str, tuple[socket.socket, socket.socket, str]] = (
            {}
        )

        self._query_to_client = {}  # query_id -> client_id mapping
        self._query_lock = threading.Lock()  # Protect the mapping

        # Start the clients and gateways listener thread
        self.gateways_listener = GatewaysListener(self)
        self.clients_listener = ClientsListener(self)
        self._clients_per_gateway = defaultdict(list)

        self.gateways_listener.start()
        self.clients_listener.start()

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping proxy...")
        self._stop_proxy()

    def _stop_proxy(self):
        logger.info("Stopping proxy...")
        self._was_closed = True

        if self.clients_listener:
            logger.info("Stopping clients listener...")
            try:
                self.clients_listener.stop()
                self.clients_listener.join()
                logger.info("Clients listener stopped.")
            except Exception as e:
                logger.error("Error stopping clients listener: %s", e)

        if self.gateways_listener:
            logger.info("Stopping gateways listener...")
            try:
                self.gateways_listener.stop()
                self.gateways_listener.join()
                logger.info("Gateways listener stopped.")
            except Exception as e:
                logger.error("Error stopping gateways listener: %s", e)

        # Close all client <-> gateway connections
        logger.info("Closing all client-gateway connections...")
        for client_id, (
            client_sock,
            gateway_sock,
            addr,
        ) in self._connected_clients.items():
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

        logger.info("Proxy stopped.")

    def run(self):
        """
        Run the proxy server.
        It listens on the specified port and handles incoming client connections.
        """
        while not self._was_closed:
            time.sleep(1)

    def start_gateway_response_handler(self, gateway_id, gateway_socket):
        """Start a response handler thread for a specific gateway"""
        if gateway_id not in self._gateway_response_threads:
            thread = threading.Thread(
                target=self._handle_gateway_responses,
                args=(gateway_id, gateway_socket),
                daemon=True,
            )
            thread.start()
            self._gateway_response_threads[gateway_id] = thread
            logger.info("Started response handler for gateway %s", gateway_id)

    def _handle_gateway_responses(self, gateway_id, gateway_socket):
        """Handle responses from a specific gateway and route to appropriate clients"""
        logger.debug("Starting gateway response handler for %s", gateway_id)

        while not self._was_closed:
            try:
                if gateway_socket.fileno() == -1:
                    break

                header = receiver.receive_data(
                    gateway_socket,
                    SIZE_OF_HEADER_RESULTS,
                    timeout=TIMEOUT_HEADER,
                )
                if not header or len(header) != SIZE_OF_HEADER_RESULTS:
                    break

                tipo_mensaje, query_id, payload_len = unpack_result_header(header)
                logger.debug(
                    "Received header from gateway %s: tipo_mensaje=%s, query_id=%s, payload_len=%s",
                    gateway_id,
                    tipo_mensaje,
                    query_id,
                    payload_len,
                )

                payload = receiver.receive_data(
                    gateway_socket, payload_len, timeout=TIMEOUT_PAYLOAD
                )
                if not payload or len(payload) != payload_len:
                    break

                logger.debug(
                    "Received payload from gateway %s: %s", gateway_id, payload
                )

                # Parse payload to get client_id
                try:
                    result_data = json.loads(payload.decode("utf-8"))
                    target_client_id = result_data.get("client_id")

                    if not target_client_id:
                        logger.error(
                            "No client_id found in payload from gateway %s", gateway_id
                        )
                        continue

                    # Find the target client
                    client_info = self._connected_clients.get(target_client_id)
                    if not client_info:
                        logger.error(
                            "Client %s not found in connected clients", target_client_id
                        )
                        continue

                    client_socket, _, _ = client_info

                    if client_socket.fileno() == -1:
                        logger.warning("Client %s socket is closed", target_client_id)
                        # Clean up
                        self._connected_clients.pop(target_client_id, None)
                        if target_client_id in self._clients_per_gateway[gateway_id]:
                            self._clients_per_gateway[gateway_id].remove(
                                target_client_id
                            )
                        continue

                    # Forward to the specific client
                    sender.send(client_socket, header)
                    sender.send(client_socket, payload)
                    logger.debug("Forwarded response to client %s", target_client_id)

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(
                        "Failed to parse payload from gateway %s: %s", gateway_id, e
                    )
                    continue
                except sender.SenderError as e:
                    logger.error("Failed to send to client: %s", e)
                    continue

            except Exception as e:
                logger.error(
                    "Gateway response handler for %s failed: %s", gateway_id, e
                )
                break

        logger.info("Gateway response handler for %s stopped", gateway_id)
