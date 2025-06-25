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
from client_handler import ClientHandler
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
        self._client_handlers: dict[str, ClientHandler] = {}
        # Dictionary to hold suspended clients: client_id -> (client_socket, gateway_id)
        self._suspended_clients: dict[str, tuple[socket.socket, str]] = {}

        self._query_to_client = {}  # query_id -> client_id mapping
        self._query_lock = threading.Lock()  # Protect the mapping
        self._clients_lock = threading.Lock()  # Protect client data structures

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
        with self._clients_lock:
            client_ids = list(self._connected_clients.keys())

        for client_id in client_ids:
            self.cleanup_client(client_id)

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

                    # Find the target client (thread-safe)
                    with self._clients_lock:
                        client_info = self._connected_clients.get(target_client_id)

                    if not client_info:
                        logger.error(
                            "Client %s not found in connected clients", target_client_id
                        )
                        continue

                    client_socket, _, _ = client_info

                    if client_socket.fileno() == -1:
                        logger.warning("Client %s socket is closed", target_client_id)
                        # Clean up the disconnected client
                        self.cleanup_client(target_client_id)
                        continue

                    # Forward to the specific client
                    sender.send(client_socket, header)
                    logger.debug(
                        "Forwarded header to client %s: tipo_mensaje=%s, query_id=%s, payload_len=%s",
                        target_client_id,
                        tipo_mensaje,
                        query_id,
                        payload_len,
                    )
                    sender.send(client_socket, payload)
                    logger.debug(
                        "Forwarded payload to client %s: %s", target_client_id, payload
                    )
                    logger.debug("Forwarded response to client %s", target_client_id)

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(
                        "Failed to parse payload from gateway %s: %s", gateway_id, e
                    )
                    continue
                except sender.SenderError as e:
                    logger.error("Failed to send to client: %s", e)
                    # If send fails, client might be disconnected
                    if target_client_id:
                        self.cleanup_client(target_client_id)
                    continue

            except Exception as e:
                logger.error(
                    "Gateway response handler for %s failed: %s", gateway_id, e
                )
                break

        # Gateway connection failed - cleanup all clients connected to this gateway
        logger.warning(
            "Gateway %s connection lost, cleaning up associated clients", gateway_id
        )
        self._cleanup_gateway_clients(gateway_id)
        logger.info("Gateway response handler for %s stopped", gateway_id)

    def get_client_info(self, client_id):
        """Get client info - thread-safe"""
        with self._clients_lock:
            return self._connected_clients.get(client_id)

    def is_client_connected(self, client_id):
        """Check if client is connected - thread-safe"""
        with self._clients_lock:
            client_info = self._connected_clients.get(client_id)
            if not client_info:
                return False

            client_socket, _, _ = client_info
            return client_socket.fileno() != -1

    def cleanup_client(self, client_id):
        """Clean up a disconnected client - thread-safe and comprehensive"""
        logger.info("Cleaning up client %s", client_id)

        with self._clients_lock:
            # Remove from connected clients
            client_info = self._connected_clients.pop(client_id, None)

            if client_info:
                client_socket, gateway_socket, addr = client_info

                # Close client socket
                try:
                    if client_socket.fileno() != -1:
                        client_socket.shutdown(socket.SHUT_RDWR)
                except Exception as e:
                    logger.debug(
                        "Error shutting down client socket for %s: %s", client_id, e
                    )
                finally:
                    try:
                        client_socket.close()
                    except Exception as e:
                        logger.debug(
                            "Error closing client socket for %s: %s", client_id, e
                        )

                # Close gateway socket
                try:
                    if gateway_socket.fileno() != -1:
                        gateway_socket.shutdown(socket.SHUT_RDWR)
                except Exception as e:
                    logger.debug(
                        "Error shutting down gateway socket for %s: %s", client_id, e
                    )
                finally:
                    try:
                        gateway_socket.close()
                    except Exception as e:
                        logger.debug(
                            "Error closing gateway socket for %s: %s", client_id, e
                        )

                logger.info("Closed sockets for client %s", client_id)

            # Remove from gateway client lists
            for gateway_id, client_list in self._clients_per_gateway.items():
                if client_id in client_list:
                    client_list.remove(client_id)
                    logger.info(
                        "Removed client %s from gateway %s", client_id, gateway_id
                    )

        # Clean up any pending query mappings (separate lock to avoid deadlock)
        with self._query_lock:
            queries_to_remove = [
                q_id
                for q_id, c_id in self._query_to_client.items()
                if c_id == client_id
            ]
            for q_id in queries_to_remove:
                del self._query_to_client[q_id]

            if queries_to_remove:
                logger.info(
                    "Removed %d pending queries for client %s",
                    len(queries_to_remove),
                    client_id,
                )

        client_handler = self._client_handlers.pop(client_id, None)
        if client_handler:
            client_handler._stop_flag.set()

    def add_client(self, client_id, client_socket, gateway_socket, addr):
        """Add a new client connection - thread-safe"""
        with self._clients_lock:
            self._connected_clients[client_id] = (client_socket, gateway_socket, addr)
            logger.info("Added client %s", client_id)

    def suspend_client(self, client_id, gateway_id):
        with self._clients_lock:
            client_info = self._connected_clients.pop(client_id, None)
            if client_info:
                client_socket, _, _ = client_info
                self._suspended_clients[client_id] = (client_socket, gateway_id)
                logger.info(
                    "Suspended client %s (gateway %s disconnected)",
                    client_id,
                    gateway_id,
                )

    def _cleanup_gateway_clients(self, gateway_id):
        """Clean up all clients associated with a failed gateway"""
        logger.info("Cleaning up all clients for failed gateway %s", gateway_id)

        # Get list of clients for this gateway
        with self._clients_lock:
            clients_to_cleanup = list(self._clients_per_gateway.get(gateway_id, []))

        # Clean up each client
        for client_id in clients_to_cleanup:
            self.suspend_client(client_id, gateway_id)

        # Clear the gateway's client list
        if gateway_id in self._clients_per_gateway:
            self._clients_per_gateway[gateway_id].clear()
            logger.info("Cleared client list for gateway %s", gateway_id)

    def update_client_gateway_socket(self, client_id, new_gateway_socket, gateway_id):
        """Update the gateway socket for an existing client - used during gateway reconnection"""
        with self._clients_lock:
            client_info = self._connected_clients.get(client_id)
            if client_info:
                client_socket, old_gateway_socket, addr = client_info

                # Close the old gateway socket
                try:
                    if old_gateway_socket.fileno() != -1:
                        old_gateway_socket.shutdown(socket.SHUT_RDWR)
                        old_gateway_socket.close()
                        logger.debug(
                            "Closed old gateway socket for client %s", client_id
                        )
                except Exception as e:
                    logger.debug(
                        "Error closing old gateway socket for client %s: %s",
                        client_id,
                        e,
                    )

                # Update with new gateway socket
                self._connected_clients[client_id] = (
                    client_socket,
                    new_gateway_socket,
                    addr,
                )

                # Ensure client is in the gateway's client list
                if client_id not in self._clients_per_gateway[gateway_id]:
                    self._clients_per_gateway[gateway_id].append(client_id)

                logger.info(
                    "Updated gateway socket for client %s on gateway %s",
                    client_id,
                    gateway_id,
                )
                return True
            else:
                logger.warning(
                    "Attempted to update gateway socket for non-existent client %s",
                    client_id,
                )
                return False

    def handle_gateway_reconnection(self, gateway_id):
        """Handle cleanup when a gateway reconnects"""
        logger.info("Handling reconnection for gateway %s", gateway_id)

        # Stop the old response handler thread if it exists
        old_thread = self._gateway_response_threads.pop(gateway_id, None)
        if old_thread and old_thread.is_alive():
            logger.info(
                "Old response handler thread for gateway %s will be stopped", gateway_id
            )

        # Get current clients for this gateway (thread-safe)
        with self._clients_lock:
            clients_for_gateway = list(self._clients_per_gateway.get(gateway_id, []))

        # Check each client's validity and clean up invalid ones
        clients_to_remove = []
        valid_clients = []

        for client_id in clients_for_gateway:
            client_info = self._connected_clients.get(client_id)
            if not client_info:
                # Client not in connected clients - remove from gateway list
                clients_to_remove.append(client_id)
                logger.info(
                    "Client %s not found in connected clients, removing", client_id
                )
                continue

            client_socket, old_gateway_socket, addr = client_info

            # Check if client socket is still valid
            if client_socket.fileno() == -1:
                clients_to_remove.append(client_id)
                logger.info(
                    "Client %s socket is invalid, will be cleaned up", client_id
                )
            else:
                valid_clients.append(client_id)
                logger.info("Client %s is valid and will be reconnected", client_id)

                # Close the old gateway socket for this client
                try:
                    if old_gateway_socket and old_gateway_socket.fileno() != -1:
                        old_gateway_socket.close()
                        logger.debug(
                            "Closed old gateway socket for client %s", client_id
                        )
                except Exception as e:
                    logger.debug(
                        "Error closing old gateway socket for client %s: %s",
                        client_id,
                        e,
                    )

        # Clean up invalid clients
        for client_id in clients_to_remove:
            self.cleanup_client(client_id)

        logger.info(
            "Gateway %s reconnection cleanup: %d valid clients, %d removed",
            gateway_id,
            len(valid_clients),
            len(clients_to_remove),
        )

        # 🔄 Restaurar clientes suspendidos para este gateway
        reactivated = 0
        with self._clients_lock:
            suspended_for_gateway = {
                client_id: (client_socket, gw_id)
                for client_id, (client_socket, gw_id) in self._suspended_clients.items()
                if gw_id == gateway_id
            }

            for client_id, (client_socket, _) in suspended_for_gateway.items():
                addr = client_socket.getpeername()

                # Restaurar cliente a _connected_clients
                self._connected_clients[client_id] = (
                    client_socket,
                    self._gateways_connected[gateway_id],
                    addr,
                )
                self._clients_per_gateway[gateway_id].append(client_id)
                del self._suspended_clients[client_id]

                logger.info(
                    "Reactivado cliente suspendido %s con gateway %s",
                    client_id,
                    gateway_id,
                )
                reactivated += 1

        logger.info(
            "Reactivados %d clientes suspendidos para gateway %s",
            reactivated,
            gateway_id,
        )
