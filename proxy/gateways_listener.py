"""Listener for gateways to check their availability and reconnect if needed."""

import socket
import threading

from common.protocol import SIZE_OF_UINT8
import common.receiver as receiver
from common.logger import get_logger

from client_handler import ClientHandler

logger = get_logger("gateways_listener")

RETRY_INTERVAL = 5  # segundos


class GatewaysListener(threading.Thread):
    def __init__(self, proxy):
        super().__init__(daemon=True)
        self.proxy = proxy
        self._stop_flag = threading.Event()

        self.gateways_port = int(
            proxy.config["DEFAULT"].get("GATEWAY_ACCEPT_PORT", 9000)
        )
        self.gateways_listener_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )
        self.gateways_listener_socket.bind(("", self.gateways_port))
        self.gateways_listener_socket.listen(
            int(self.proxy.config["DEFAULT"].get("LISTEN_BACKLOG", 5))
        )
        self.gateways_listener_socket.settimeout(
            1
        )  # Para chequear la bandera de stop periódicamente

    def run(self):
        logger.info(
            "GatewaysListener listening for gateway connections on port %d",
            self.gateways_port,
        )
        while not self._stop_flag.is_set():
            try:
                gateway_socket, addr = self.gateways_listener_socket.accept()
                logger.info("Received connection from gateway at %s", addr)

                # Leer identificador del gateway
                gateway_id = self._receive_identifier(gateway_socket)
                if not gateway_id:
                    logger.warning("Failed to receive gateway ID, closing socket.")
                    gateway_socket.close()
                    continue

                logger.info("Identified gateway as '%s'", gateway_id)

                # Handle gateway reconnection properly
                self._handle_gateway_reconnection(gateway_id, gateway_socket)

            except socket.timeout:
                continue
            except Exception as e:
                logger.error("Error accepting gateway connection: %s", e)

    def _handle_gateway_reconnection(self, gateway_id, new_gateway_socket):
        """Handle gateway reconnection with proper cleanup"""
        is_reconnection = gateway_id in self.proxy._gateways_connected

        if is_reconnection:
            logger.info("Gateway %s is reconnecting, handling cleanup", gateway_id)

            # First, handle the reconnection cleanup in proxy
            self.proxy.handle_gateway_reconnection(gateway_id)

            # Close the old gateway connection
            old_socket = self.proxy._gateways_connected.get(gateway_id)
            if old_socket and old_socket.fileno() != -1:
                try:
                    old_socket.shutdown(socket.SHUT_RDWR)
                    old_socket.close()
                    logger.info("Closed old connection for gateway %s", gateway_id)
                except Exception as e:
                    logger.debug("Error closing old gateway socket: %s", e)

        # Register the new gateway connection
        self.proxy._gateways_connected[gateway_id] = new_gateway_socket
        if gateway_id not in self.proxy._gateway_locks:
            self.proxy._gateway_locks[gateway_id] = threading.Lock()

        # Start the response handler for the new connection
        self.proxy.start_gateway_response_handler(gateway_id, new_gateway_socket)

        logger.info("Gateway '%s' connected and registered.", gateway_id)

        # Handle client reconnections if this is a reconnection
        if is_reconnection:
            self._handle_client_reconnections(gateway_id, new_gateway_socket)

    def _handle_client_reconnections(self, gateway_id, new_gateway_socket):
        """Handle reconnecting clients to the new gateway socket"""
        logger.info("Handling client reconnections for gateway %s", gateway_id)

        # Get clients that were associated with this gateway
        clients_to_reconnect = list(self.proxy._clients_per_gateway.get(gateway_id, []))

        # Track successful reconnections
        successfully_reconnected = []

        for client_id in clients_to_reconnect:
            try:
                # Get client info
                client_info = self.proxy.get_client_info(client_id)
                if not client_info:
                    logger.warning(
                        "Client %s not found in connected clients", client_id
                    )
                    continue

                client_socket, old_gateway_socket, addr = client_info

                # Check if client socket is still valid
                if client_socket.fileno() == -1:
                    logger.info(
                        "Client %s socket is closed, removing from tracking",
                        client_id,
                    )
                    self._cleanup_client(client_id, gateway_id)
                    continue

                # Update the client's gateway socket
                success = self.proxy.update_client_gateway_socket(
                    client_id, new_gateway_socket, gateway_id
                )

                if success:
                    successfully_reconnected.append(client_id)
                    logger.info(
                        "Successfully reconnected client %s to gateway %s",
                        client_id,
                        gateway_id,
                    )

                    # Reiniciar el handler si está detenido
                    if client_id in self.proxy._client_handlers:
                        handler = self.proxy._client_handlers[client_id]
                        if not handler.is_alive():
                            logger.info("Restarting ClientHandler for %s", client_id)
                            new_handler = ClientHandler(
                                self.proxy, client_socket, addr, gateway_id
                            )
                            self.proxy._client_handlers[client_id] = new_handler
                            new_handler.start()
                else:
                    logger.error(
                        "Failed to reconnect client %s to gateway %s",
                        client_id,
                        gateway_id,
                    )
                    # Clean up failed reconnection
                    self._cleanup_client(client_id, gateway_id)

            except Exception as e:
                logger.error(
                    "Error reconnecting client %s to gateway %s: %s",
                    client_id,
                    gateway_id,
                    e,
                )
                # Clean up on error
                self._cleanup_client(client_id, gateway_id)

        logger.info(
            "Reconnected %d/%d clients to gateway %s",
            len(successfully_reconnected),
            len(clients_to_reconnect),
            gateway_id,
        )

    def _receive_identifier(self, sock):
        try:
            gateway_number = receiver.receive_data(
                sock, SIZE_OF_UINT8, timeout=RETRY_INTERVAL
            )
            if not gateway_number:
                return None
            return int.from_bytes(gateway_number, byteorder="big")
        except Exception as e:
            logger.error("Failed to read gateway number: %s", e)
            return None

    def _cleanup_client(self, client_id, gateway_id):
        """Clean up a disconnected client"""
        logger.info("Cleaning up client %s from gateway %s", client_id, gateway_id)

        # Use the proxy's cleanup method for comprehensive cleanup
        self.proxy.cleanup_client(client_id)

        # Also remove from this gateway's client list (if proxy cleanup didn't)
        if client_id in self.proxy._clients_per_gateway[gateway_id]:
            self.proxy._clients_per_gateway[gateway_id].remove(client_id)
            logger.debug(
                "Removed client %s from gateway %s client list", client_id, gateway_id
            )

    def stop(self):
        logger.info("Stopping gateways listener...")
        self._stop_flag.set()
        try:
            self.gateways_listener_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        self.gateways_listener_socket.close()
        self.join()
        logger.info("Gateways listener stopped.")
