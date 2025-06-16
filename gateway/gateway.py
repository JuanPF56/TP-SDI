"""
Gateway Node Main Script
This script initializes the gateway node, loads the configuration
and starts the gateway service.
"""

import socket
import signal
import uuid
import os

from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher
from common.logger import get_logger

logger = get_logger("Gateway")


class Gateway:
    def __init__(self, config):
        self.config = config

        # Initialize gateway socket
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(("", int(config["DEFAULT"]["GATEWAY_PORT"])))
        self._gateway_socket.listen(int(config["DEFAULT"]["LISTEN_BACKLOG"]))
        logger.info("Gateway listening on port %s", config["DEFAULT"]["GATEWAY_PORT"])
        self._was_closed = False

        # Initialize connected clients registry that monitors the connected clients
        self._clients_connected = ClientRegistry()
        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])

        # Initialize and start the ResultDispatcher
        self._result_dispatcher = None
        self._setup_result_dispatcher()

        try:
            with open("/tmp/gateway_ready", "w", encoding="utf-8") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error("Failed to create healthcheck file: %s", e)

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _setup_result_dispatcher(self):
        """
        Set up the ResultDispatcher.
        """
        self._result_dispatcher = ResultDispatcher(
            self.config, clients_connected=self._clients_connected
        )
        self._result_dispatcher.start()

    def run(self):
        while not self._was_closed:
            try:
                self._reap_disconnected_clients()

                new_connected_client = self._accept_new_connection()
                if new_connected_client is None:
                    logger.error("Failed to accept new connection")
                    continue

                logger.info(
                    "New client connected: %s", new_connected_client.get_client_id()
                )
                new_connected_client.start()

            except OSError as e:
                if self._was_closed:
                    break
                logger.error("Error accepting new connection: %s", e)

    def _accept_new_connection(self):
        logger.info("Waiting for new connections...")
        accepted_socket, accepted_address = self._gateway_socket.accept()
        logger.info("New connection from %s", accepted_address)

        new_connected_client = ConnectedClient(
            client_id=str(uuid.uuid4()),
            client_socket=accepted_socket,
            client_addr=accepted_address,
            config=self.config,
        )
        self._clients_connected.add(new_connected_client)
        return new_connected_client

    def _reap_disconnected_clients(self):
        try:
            logger.info("Checking for any disconnected clients...")
            all_clients = self._clients_connected.get_all()
            for client in all_clients.values():
                if not client.client_is_connected():
                    logger.info(
                        "Removing disconnected client %s", client.get_client_id()
                    )
                    self._clients_connected.remove(client)
            logger.info("Done checking.")
        except Exception as e:
            logger.error("Error during client cleanup: %s", e)

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping server...")
        self._stop_server()

    def _stop_server(self):
        logger.info("Stopping server...")

        if self._result_dispatcher:
            try:
                self._result_dispatcher.stop()
                self._result_dispatcher.join()
                logger.info("ResultDispatcher stopped.")
            except Exception as e:
                logger.warning("Error stopping ResultDispatcher: %s", e)

        self._was_closed = True

        try:
            self._close_connected_clients()
        except Exception as e:
            logger.warning("Error closing clients: %s", e)

        if self._gateway_socket:
            try:
                self._gateway_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                logger.warning("Gateway socket already shut down or not connected.")
            except Exception as e:
                logger.error("Unexpected error during socket shutdown: %s", e)
            finally:
                try:
                    self._gateway_socket.close()
                    logger.info("Gateway socket closed.")
                except Exception as e:
                    logger.warning("Error closing gateway socket: %s", e)
                self._gateway_socket = None

        try:
            os.remove("/tmp/gateway_ready")
            logger.info("Healthcheck file removed.")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error("Error removing healthcheck file: %s", e)

        logger.info("Server stopped.")

    def _close_connected_clients(self):
        logger.info("Closing connected clients...")
        try:
            self._clients_connected.clear()
        except Exception as e:
            logger.error("Error closing client socket: %s", e)
