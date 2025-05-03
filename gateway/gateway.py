import socket
import signal
import os
import uuid

from common.logger import get_logger
logger = get_logger("Gateway")

from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher

class Gateway():
    def __init__(self, config):
        self.config = config

        # Initialize gateway socket
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', int(config["DEFAULT"]["GATEWAY_PORT"])))
        self._gateway_socket.listen(int(config["DEFAULT"]["LISTEN_BACKLOG"]))
        logger.info(f"Gateway listening on port {config['DEFAULT']['GATEWAY_PORT']}")
        self._was_closed = False

        # Initialize connected clients registry that monitors the connected clients
        self._clients_connected = ClientRegistry()
        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])

        # Initialize and start the ResultDispatcher
        self._result_dispatcher = None
        self._setup_result_dispatcher()

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _setup_result_dispatcher(self):
        """
        Set up the ResultDispatcher.
        """
        self._result_dispatcher = ResultDispatcher(
            self.config,
            clients_connected=self._clients_connected
        )
        self._result_dispatcher.start()

    def run(self):
        while not self._was_closed:
            try:
                new_connected_client = self.__accept_new_connection()
                if new_connected_client is None:
                    logger.error("Failed to accept new connection")
                    continue

                logger.info(f"New client connected: {new_connected_client.get_client_id()}")
                new_connected_client.start()

            except OSError as e:
                if self._was_closed:
                    break
                logger.error(f"Error accepting new connection: {e}")

    def __accept_new_connection(self):
        logger.info("Waiting for new connections...")
        accepted_socket, accepted_address = self._gateway_socket.accept()
        logger.info(f"New connection from {accepted_address}")

        new_connected_client = ConnectedClient(
            client_id = str(uuid.uuid4()),
            client_socket = accepted_socket,
            client_addr = accepted_address,
            config=self.config
        )
        self._clients_connected.add(new_connected_client)
        return new_connected_client

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping server...")
        self._stop_server()

    def _stop_server(self):
        try:
            logger.info("Stopping server...")
            if self._result_dispatcher:
                self._result_dispatcher.stop()
                self._result_dispatcher.join()
                logger.info("ResultDispatcher stopped.")

            if self._gateway_socket:
                self._was_closed = True
                self._close_connected_clients()
                try:
                    self._gateway_socket.shutdown(socket.SHUT_RDWR)
                except OSError as e:
                    logger.error(f"Socket already shut down")
                finally:
                    if self._gateway_socket:
                        self._gateway_socket.close()
                        logger.info("Gateway socket closed.")
                    try:
                        os.remove("/tmp/gateway_ready")
                    except FileNotFoundError:
                        pass
                    logger.info("Server stopped.")

        except Exception as e:
            logger.error(f"Failed to close server properly: {e}")

    def _close_connected_clients(self):
        logger.info("Closing connected clients...")
        try:
            self._clients_connected.clear()
        except Exception as e:
            logger.error(f"Error closing client socket: {e}")