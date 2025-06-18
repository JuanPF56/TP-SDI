import socket
import signal
import os

from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher
from common.protocol import SIZE_OF_UUID
import common.receiver as receiver
from common.logger import get_logger

logger = get_logger("Gateway")

TIMEOUT_PROXY = 3600


class Gateway:
    def __init__(self, config):
        self.config = config
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.port = int(os.getenv("GATEWAY_PORT", config["DEFAULT"]["GATEWAY_PORT"]))

        # Actuar como servidor: escuchar conexiones del proxy
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._gateway_socket.bind(("", self.port))
        self._gateway_socket.listen(int(config["DEFAULT"]["LISTEN_BACKLOG"]))
        logger.info("Gateway listening on port %s", self.port)

        self._was_closed = False

        self._clients_connected = ClientRegistry()
        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])

        self._result_dispatcher = None
        self._setup_result_dispatcher()

        try:
            with open(f"/tmp/{self.node_name}_ready", "w", encoding="utf-8") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error("Failed to create healthcheck file: %s", e)

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _setup_result_dispatcher(self):
        self._result_dispatcher = ResultDispatcher(
            self.config, clients_connected=self._clients_connected
        )
        self._result_dispatcher.start()

    def run(self):
        logger.info("Gateway started. Waiting for proxy connections...")
        while not self._was_closed:
            try:
                self._reap_disconnected_clients()

                proxy_socket, proxy_addr = self._gateway_socket.accept()
                logger.info("New connection from proxy address %s", proxy_addr)

                client_id = receiver.receive_data(
                    proxy_socket, SIZE_OF_UUID, timeout=TIMEOUT_PROXY
                )
                if not client_id:
                    logger.warning("No UUID received. Closing connection.")
                    proxy_socket.close()
                    continue

                client_id = client_id.decode("utf-8")
                new_connected_client = ConnectedClient(
                    client_id=client_id,
                    client_socket=proxy_socket,
                    client_addr=proxy_addr,
                    config=self.config,
                )
                self._clients_connected.add(new_connected_client)
                logger.info("New client connected: %s", client_id)
                new_connected_client.start()

            except OSError as e:
                if self._was_closed:
                    break
                logger.error("Error accepting new connection: %s", e)

    def _reap_disconnected_clients(self):
        try:
            logger.info("Checking for any disconnected clients...")
            for client in list(self._clients_connected.get_all().values()):
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
            os.remove(f"/tmp/{self.node_name}_ready")
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
