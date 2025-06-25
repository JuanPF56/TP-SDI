import socket
import signal
import os
import time
import threading

from protocol_gateway_proxy import ProtocolGatewayProxy
from raw_message import RawMessage
from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher
from storage import load_clients_from_disk
from common.protocol import TIPO_MENSAJE
from common.logger import get_logger

logger = get_logger("Gateway")

MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 5
DELAY_RETRY_RECEIVE_MESSAGE = 2
MAX_DELAY_RECEIVE_MESSAGE = 60


class Gateway:
    def __init__(self, config):
        self.config = config
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.host = os.getenv("PROXY_HOST", config["DEFAULT"]["PROXY_HOST"])
        self.port = int(os.getenv("PROXY_PORT", config["DEFAULT"]["PROXY_PORT"]))

        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._protocol = None
        self._socket_lock = (
            threading.Lock()
        )  # Shared lock for send operation for clients
        self._connect_to_proxy()
        self._was_closed = False

        self._clients_connected = ClientRegistry()
        self._setup_result_dispatcher()

        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])
        self._result_dispatcher = None

        self.restore_connected_clients()

        try:
            with open(f"/tmp/{self.node_name}_ready", "w", encoding="utf-8") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error("Failed to create healthcheck file: %s", e)

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _connect_to_proxy(self, retries=MAX_RETRIES, delay=DELAY_BETWEEN_RETRIES):
        attempt = 0
        while attempt < retries:
            try:
                self._gateway_socket.connect((self.host, self.port))
                logger.info("Connected to proxy at %s:%s", self.host, self.port)
                self._protocol = ProtocolGatewayProxy(
                    gateway_socket=self._gateway_socket,
                )
                return self.send_gateway_number()
            except Exception as e:
                attempt += 1
                logger.warning("Connection attempt %d failed: %s", attempt, e)
                if attempt < retries:
                    logger.info("Retrying in %d seconds...", delay)
                    time.sleep(delay)
                else:
                    logger.error(
                        "Max connection attempts reached. Check if server is up."
                    )

    def send_gateway_number(self) -> bool:
        gateway_number = int(self.node_name.split("_")[-1])
        return self._protocol.send_gateway_number(gateway_number)

    def restore_connected_clients(self):
        """
        Restore connected clients from the registry.
        This is called when the gateway is restarted to re-establish connections.
        """
        logger.info("Restoring connected clients...")
        clients_to_restore = load_clients_from_disk()
        if not clients_to_restore:
            logger.info("No clients to restore.")
            return
        for client_id in clients_to_restore:
            logger.info("Restoring client %s", client_id)
            try:
                restored_client = ConnectedClient(
                    client_id=client_id,
                    gateway_socket=self._gateway_socket,
                    config=self.config,
                    shared_socket_lock=self._socket_lock,
                )
                self._clients_connected.add(restored_client)
                logger.info("New client connected: %s", client_id)
                restored_client.start()
            except Exception as e:
                logger.error("Error restoring client %s: %s", client_id, e)

    def gateway_is_connected(self) -> bool:
        """
        Check if the gateway is connected
        """
        return self._gateway_socket is not None and self._gateway_socket.fileno() != -1

    def _setup_result_dispatcher(self):
        self._result_dispatcher = ResultDispatcher(
            self.config, clients_connected=self._clients_connected
        )
        self._result_dispatcher.start()

    def _handle_new_client(self, encoded_id):
        try:
            client_id = encoded_id.decode("utf-8").rstrip("\x00")
            logger.debug("Received client ID: '%s' (len=%d)", client_id, len(client_id))

            existing_client = self._clients_connected.get_by_uuid(client_id)
            if existing_client:
                logger.info(
                    "Client %s already connected, skipping creation.", client_id
                )

            else:
                new_connected_client = ConnectedClient(
                    client_id=client_id,
                    gateway_socket=self._gateway_socket,
                    config=self.config,
                    shared_socket_lock=self._socket_lock,
                )
                self._clients_connected.add(new_connected_client)
                logger.info("New client connected: %s", client_id)
                new_connected_client.start()

        except OSError as e:
            logger.error("Error accepting new connection: %s", e)

    def _handle_batch_messages(self, raw_message: RawMessage) -> bool:
        client_id_from_message = raw_message.encoded_id.decode("utf-8").rstrip("\x00")
        client = self._clients_connected.get_by_uuid(client_id_from_message)
        if not client:
            logger.warning(
                "Client not found for batch message %s", raw_message.batch_number
            )
            logger.info("Creating new client from batch message")
            client = ConnectedClient(
                client_id=client_id_from_message,
                gateway_socket=self._gateway_socket,
                config=self.config,
                shared_socket_lock=self._socket_lock,
            )
            self._clients_connected.add(client)

        logger.debug("Handling batch message for client %s", client.get_client_id())
        return client.process_batch(
            raw_message.message_id,
            raw_message.tipo_mensaje,
            raw_message.batch_number,
            raw_message.is_last_batch,
            raw_message.payload,
        )

    def run(self):
        delay = DELAY_RETRY_RECEIVE_MESSAGE
        logger.info("Gateway running...")
        while not self._was_closed:
            try:
                if self._gateway_socket.fileno() == -1:
                    logger.info("Socket already closed, stopping gateway loop.")
                    break

                raw_message = self._protocol.receive_message()
                if raw_message is None:
                    delay = min(delay, MAX_DELAY_RECEIVE_MESSAGE)
                    logger.info(
                        "No messages received, retrying in %s seconds...", delay
                    )
                    # exponential backoff for retries
                    time.sleep(delay)
                    delay *= 2
                    continue

                if raw_message.tipo_mensaje == TIPO_MENSAJE["NEW_CLIENT"]:
                    self._handle_new_client(raw_message.encoded_id)

                elif (
                    raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_MOVIES"]
                    or raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_CREDITS"]
                    or raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_RATINGS"]
                ):
                    try:
                        self._handle_batch_messages(raw_message)
                    except Exception as e:
                        logger.error(
                            "Error in batch with: message_id=%s, tipo_mensaje=%s, batch_number=%s: %s",
                            raw_message.message_id,
                            raw_message.tipo_mensaje,
                            raw_message.batch_number,
                            e,
                        )
                        continue

                else:
                    logger.warning(
                        "Unknown message type received: %s", raw_message.tipo_mensaje
                    )

            except OSError as e:
                if self._was_closed:
                    break
                logger.error("Error accepting new connection: %s", e)
                return

            except Exception as e:
                logger.error("Unexpected error: %s", e)
                return

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
