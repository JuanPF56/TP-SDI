import socket
import signal
import os
import time
import threading

from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher
from common.protocol import unpack_header, TIPO_MENSAJE, SIZE_OF_UUID, SIZE_OF_HEADER
import common.sender as sender
import common.receiver as receiver
from common.logger import get_logger

logger = get_logger("Gateway")

MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 50

TIMEOUT_PROXY = 3600


class Gateway:
    def __init__(self, config):
        self.config = config
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.host = os.getenv("PROXY_HOST", config["DEFAULT"]["PROXY_HOST"])
        self.port = int(os.getenv("PROXY_PORT", config["DEFAULT"]["PROXY_PORT"]))

        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_lock = threading.Lock()
        self._connect_to_proxy()

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

    def _connect_to_proxy(self, retries=MAX_RETRIES, delay=DELAY_BETWEEN_RETRIES):
        attempt = 0
        while attempt < retries:
            try:
                self._gateway_socket.connect((self.host, self.port))
                logger.info("Connected to proxy at %s:%s", self.host, self.port)
                gateway_number = int(self.node_name.split("_")[-1])
                sender.send(
                    self._gateway_socket,
                    gateway_number.to_bytes(1, byteorder="big"),
                )
                return
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

            if existing_client and existing_client.was_closed:
                logger.info("Reusing existing client object for %s", client_id)
                # Reabrí el socket en el objeto existente (si es posible)
                existing_client._gateway_socket = self._gateway_socket
                existing_client._stop_flag.clear()
                existing_client._running = True
                existing_client.was_closed = False
                existing_client.start()
                existing_client.send_all_stored_results()
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

    def _handle_disconnect_client(self):
        try:
            client_id = receiver.receive_data(
                self._gateway_socket, SIZE_OF_UUID, timeout=TIMEOUT_PROXY
            )
            if not client_id:
                logger.warning("No UUID received for disconnection.")
                return

            client_id = client_id.decode("utf-8").rstrip("\x00")
            logger.debug("Received client ID for disconnection: '%s'", client_id)

            client_to_disconnect = self._clients_connected.get_by_uuid(client_id)
            if client_to_disconnect:
                logger.info("Disconnecting client: %s", client_id)
                client_to_disconnect._stop_client()
                self._clients_connected.remove(client_to_disconnect)
            else:
                logger.warning("Client %s not found for disconnection.", client_id)

        except OSError as e:
            logger.error("Error handling disconnect request: %s", e)

    def _handle_batch_messages(
        self,
        message_id,
        tipo_mensaje,
        encoded_id,
        batch_number,
        is_last_batch,
        payload_length,
    ) -> bool:
        client = self._clients_connected.get_by_uuid(
            encoded_id.decode("utf-8").rstrip("\x00")
        )
        if not client:
            logger.warning("Client not found for batch message %s", batch_number)
            return False

        logger.debug("Handling batch message for client %s", client.get_client_id())
        return client.process_batch(
            message_id, tipo_mensaje, batch_number, is_last_batch, payload_length
        )

    def run(self):
        logger.info("Gateway running...")
        while not self._was_closed:
            try:
                # self._reap_completed_clients()

                if self._gateway_socket.fileno() == -1:
                    logger.info("Socket already closed, stopping gateway loop.")
                    break

                new_message = receiver.receive_data(
                    self._gateway_socket, SIZE_OF_HEADER, timeout=TIMEOUT_PROXY
                )
                (
                    message_id,
                    tipo_mensaje,
                    encoded_id,
                    batch_number,
                    is_last_batch,
                    payload_length,
                ) = unpack_header(new_message)

                if tipo_mensaje == TIPO_MENSAJE["NEW_CLIENT"]:
                    self._handle_new_client(encoded_id)

                elif tipo_mensaje == TIPO_MENSAJE["DISCONNECT_CLIENT"]:
                    self._handle_disconnect_client()

                elif (
                    tipo_mensaje == TIPO_MENSAJE["BATCH_MOVIES"]
                    or tipo_mensaje == TIPO_MENSAJE["BATCH_CREDITS"]
                    or tipo_mensaje == TIPO_MENSAJE["BATCH_RATINGS"]
                ):
                    try:
                        self._handle_batch_messages(
                            message_id,
                            tipo_mensaje,
                            encoded_id,
                            batch_number,
                            is_last_batch,
                            payload_length,
                        )
                    except Exception as e:
                        logger.error(
                            "Error in batch with: message_id=%s, tipo_mensaje=%s, batch_number=%s: %s",
                            message_id,
                            tipo_mensaje,
                            batch_number,
                            e,
                        )
                        continue

                else:
                    logger.warning("Unknown message type received: %s", tipo_mensaje)

            except OSError as e:
                if self._was_closed:
                    break
                logger.error("Error accepting new connection: %s", e)
                return

            except Exception as e:
                logger.error("Unexpected error: %s", e)
                return

    def _reap_completed_clients(self):
        try:
            logger.info("Checking for any completed clients...")
            for client in list(self._clients_connected.get_all().values()):
                if client.all_answers_sent:
                    logger.info(
                        "Removing completed client %s",
                        client.get_client_id(),
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
