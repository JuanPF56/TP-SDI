import socket
import signal
import os
import time
import threading
import queue

from protocol_gateway_proxy import ProtocolGatewayProxy
from raw_message import RawMessage
from message_queue import MessageQueue
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
PROCESSING_THREAD_TIMEOUT = 30


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
        self._result_dispatcher = None
        self._setup_result_dispatcher()

        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])

        # Initialize message queue and processing thread
        self._message_queue = MessageQueue(f"/tmp/gateway_{self.node_name}_messages")
        self._processing_thread = None
        self._start_message_processor()

        self.restore_connected_clients()

        try:
            with open(f"/tmp/{self.node_name}_ready", "w", encoding="utf-8") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")

            # Log recovery status
            queued_messages = self._message_queue.qsize()
            queued_results = (
                self._result_dispatcher.get_queued_results_count()
                if self._result_dispatcher
                else 0
            )
            if queued_messages > 0 or queued_results > 0:
                logger.info(
                    "Recovery status: %d queued messages, %d queued results",
                    queued_messages,
                    queued_results,
                )
            else:
                logger.info("No pending messages or results to recover")

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
                    config=self.config,
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
            self.config,
            clients_connected=self._clients_connected,
            gateway_protocol=self._protocol,
        )
        self._result_dispatcher.start()

    def _start_message_processor(self):
        """Start the message processing thread"""
        self._processing_thread = threading.Thread(
            target=self._process_messages, name="MessageProcessor", daemon=True
        )
        self._processing_thread.start()
        logger.info("Message processor thread started")

    def _process_messages(self):
        """Process messages from queue in separate thread"""
        logger.info("Message processor started")

        while not self._was_closed:
            try:
                # Get message from queue with timeout
                message_id, raw_message = self._message_queue.get(timeout=1.0)

                logger.debug(
                    "Processing message %s (type: %s)",
                    message_id,
                    raw_message.tipo_mensaje,
                )

                # Process the message
                success = self._process_raw_message(raw_message)

                if success:
                    # Mark as done and remove from disk
                    self._message_queue.task_done(message_id)
                else:
                    logger.error("Failed to process message %s", message_id)
                    # You could implement retry logic here
                    self._message_queue.task_done(message_id)  # Remove even if failed

            except queue.Empty:
                # Timeout waiting for message, continue loop
                continue
            except Exception as e:
                logger.error("Error in message processor: %s", e)
                # Continue processing other messages
                continue

        logger.info("Message processor stopped")

    def _process_raw_message(self, raw_message: RawMessage) -> bool:
        """Process a single raw message. Returns True if successful."""
        try:
            if raw_message.tipo_mensaje == TIPO_MENSAJE["NEW_CLIENT"]:
                self._handle_new_client(raw_message.encoded_id)
                return True

            elif (
                raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_MOVIES"]
                or raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_CREDITS"]
                or raw_message.tipo_mensaje == TIPO_MENSAJE["BATCH_RATINGS"]
            ):
                return self._handle_batch_messages(raw_message)

            else:
                logger.warning(
                    "Unknown message type received: %s", raw_message.tipo_mensaje
                )
                return False

        except Exception as e:
            logger.error(
                "Error processing message (type: %s): %s", raw_message.tipo_mensaje, e
            )
            return False

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
                    config=self.config,
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
                config=self.config,
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
                gateway_status = self.get_status()
                logger.info("Gateway status: %s", gateway_status)

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

                # Reset delay on successful message reception
                delay = DELAY_RETRY_RECEIVE_MESSAGE

                # Queue the message for processing
                message_id = self._message_queue.put(raw_message)
                logger.debug(
                    "Message %s queued (queue size: %d)",
                    message_id,
                    self._message_queue.qsize(),
                )

            except OSError as e:
                if self._was_closed:
                    break
                logger.error("Error receiving message: %s", e)
                # You might want to reconnect here
                break

            except Exception as e:
                logger.error("Unexpected error in main loop: %s", e)
                break

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping server...")
        self._stop_server()

    def _stop_server(self):
        logger.info("Stopping server...")

        # Stop accepting new messages
        self._was_closed = True

        # Wait for processing thread to finish current messages
        if self._processing_thread and self._processing_thread.is_alive():
            logger.info("Waiting for message processor to finish...")
            self._processing_thread.join(timeout=PROCESSING_THREAD_TIMEOUT)
            if self._processing_thread.is_alive():
                logger.warning("Message processor did not finish within timeout")

        # Stop result dispatcher (this will also stop the result sender thread)
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

    def get_status(self) -> dict:
        """Get gateway status including queue information"""
        return {
            "gateway_connected": self.gateway_is_connected(),
            "clients_connected": (
                self._clients_connected.count() if self._clients_connected else 0
            ),
            "queued_messages": (
                self._message_queue.qsize() if self._message_queue else 0
            ),
            "queued_results": (
                self._result_dispatcher.get_queued_results_count()
                if self._result_dispatcher
                else 0
            ),
            "was_closed": self._was_closed,
        }
