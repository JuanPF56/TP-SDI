"""ClientHandler class to manage communication between clients and gateways."""

import threading
import uuid
import logging
import time
from collections import deque

from common.protocol import (
    SIZE_OF_HEADER,
    TIPO_MENSAJE,
    pack_header,
    unpack_header,
)
import common.receiver as receiver
import common.sender as sender
from common.logger import get_logger

logger = get_logger("client_handler")
logger.setLevel(logging.INFO)

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600

RETRY_INTERVAL = 2
MAX_BUFFER_SIZE = 1000  # Maximum number of messages to buffer per client


class BufferedMessage:
    """Represents a buffered message with header and payload."""

    def __init__(self, header, payload, timestamp=None):
        self.header = header
        self.payload = payload
        self.timestamp = timestamp or time.time()


class ClientHandler(threading.Thread):
    def __init__(self, proxy, client_socket, addr, gateway_id, client_id=None):
        super().__init__(daemon=True)
        self.proxy = proxy
        self.client_socket = client_socket
        self.addr = addr
        self.gateway_id = gateway_id
        self._stop_flag = threading.Event()

        self.client_id = client_id or str(uuid.uuid4())
        self.gateway_socket = self.proxy._gateways_connected.get(gateway_id)
        if not self.gateway_socket:
            raise RuntimeError(
                f"No gateway socket found for gateway ID {self.gateway_id}"
            )

        # Message buffer for when gateway is down
        self.message_buffer = deque(maxlen=MAX_BUFFER_SIZE)
        self.buffer_lock = threading.Lock()
        self.gateway_available = threading.Event()

        # Start buffer flush thread
        self.buffer_thread = threading.Thread(
            target=self._buffer_flush_worker, daemon=True
        )
        self.buffer_thread.start()

    def run(self):
        try:
            self._register_client()
            self._forward_client_to_gateway()

        except Exception as e:
            logger.error("Error in client handler: %s", e)
            self.client_socket.close()

    def _register_client(self):
        logger.info(
            "ClientHandler starting with client_id=%s gateway_id=%s",
            self.client_id,
            self.gateway_id,
        )

        logger.info(
            "Connected client %s to gateway %s:%s",
            self.client_id,
            *self.gateway_socket.getsockname(),
        )

        encoded_id = self.client_id.encode("utf-8")
        # Asegurar longitud 36 bytes
        if len(encoded_id) > 36:
            logger.error("Client ID %s too long, truncating.", self.client_id)
            encoded_id = encoded_id[:36]
        elif len(encoded_id) < 36:
            logger.warning("Client ID %s too short, padding.", self.client_id)
            encoded_id = encoded_id.ljust(36, b"\x00")

        sender.send(self.client_socket, encoded_id)

        # Registrar en estructuras del proxy
        self.proxy._connected_clients[self.client_id] = (
            self.client_socket,
            self.gateway_socket,
            self.addr,
        )
        self.proxy._clients_per_gateway[self.gateway_id].append(self.client_id)

        header = pack_header(
            message_id=0,
            tipo_de_mensaje=TIPO_MENSAJE["NEW_CLIENT"],
            encoded_id=encoded_id,
            batch_number=0,
            is_last_batch=0,
            payload_length=0,  # no payloadneeded, client_id is in header
        )
        with self.proxy._gateway_locks[self.gateway_id]:
            sender.send(self.gateway_socket, header)

        logger.info(
            "Sent client ID %s to gateway and client %s", self.client_id, self.addr
        )

    def _is_gateway_available(self):
        """Check if the gateway is currently available."""
        current_client_info = self.proxy._connected_clients.get(self.client_id)
        if not current_client_info:
            return False

        _, current_gateway_socket, _ = current_client_info
        return current_gateway_socket and current_gateway_socket.fileno() != -1

    def _buffer_message(self, header, payload):
        """Add a message to the buffer."""
        with self.buffer_lock:
            if len(self.message_buffer) >= MAX_BUFFER_SIZE:
                # Remove oldest message if buffer is full
                dropped = self.message_buffer.popleft()
                logger.warning(
                    "Buffer full for client %s, dropping oldest message (age: %.2fs)",
                    self.client_id,
                    time.time() - dropped.timestamp,
                )

            buffered_msg = BufferedMessage(header, payload)
            self.message_buffer.append(buffered_msg)
            logger.info(
                "Buffered message for client %s (buffer size: %d)",
                self.client_id,
                len(self.message_buffer),
            )

    def _buffer_flush_worker(self):
        """Background thread to flush buffered messages when gateway becomes available."""
        while not self._stop_flag.is_set():
            try:
                # Wait for gateway to become available or stop signal
                if not self._is_gateway_available():
                    time.sleep(RETRY_INTERVAL)
                    continue

                # Gateway is available, flush buffer
                messages_to_send = []
                with self.buffer_lock:
                    while self.message_buffer:
                        messages_to_send.append(self.message_buffer.popleft())

                if messages_to_send:
                    logger.info(
                        "Flushing %d buffered messages for client %s",
                        len(messages_to_send),
                        self.client_id,
                    )

                    success_count = 0
                    for msg in messages_to_send:
                        if self._send_to_gateway(msg.header, msg.payload):
                            success_count += 1
                        else:
                            # Gateway went down again, re-buffer remaining messages
                            with self.buffer_lock:
                                self.message_buffer.appendleft(msg)
                                # Re-buffer remaining messages
                                for remaining_msg in messages_to_send[
                                    success_count + 1 :
                                ]:
                                    if len(self.message_buffer) < MAX_BUFFER_SIZE:
                                        self.message_buffer.append(remaining_msg)
                            break

                    if success_count > 0:
                        logger.info(
                            "Successfully sent %d/%d buffered messages for client %s",
                            success_count,
                            len(messages_to_send),
                            self.client_id,
                        )

                time.sleep(0.1)  # Small delay to prevent busy waiting

            except Exception as e:
                logger.error(
                    "Error in buffer flush worker for client %s: %s", self.client_id, e
                )
                time.sleep(RETRY_INTERVAL)

    def _send_to_gateway(self, header, payload):
        """Send message to gateway. Returns True if successful, False otherwise."""
        try:
            current_client_info = self.proxy._connected_clients.get(self.client_id)
            if not current_client_info:
                return False

            _, current_gateway_socket, _ = current_client_info

            if not current_gateway_socket or current_gateway_socket.fileno() == -1:
                return False

            lock = self.proxy._gateway_locks[self.gateway_id]
            with lock:
                sender.send(current_gateway_socket, header)
                sender.send(current_gateway_socket, payload)

            return True

        except Exception as e:
            logger.error("Failed to send message to gateway: %s", e)
            return False

    def _forward_client_to_gateway(self):
        logger.debug("FORWARD client ➡️ gateway")

        while not self._stop_flag.is_set():
            try:
                # Get the current gateway socket (it might change due to reconnection)
                current_client_info = self.proxy._connected_clients.get(self.client_id)
                if not current_client_info:
                    logger.warning(
                        "Client %s no longer in connected clients or suspended",
                        self.client_id,
                    )
                    time.sleep(RETRY_INTERVAL)
                    continue

                client_socket, current_gateway_socket, _ = current_client_info

                if client_socket.fileno() == -1:
                    logger.info("Client %s socket is closed", self.client_id)
                    break

                # Receive message from client
                header = receiver.receive_data(
                    self.client_socket, SIZE_OF_HEADER, timeout=TIMEOUT_HEADER
                )
                if not header or len(header) != SIZE_OF_HEADER:
                    break

                (
                    message_id,
                    tipo_mensaje,
                    encoded_id,
                    _current_batch,
                    is_last_batch_o_query_id,
                    payload_len,
                ) = unpack_header(header)

                payload = receiver.receive_data(
                    self.client_socket, payload_len, timeout=TIMEOUT_PAYLOAD
                )
                if not payload or len(payload) != payload_len:
                    break

                # Try to send directly to gateway first
                if self._is_gateway_available():
                    if self._send_to_gateway(header, payload):
                        logger.debug(
                            "Forwarded message ID %s from client %s to gateway %s",
                            message_id,
                            self.client_id,
                            self.gateway_id,
                        )
                        continue

                # Gateway is down or send failed, buffer the message
                logger.warning(
                    "Gateway %s is down, buffering message ID %s from client %s",
                    self.gateway_id,
                    message_id,
                    self.client_id,
                )
                self._buffer_message(header, payload)

            except receiver.ReceiverError:
                logger.warning(
                    "Client socket closed the sending connection, closing proxy side"
                )
                self._stop_flag.set()
                self.client_socket.close()
                break

            except Exception as e:
                logger.error("Forward client -> gateway failed: %s", e)
                self._stop_flag.set()
                break

    def stop(self):
        """Stop the client handler and flush any remaining buffered messages."""
        self._stop_flag.set()

        # Try to flush remaining messages one last time
        if self._is_gateway_available():
            with self.buffer_lock:
                remaining_messages = len(self.message_buffer)
                if remaining_messages > 0:
                    logger.info(
                        "Attempting to flush %d remaining messages for client %s",
                        remaining_messages,
                        self.client_id,
                    )

                    while self.message_buffer:
                        msg = self.message_buffer.popleft()
                        if not self._send_to_gateway(msg.header, msg.payload):
                            break

        # Log any messages that couldn't be sent
        with self.buffer_lock:
            if self.message_buffer:
                logger.warning(
                    "Client %s stopping with %d unsent buffered messages",
                    self.client_id,
                    len(self.message_buffer),
                )
