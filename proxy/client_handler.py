"""ClientHandler class to manage communication between clients and gateways."""

import threading
import uuid
import logging
import traceback

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


class ClientHandler:
    def __init__(self, proxy, client_socket, addr, gateway_id, client_id=None):
        self.proxy = proxy
        self.client_socket = client_socket
        self.addr = addr
        self.gateway_id = gateway_id

        self.client_id = client_id or str(uuid.uuid4())
        self.gateway_socket = self.proxy._gateways_connected.get(gateway_id)
        if not self.gateway_socket:
            raise RuntimeError(
                f"No gateway socket found for gateway ID {self.gateway_id}"
            )

    def start(self):
        try:
            self._register_client()

            threading.Thread(
                target=self._forward_client_to_gateway,
                daemon=True,
            ).start()

        except Exception as e:
            logger.error("Error in client handler: %s", e)
            logger.debug("Exception traceback:\n%s", traceback.format_exc())
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
        payload_length = len(encoded_id)

        header = pack_header(
            message_id=0,
            tipo_de_mensaje=TIPO_MENSAJE["NEW_CLIENT"],
            encoded_id=encoded_id,
            batch_number=0,
            is_last_batch=0,
            payload_length=payload_length,
        )
        sender.send(self.client_socket, encoded_id)

        # Registrar en estructuras del proxy
        self.proxy._connected_clients[self.client_id] = (
            self.client_socket,
            self.gateway_socket,
            self.addr,
        )
        self.proxy._clients_per_gateway[self.gateway_id].append(self.client_id)

        # Asegurar longitud 36 bytes
        if len(encoded_id) > 36:
            logger.error("Client ID %s too long, truncating.", self.client_id)
            encoded_id = encoded_id[:36]
        elif len(encoded_id) < 36:
            logger.warning("Client ID %s too short, padding.", self.client_id)
            encoded_id = encoded_id.ljust(36, b"\x00")

        with self.proxy._gateway_locks[self.gateway_id]:
            sender.send(self.gateway_socket, header)

        logger.info(
            "Sent client ID %s to gateway and client %s", self.client_id, self.addr
        )

    def _forward_client_to_gateway(self):
        logger.debug("FORWARD client ➡️ gateway")

        while True:
            try:

                if (
                    self.client_socket.fileno() == -1
                    or self.gateway_socket.fileno() == -1
                ):
                    break

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

                lock = self.proxy._gateway_locks[self.gateway_id]
                with lock:
                    sender.send(self.gateway_socket, header)
                    sender.send(self.gateway_socket, payload)

            except Exception as e:
                logger.error("Forward client -> gateway failed: %s", e)
                break
