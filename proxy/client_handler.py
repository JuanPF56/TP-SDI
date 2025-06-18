# proxy/threads/client_handler.py

import socket
import struct
import threading
import uuid
import logging

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UUID
import common.receiver as receiver
import common.sender as sender
from common.logger import get_logger

logger = get_logger("client_handler")
logger.setLevel(logging.DEBUG)

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600


def _forward(source: socket.socket, destination: socket.socket):
    try:
        while True:
            logger.debug("Waiting from source...")
            header = receiver.receive_data(
                source, SIZE_OF_HEADER, timeout=TIMEOUT_HEADER
            )
            if len(header) != SIZE_OF_HEADER:
                logger.error(
                    "Incomplete header received from source, expected %d bytes, got %d",
                    SIZE_OF_HEADER,
                    len(header),
                )
                logger.error("Header content: %s", header)
                break
            if not header:
                logger.debug("Connection closed while reading header.")
                break

            (
                tipo_mensaje,
                encoded_id,
                _current_batch,
                is_last_batch_o_query_id,
                payload_len,
            ) = struct.unpack(">B36sIBI", header)

            if tipo_mensaje == 4:  # TIPO_MENSAJE.RESULTS
                logger.debug(
                    "Received results header: tipo_mensaje=%d, client_id=%s, query_id=%d, payload_len=%d",
                    tipo_mensaje,
                    encoded_id.decode("utf-8"),
                    is_last_batch_o_query_id,
                    payload_len,
                )

            payload = receiver.receive_data(
                source, payload_len, timeout=TIMEOUT_PAYLOAD
            )
            if len(payload) != payload_len:
                logger.error(
                    "Incomplete payload received, expected %d bytes, got %d",
                    payload_len,
                    len(payload),
                )
                break
            if not payload:
                logger.debug("Connection closed while reading payload.")
                break

            if tipo_mensaje == 4:  # TIPO_MENSAJE.RESULTS
                destination_id = encoded_id.decode("utf-8")
                logger.debug(
                    "Forwarding results to client %s with payload %s",
                    destination_id,
                    payload.decode("utf-8", errors="ignore"),
                )
            sender.send(destination, header)
            sender.send(destination, payload)

    except Exception as e:
        logger.error("Forwarding error: %s", e)


def client_handler(proxy, client_socket, addr, gateway):
    client_id = str(uuid.uuid4())
    host, port = gateway

    try:
        gateway_socket = socket.create_connection((host, port), timeout=None)
        logger.info("Connected to %s:%s for client %s", host, port, client_id)

        proxy._connected_clients[client_id] = (client_socket, gateway_socket)

        encoded_client_id = client_id.encode("utf-8")

        sender.send(gateway_socket, encoded_client_id)
        logger.info("Sent client ID %s to gateway %s:%s", client_id, host, port)

        sender.send(client_socket, encoded_client_id)
        logger.info("Sent client ID %s to client %s", encoded_client_id.decode(), addr)

        threading.Thread(
            target=_forward, args=(client_socket, gateway_socket), daemon=True
        ).start()
        threading.Thread(
            target=_forward, args=(gateway_socket, client_socket), daemon=True
        ).start()

    except Exception as e:
        logger.error("Error in client handler: %s", e)
        client_socket.close()
