# proxy/threads/client_handler.py

import socket
import threading
import uuid
import logging

from gateway_connection import GatewayConnection

from common.protocol import (
    SIZE_OF_HEADER,
    TIPO_MENSAJE,
    SIZE_OF_HEADER_RESULTS,
    unpack_header,
    unpack_result_header,
)
import common.receiver as receiver
import common.sender as sender
from common.logger import get_logger

logger = get_logger("client_handler")
logger.setLevel(logging.INFO)

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600


def _forward_client_to_gateway(source: socket.socket, destination: GatewayConnection):
    logger.debug(
        "⬅️ FORWARD thread started: source=%s -> destination=%s",
        source.getsockname(),
        destination.host,
    )

    try:
        while True:
            # logger.debug("Waiting from source...")
            if source.fileno() == -1:
                logger.debug("Source socket is closed, exiting forward thread.")
                break
            if destination.fileno() == -1:
                logger.debug("Destination socket is closed, exiting forward thread.")
                break
            if not source or not destination:
                logger.debug(
                    "Source or destination socket is None, exiting forward thread."
                )
                break

            header = receiver.receive_data(
                source, SIZE_OF_HEADER, timeout=TIMEOUT_HEADER
            )
            if not header:
                logger.debug("Connection closed while reading header.")
                break
            if len(header) != SIZE_OF_HEADER:
                logger.error(
                    "Incomplete header received from source, expected %d bytes, got %d",
                    SIZE_OF_HEADER,
                    len(header),
                )
                logger.error("Header content: %s", header)
                break

            (
                message_id,
                tipo_mensaje,
                encoded_id,
                _current_batch,
                is_last_batch_o_query_id,
                payload_len,
            ) = unpack_header(header)

            if int(tipo_mensaje) not in TIPO_MENSAJE.values():
                logger.warning("Received unknown message type: %r", tipo_mensaje)

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

            destination.send(header)
            destination.send(payload)

    except Exception as e:
        logger.error("Forwarding error: %s", e)


def _forward_gateway_to_client(source: GatewayConnection, destination: socket.socket):
    logger.debug(
        "⬅️ FORWARD thread started: source=%s -> destination=%s",
        source.host,
        destination.getsockname(),
    )

    try:
        while True:
            logger.debug("Waiting from source...")
            if source.fileno() == -1:
                logger.error("Source socket is closed, exiting forward thread.")
                break
            if destination.fileno() == -1:
                logger.error("Destination socket is closed, exiting forward thread.")
                break
            if not source or not destination:
                logger.error(
                    "Source or destination socket is None, exiting forward thread."
                )
                break

            header = source.receive(SIZE_OF_HEADER_RESULTS, timeout=TIMEOUT_HEADER)
            logger.debug("Received header: %s, en hexa: %s", header, header.hex())

            if not header:
                logger.error("Connection closed while reading header.")
                break
            if len(header) != SIZE_OF_HEADER_RESULTS:
                logger.error(
                    "Incomplete header received from source, expected %d bytes, got %d",
                    SIZE_OF_HEADER_RESULTS,
                    len(header),
                )
                break

            tipo_mensaje, query_id, payload_len = unpack_result_header(header)

            if int(tipo_mensaje) == int(TIPO_MENSAJE["RESULTS"]):
                logger.debug(
                    "Received results header: tipo_mensaje=%d, query_id=%d, payload_len=%d",
                    tipo_mensaje,
                    query_id,
                    payload_len,
                )

            if int(tipo_mensaje) not in TIPO_MENSAJE.values():
                logger.warning("Received unknown message type: %r", tipo_mensaje)

            payload = source.receive(payload_len, timeout=TIMEOUT_PAYLOAD)
            if not payload:
                logger.error("Connection closed while reading payload.")
                break
            if len(payload) != payload_len:
                logger.error(
                    "Incomplete payload received, expected %d bytes, got %d",
                    payload_len,
                    len(payload),
                )
                break

            if int(tipo_mensaje) not in TIPO_MENSAJE.values() and payload:
                logger.error(
                    "Forwarding error PAYLOAD: %s",
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
        encoded_client_id = client_id.encode("utf-8")
        if len(encoded_client_id) > 36:
            logger.error(
                "Client ID %s exceeds maximum length of 36 bytes, truncating.",
                client_id,
            )
            encoded_client_id = encoded_client_id[:36]
        elif len(encoded_client_id) < 36:
            logger.warning(
                "Client ID %s is shorter than 36 bytes, padding with null bytes.",
                client_id,
            )
            encoded_client_id = encoded_client_id.ljust(36, b"\x00")

        gateway_conn = GatewayConnection(host, port, encoded_client_id, logger)
        logger.info("Connected to %s:%s for client %s", host, port, client_id)

        proxy._connected_clients[client_id] = (client_socket, gateway_conn)
        proxy.gateway_connections[(host, port)] = gateway_conn

        gateway_conn.send(encoded_client_id)
        logger.info(
            "Sent client ID %s as encoded %s to gateway %s:%s",
            client_id,
            encoded_client_id.decode("utf-8"),
            host,
            port,
        )

        sender.send(client_socket, encoded_client_id)
        logger.info(
            "Sent client ID %s as encoded %s to client %s",
            encoded_client_id.decode("utf-8"),
            client_id,
            addr,
        )

        threading.Thread(
            target=_forward_client_to_gateway,
            args=(client_socket, gateway_conn),
            daemon=True,
        ).start()

        threading.Thread(
            target=_forward_gateway_to_client,
            args=(gateway_conn, client_socket),
            daemon=True,
        ).start()

    except Exception as e:
        logger.error("Error in client handler: %s", e)
        client_socket.close()
