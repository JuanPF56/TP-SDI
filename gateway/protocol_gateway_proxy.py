"""
ProtocolGateway
This module implements the ProtocolGatewayProxy class, which handles communication between the current Gateway and the Proxy.
"""

import socket
import threading

from raw_message import RawMessage

import common.receiver as receiver
import common.sender as sender
from common.protocol import (
    SIZE_OF_HEADER,
    TIPO_MENSAJE,
)
from common.logger import get_logger

logger = get_logger("Protocol Gateway-Proxy")

TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

TIMEOUT_HEADER = 300
TIMEOUT_PAYLOAD = 300


class ProtocolGatewayProxy:
    def __init__(
        self,
        gateway_socket: socket.socket,
    ):
        self._gateway_socket = gateway_socket

    def gateway_is_connected(self) -> bool:
        """
        Check if the gateway is connected
        """
        return self._gateway_socket is not None and self._gateway_socket.fileno() != -1

    def send_gateway_number(self, gateway_number: int) -> bool:
        """
        Send the gateway number to the client.
        """
        if not self.gateway_is_connected():
            logger.error("Gateway socket is not connected")
            return False

        try:
            sender.send(
                self._gateway_socket, gateway_number.to_bytes(1, byteorder="big")
            )
            logger.info("Sent gateway number: %d", gateway_number)
            return True

        except sender.SenderConnectionLostError as e:
            logger.error("Connection error while sending gateway number: %s", e)
            return False

        except sender.SenderError as e:
            logger.error("Connection error while sending gateway number: %s", e)
            return False

        except Exception as e:
            logger.error("Unexpected error while sending gateway number: %s", e)
            return False

    def receive_message(self) -> RawMessage | None:
        """
        Receive a message from the gateway.
        Returns a tuple containing the message type and the payload.
        """
        if not self.gateway_is_connected():
            logger.error("Gateway socket is not connected")
            return None

        try:
            header = receiver.receive_data(
                self._gateway_socket, SIZE_OF_HEADER, TIMEOUT_HEADER
            )
            if not header:
                return None

            raw_message = RawMessage(header)
            raw_message.unpack_header()

            payload = receiver.receive_data(
                self._gateway_socket, raw_message.payload_length, TIMEOUT_PAYLOAD
            )

            raw_message.add_payload(payload)

            logger.info(
                "Received message type: %s with size: %d",
                TIPO_MENSAJE_INVERSO[raw_message.tipo_mensaje],
                raw_message.payload_length,
            )
            return raw_message

        except TimeoutError:
            logger.warning("Timeout while waiting to receive a message")
            return None

        except receiver.ReceiverError as e:
            logger.error("Error while receiving message: %s", e)
            return None

        except Exception as e:
            logger.error("Unexpected error while receiving message: %s", e)
            return None
