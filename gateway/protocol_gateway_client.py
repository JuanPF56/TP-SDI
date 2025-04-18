import socket
import struct

import common.receiver as receiver
import common.sender as sender

from common.logger import get_logger
logger = get_logger("Gateway Protocol")

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UINT8, TIPO_MENSAJE
TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

class ProtocolGateway:
    def __init__(self, client_socket: socket.socket):
        self._client_socket = client_socket

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._client_socket is not None
    
    def receive_header(self) -> tuple | None:
        try:
            header = receiver.receive_data(self._client_socket, SIZE_OF_HEADER)
            if not header or len(header) != SIZE_OF_HEADER:
                logger.error("Invalid or incomplete header received")
                return None

            type_of_batch, current_batch, is_last_batch, payload_len = struct.unpack(">BI B I", header)
            message_code = TIPO_MENSAJE_INVERSO.get(type_of_batch)

            if message_code is None:
                logger.error(f"Unknown message code: {type_of_batch}")
                return None

            return message_code, current_batch, is_last_batch, payload_len

        except Exception as e:
            logger.error(f"Error receiving header: {e}")
            return None
            
    def receive_payload(self, payload_len: int) -> bytes:
        """
        Receive the payload from the client
        """
        if payload_len > 0:
            data = receiver.receive_data(self._client_socket, payload_len)
            if not data or len(data) != payload_len:
                logger.error("Invalid or incomplete data received")
                return None
            return data
        else:
            logger.error("Payload length is zero")
            return None
    
    def process_payload(self, payload: bytes) -> None:
        """
        Process the payload
        """
        decoded_payload = payload.decode("utf-8")
        lines = decoded_payload.split("\n")
        
        for i, line in enumerate(lines[:3]):
            logger.debug(f"  Line {i + 1}: {line.replace(chr(0), ' | ')}")
        if len(lines) > 3:
            logger.debug(f"  ... ({len(lines) - 3} more lines)")
    
    def send_confirmation(self, message_code: int) -> None:
        """
        Send confirmation to the client
        """
        sender.send(self._client_socket, message_code.to_bytes(SIZE_OF_UINT8, byteorder="big"))