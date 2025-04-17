import common.receiver as receiver
import common.sender as sender
import socket

from common.logger import get_logger
logger = get_logger("Client")

SIZE_OF_UINT32 = 4

class ProtocolClient:
    def __init__(self, socket: socket.socket):
        self._socket = socket

    def send_csv_line(self, line: str):
        """
        Sends a CSV line to the server.
        First sends the length of the line as a 4-byte unsigned integer,
        followed by the UTF-8 encoded line.
        """
        try:
            encoded = line.encode("utf-8")
            length = len(encoded)

            sender.send(self._socket, length.to_bytes(SIZE_OF_UINT32, byteorder="big"))
            sender.send(self._socket, encoded)

            logger.info(f"Sent line of {length} bytes")
            logger.debug(f"Line content: {line}")
        except Exception as e:
            logger.error(f"Error sending CSV line: {e}")

    def receive(self):
        try:
            data = receiver.receive_string(self._socket)
            return data
        except Exception as e:
            logger.error(f"Failed to receive data: {e}")
            raise