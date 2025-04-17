import common.receiver as receiver
import common.sender as sender
import socket

from common.logger import get_logger
logger = get_logger("Client")

SIZE_OF_UINT8 = 1
SIZE_OF_UINT32 = 4

class ProtocolClient:
    def __init__(self, socket: socket.socket):
        self._socket = socket

    def send_amount_of_lines(self, amount: int):
        """
        Sends the amount of lines to the server.
        The amount is sent as a 4-byte unsigned integer.
        """
        try:
            sender.send(self._socket, amount.to_bytes(SIZE_OF_UINT32, byteorder="big"))
            logger.debug(f"Sent amount of lines: {amount}")
        except Exception as e:
            logger.error(f"Error sending amount of lines: {e}")

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

            logger.debug(f"Sent line of {length} bytes")
            logger.debug(f"Line content: {line}")
        except Exception as e:
            logger.error(f"Error sending CSV line: {e}")

    def receive_confirmation(self):
        """
        Receives a confirmation message from the server.
        The confirmation message is expected to be a 1-byte unsigned integer.
        """
        try:
            data = receiver.receive_data(self._socket, SIZE_OF_UINT8)
            confirmation = int.from_bytes(data, byteorder="big")
            logger.debug(f"Received confirmation: {confirmation}")
            return confirmation
        except Exception as e:
            logger.error(f"Error receiving confirmation: {e}")
            return None