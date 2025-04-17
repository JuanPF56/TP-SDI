import socket

import common.receiver as receiver
import common.sender as sender

from common.logger import get_logger
logger = get_logger("Gateway")

SIZE_OF_UINT8 = 1
SIZE_OF_UINT32 = 4

class ProtocolGateway:
    def __init__(self, client_socket: socket.socket):
        self._client_socket = client_socket

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._client_socket is not None
    
    def receive_message_code(self) -> int:
        """
        Receive the message code from the client
        """
        message_code = receiver.receive_data(self._client_socket, SIZE_OF_UINT8)
        deserialize_message_code = int.from_bytes(message_code, byteorder="big")
        if deserialize_message_code == "":
            logger.error("Message code is empty")
            return None
        return deserialize_message_code
    
    def receive_amount_of_lines(self) -> int:
        """
        Receive the amount of lines from the client
        """
        amount_of_lines = receiver.receive_data(self._client_socket, SIZE_OF_UINT32)
        deserialize_amount_of_lines = int.from_bytes(amount_of_lines, byteorder="big")
        if deserialize_amount_of_lines == "":
            logger.error("Amount of lines is empty")
            return None
        return deserialize_amount_of_lines

    def receive_movie_data_len(self) -> str:
        """
        Receive the length of the movie data from the client
        """
        movie_data_len = receiver.receive_data(self._client_socket, SIZE_OF_UINT32)
        deserialize_movie_data_len = int.from_bytes(movie_data_len, byteorder="big")
        if deserialize_movie_data_len == "":
            logger.error("Data length is empty")
            return None
        return deserialize_movie_data_len
    
    def receive_movie_data(self, data_len: int) -> bytes:
        """
        Receive the movie data from the client
        """
        movie_data = receiver.receive_data(self._client_socket, data_len)
        if movie_data == "":
            logger.error("Movie data is empty")
            return None
        return movie_data
    
    def send_confirmation(self, message_code: int) -> None:
        """
        Send confirmation to the client
        """
        sender.send(self._client_socket, message_code.to_bytes(SIZE_OF_UINT8, byteorder="big"))