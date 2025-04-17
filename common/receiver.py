import socket
import logging

SIZE_OF_UINT8 = 1

def receive_data(socket_sender: socket.socket, num_bytes: int) -> bytes:
    """
    Receives exactly num_bytes bytes from the sender.
    """
    data = b""
    while len(data) < num_bytes:
        chunk = socket_sender.recv(num_bytes - len(data))
        if not chunk:
            logging.error("Connection closed by sender")
            raise ConnectionError("Connection closed by sender")
        data += chunk
    return data

def receive_len(socket_sender: socket.socket) -> int:
    """
    Receives an unsigned 8-bit integer from the sender.
    The integer is sent in network byte order.
    """
    length_bytes = receive_data(socket_sender, SIZE_OF_UINT8)
    return int.from_bytes(length_bytes, byteorder="big")

def receive_string(socket_sender: socket.socket) -> str:
    """
    Receives a string from the sender preceded by its length.
    The string is encoded in UTF-8.
    """
    length = receive_len(socket_sender)
    data = receive_data(socket_sender, length)
    return data.decode("utf-8")
