import socket
import logging

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
