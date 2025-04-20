import socket
import logging

def send(sock: socket.socket, data: bytes) -> None:
    """
    Sends the data to the socket and handles any socket errors.
    """
    try:
        sock.sendall(data)
    except (BrokenPipeError, ConnectionResetError):
        logging.error("Connection closed by receiver")
    except socket.error as e:
        logging.error(f"Socket error: {e}")