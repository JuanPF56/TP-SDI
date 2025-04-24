import socket
import logging

def send(sock: socket.socket, data: bytes) -> None:
    """
    Sends the data to the socket and handles any socket errors.
    """
    try:
        if sock.fileno() != -1:
            sock.sendall(data)
        else:
            logging.warning("Attempted to send on a closed socket")
    except (BrokenPipeError, ConnectionResetError):
        logging.error("Connection closed by receiver")
    except socket.error as e:
        logging.error(f"Socket error: {e}")