import socket

from common.logger import get_logger
logger = get_logger("Receiver")

def receive_data(socket_sender: socket.socket, num_bytes: int) -> bytes:
    """
    Receives exactly num_bytes bytes from the sender.
    """
    data = b""
    try:
        while len(data) < num_bytes:
            chunk = socket_sender.recv(num_bytes - len(data))
            if not chunk:
                logger.error("Connection closed by sender")
                raise ConnectionError("Connection closed by sender")
            data += chunk
    except (OSError, socket.error) as e:
        logger.error(f"Socket receive error: {e}")
        raise ConnectionError(f"Receive failed: {e}")
    return data
