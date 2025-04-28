import socket

from common.logger import get_logger
logger = get_logger("Sender")

def send(sock: socket.socket, data: bytes) -> None:
    """
    Sends the data to the socket and handles any socket errors.
    """
    try:
        if sock.fileno() != -1:
            sock.sendall(data)
        else:
            logger.warning("Attempted to send on a closed socket")
        
    except (BrokenPipeError, ConnectionResetError):
        logger.error("Connection closed by receiver")
        raise ConnectionError("Connection closed by receiver")
    
    except socket.error as e:
        logger.error(f"Socket error: {e}")
        raise ConnectionError(f"Socket error: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise ConnectionError(f"Unexpected error: {e}")