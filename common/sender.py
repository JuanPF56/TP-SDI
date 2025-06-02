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
            raise SenderConnectionLostError("Socket is closed")

    except (BrokenPipeError, ConnectionResetError):
        logger.error("Connection closed by receiver")
        raise SenderConnectionLostError("Connection closed by receiver")

    except socket.error as e:
        logger.error("Error in Socket sender: %s", e)
        raise SenderError(f"Error in Socket sender: {e}")
    except Exception as e:
        logger.error("Unexpected error in sender: %s", e)
        raise SenderError(f"Unexpected error in sender: {e}")


# Exception classes for the Sender module
class SenderConnectionLostError(Exception):
    """Exception raised when the connection is lost during sending."""

    pass


class SenderError(Exception):
    """Exception raised for errors in the Sender module."""

    pass
