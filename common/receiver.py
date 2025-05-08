import socket

from common.logger import get_logger
logger = get_logger("Receiver")

MAX_EMPTY_READS = 5

def receive_data(socket_sender: socket.socket, num_bytes: int, timeout: int) -> bytes:
    """
    Receives exactly num_bytes bytes from the sender.
    If the sender closes the connection before sending all bytes,
    it will return the bytes received so far.
    If the sender does not send any bytes for a certain period,
    it will raise a timeout error.
    """
    data = b""
    empty_reads = 0

    socket_sender.settimeout(timeout)
    try:
        while len(data) < num_bytes:

            logger.debug(f"Expecting {num_bytes} bytes, received {len(data)} bytes so far.")
            try:
                chunk = socket_sender.recv(num_bytes - len(data))
                logger.debug(f"Received chunk of size {len(chunk)} bytes.")

                if not chunk:
                    logger.warning(f"Connection closed by sender while expecting {num_bytes} bytes, received {len(data)} bytes.")
                    break

                data += chunk
                empty_reads = 0  # reset empty read counter

            except socket.timeout:
                empty_reads += 1
                logger.warning(f"Socket timeout ({empty_reads}/{MAX_EMPTY_READS})...")
                if empty_reads >= MAX_EMPTY_READS:
                    logger.error("Max empty reads reached, sender may have disconnected.")
                    raise TimeoutError("Max empty reads reached, sender may have disconnected.")

    except (OSError, socket.error) as e:
        logger.error(f"Socket receive error: {e}")
        raise ReceiverError(f"Socket receive failed: {e}")

    except Exception as e:
        logger.error(f"Unexpected error in receiver: {e}")
        raise ReceiverError(f"Unexpected error in receiver: {e}")

    return data


# Exception classes for receiver errors
class ReceiverError(Exception):
    """Base class for receiver-related exceptions."""
    pass