import socket
import select
from typing import Callable

from common.logger import get_logger
logger = get_logger("Receiver")

MAX_EMPTY_READS = 5

def receive_data(socket_sender: socket.socket, num_bytes: int, connected_checker: Callable[[], bool], timeout: int) -> bytes:
    """
    Receives exactly num_bytes bytes from the sender.
    Periodically checks if the connection is still active using connected_checker.
    If the sender closes connection mid-transfer or connection drops, returns what was received.
    """
    data = b""
    empty_reads = 0

    try:
        while len(data) < num_bytes:
            if not connected_checker():
                logger.warning("Connection flagged as closed, stopping receive.")
                break

            readable, _, exceptional = select.select([socket_sender], [], [socket_sender], timeout)

            if exceptional:
                logger.error("Socket reported as exceptional, assuming disconnection.")
                break

            if not readable:
                # No data available to read
                empty_reads += 1
                logger.warning(f"No data available to read ({empty_reads}/{MAX_EMPTY_READS})...")
                if empty_reads >= MAX_EMPTY_READS:
                    logger.error("Max empty reads reached, assuming sender is disconnected.")
                    break
                continue

            logger.debug(f"Expecting {num_bytes} bytes, received {len(data)} bytes.")
            chunk = socket_sender.recv(num_bytes - len(data))
            logger.debug(f"Received chunk of size {len(chunk)} bytes.")

            if not chunk:
                logger.warning(f"Connection closed by sender while expecting {num_bytes} bytes, received {len(data)} bytes.")
                break

            data += chunk
            empty_reads = 0

    except (OSError, socket.error) as e:
        logger.error(f"Socket receive error: {e}")
        raise ConnectionError(f"Receive failed: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise ConnectionError(f"Unexpected error: {e}")

    return data