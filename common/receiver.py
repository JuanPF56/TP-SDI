import socket

from common.logger import get_logger
logger = get_logger("Receiver")

MAX_EMPTY_READS = 5

def receive_data(socket_sender: socket.socket, num_bytes: int, timeout: int) -> bytes:
    """
    Receives exactly num_bytes bytes from the sender.
    Periodically checks if the connection is still active using connected_checker.
    If the sender closes connection mid-transfer or connection drops, returns what was received.
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
                    break

    except (OSError, socket.error) as e:
        logger.error(f"Socket receive error: {e}")
        raise ConnectionError(f"Receive failed: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise ConnectionError(f"Unexpected error: {e}")

    return data