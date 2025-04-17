import common.receiver as receiver
import common.sender as sender
import socket

import os
import csv

import struct
TIPO_BATCH = {
    "BATCH_MOVIES": 1,
    "BATCH_ACTORS": 2,
    "BATCH_RATINGS": 3,
}

HEADER_SIZE = 1 + 2 + 2 + 4  # tipo_de_mensaje (1 byte) + total_de_batches (2 bytes) + nro_batch_actual (2 bytes) + payload_len (4 bytes)

from common.logger import get_logger
logger = get_logger("Protocol Client")

SIZE_OF_UINT8 = 1
SIZE_OF_UINT32 = 4

class ProtocolClient:
    def __init__(self, socket: socket.socket, max_batch_size):
        self._socket = socket
        self._max_batch_size = max_batch_size

    def _is_connected(self) -> bool:
        """
        Check if the socket is connected.
        This is a simple check to see if the socket is still open.
        """
        try:
            self._socket.getpeername()
            return True
        except socket.error:
            return False
        
    def send_dataset(self, dataset_path, dataset_name, message_type_str):
        logger.info(f"Sending dataset {dataset_name} as {message_type_str}...")

        tipo_de_mensaje = TIPO_BATCH[message_type_str]
        csv_path = os.path.join(dataset_path, f"{dataset_name}.csv")
        if not os.path.exists(csv_path):
            logger.error(f"Dataset {dataset_name} not found at {csv_path}")
            return

        try:
            with open(csv_path, newline='', encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile, quotechar='"', delimiter=',', skipinitialspace=True)
                headers = next(reader)
                max_payload_size = self._max_batch_size - HEADER_SIZE

                batches = []
                current_batch = []
                current_size = 0

                for row in reader:
                    line = "\0".join(row)
                    encoded_line = (line + "\n").encode("utf-8")
                    line_size = len(encoded_line)

                    if current_size + line_size > max_payload_size:
                        batches.append(current_batch)
                        current_batch = []
                        current_size = 0

                    current_batch.append(line)
                    current_size += line_size

                if current_batch:
                    batches.append(current_batch)

                total_batches = len(batches)
                logger.info(f"Total batches for {dataset_name}: {total_batches}")

                for idx, batch in enumerate(batches):
                    payload = "\n".join(batch).encode("utf-8")
                    payload_len = len(payload)

                    # Build header: tipo_de_mensaje (1 byte) + total_de_batches (2 bytes) + nro_batch_actual (2 bytes) + payload_len (4 bytes)
                    header = struct.pack(">BHHI", tipo_de_mensaje, total_batches, idx + 1, payload_len)
                    message = header + payload

                    logger.info(f"Sending batch {idx + 1}/{total_batches} of size {payload_len} bytes")
                    self.send_batch(message)

        except Exception as e:
            logger.error(f"Error reading/sending CSV: {e}")
    

    def send_batch(self, batch_data: bytes):
        try:
            length = len(batch_data)

            if length > self._max_batch_size:
                raise ValueError(f"Batch size {length} exceeds max allowed {self._max_batch_size}")

            sender.send(self._socket, length.to_bytes(SIZE_OF_UINT32, byteorder="big"))
            sender.send(self._socket, batch_data)

            logger.debug(f"Sent block of {length} bytes")

            # Logging solo los primeros N registros para debug legible
            N = 3
            try:
                decoded = batch_data.decode("utf-8")
                lines = decoded.split("\n")
                logger.debug(f"Preview of first {N} lines in batch:")
                for i, line in enumerate(lines[:N]):
                    readable = line.replace("\0", " | ")
                    logger.debug(f"  Line {i + 1}: {readable}")
                if len(lines) > N:
                    logger.debug(f"  ... ({len(lines) - N} more lines not shown)")
            except Exception as decode_err:
                pass

            logger.debug(f"{'-'*50}")
        except Exception as e:
            logger.error(f"Error sending CSV batch: {e}")

    def receive_confirmation(self):
        """
        Receives a confirmation message from the server.
        The confirmation message is expected to be a 1-byte unsigned integer.
        """
        logger.info("Awaiting confirmation from server...")
        try:
            data = receiver.receive_data(self._socket, SIZE_OF_UINT8)
            confirmation = int.from_bytes(data, byteorder="big")
            logger.debug(f"Received confirmation: {confirmation}")
            return confirmation
        except Exception as e:
            logger.error(f"Error receiving confirmation: {e}")
            return None