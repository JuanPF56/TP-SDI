import common.receiver as receiver
import common.sender as sender
import socket

import os
import csv

import struct

from common.logger import get_logger
logger = get_logger("Protocol Client")

from common.protocol import TIPO_MENSAJE, SIZE_OF_HEADER, SIZE_OF_UINT8

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

        tipo_de_mensaje = TIPO_MENSAJE[message_type_str]
        csv_path = os.path.join(dataset_path, f"{dataset_name}.csv")

        if not os.path.exists(csv_path):
            logger.error(f"Dataset {dataset_name} not found at {csv_path}")
            return

        try:
            batches = self._build_batches(csv_path, dataset_name)
            total_batches = len(batches)
            logger.info(f"Total batches for {dataset_name}: {total_batches}")

            for idx, batch in enumerate(batches):
                payload = "\n".join(batch).encode("utf-8")
                payload_len = len(payload)

                header = struct.pack(">BHHI", tipo_de_mensaje, total_batches, idx + 1, payload_len)
                if len(header) != SIZE_OF_HEADER:
                    raise ValueError(f"Header size {len(header)} does not match expected {SIZE_OF_HEADER}")

                logger.debug(f"{dataset_name} - Sending batch {idx + 1}/{total_batches} of size {payload_len} bytes")
                self.send_batch(header, payload)

        except Exception as e:
            logger.error(f"Error reading/sending CSV: {e}")

    def _build_batches(self, csv_path, dataset_name):
        batches = []
        max_payload_size = self._max_batch_size - SIZE_OF_HEADER

        with open(csv_path, newline='', encoding="utf-8") as csvfile:
            if dataset_name == "credits":
                lines = csvfile.readlines()
                lines = lines[1:] # Skip header
                current_batch, current_payload = [], bytearray()

                for line in lines:
                    line_bytes = line.encode("utf-8")

                    if len(line_bytes) > max_payload_size:
                        logger.debug(f"Fragmenting oversized line (size={len(line_bytes)})")

                        start = 0
                        while start < len(line_bytes):
                            end = min(start + max_payload_size, len(line_bytes))
                            chunk = line_bytes[start:end]
                            chunk_str = chunk.decode("utf-8", errors="replace")

                            if current_payload and len(current_payload) + 1 + len(chunk) > max_payload_size:
                                batches.append(current_batch)
                                current_batch, current_payload = [], bytearray()

                            if current_payload:
                                current_payload += b"\n"
                            current_payload += chunk
                            current_batch.append(chunk_str)
                            start = end

                    else:
                        extra_bytes = b"\n" if current_payload else b""
                        projected_payload = current_payload + extra_bytes + line_bytes

                        if len(projected_payload) > max_payload_size:
                            batches.append(current_batch)
                            current_batch, current_payload = [], bytearray()

                        if current_payload:
                            current_payload += b"\n"
                        current_payload += line_bytes
                        current_batch.append(line.rstrip("\n"))

                if current_batch:
                    batches.append(current_batch)

            else:
                reader = csv.reader(csvfile, quotechar='"', delimiter=',', skipinitialspace=True)
                headers = next(reader)
                current_batch, current_payload = [], bytearray()

                for row in reader:
                    line = "\0".join(row)
                    encoded_line = (line + "\n").encode("utf-8")

                    if len(current_payload) + len(encoded_line) > max_payload_size:
                        batches.append(current_batch)
                        current_batch, current_payload = [], bytearray()

                    current_batch.append(line)
                    current_payload += encoded_line

                if current_batch:
                    batches.append(current_batch)

        return batches

    def send_batch(self, header: bytes, payload: bytes):
        try:
            batch_size = len(header) + len(payload)
            batch_data = header + payload
            if len(batch_data) > self._max_batch_size:
                raise ValueError(f"Batch size {len(batch_data)} exceeds max allowed {self._max_batch_size}")

            sender.send(self._socket, header)
            sender.send(self._socket, payload)

            logger.debug(f"Sent block of {batch_size} bytes")

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