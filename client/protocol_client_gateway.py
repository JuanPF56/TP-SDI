import common.receiver as receiver
import common.sender as sender
import common.exceptions as exceptions
import socket

import os
import csv

import struct
import json

from common.logger import get_logger
logger = get_logger("Protocol Client")

from common.protocol import TIPO_MENSAJE, SIZE_OF_HEADER, SIZE_OF_UINT8, SIZE_OF_HEADER_RESULTS

class ProtocolClient:
    def __init__(self, socket: socket.socket, max_batch_size):
        self._socket = socket
        self._max_batch_size = max_batch_size
        self._connected = True

    def _is_connected(self):
        return self._connected

    def send_dataset(self, dataset_path, dataset_name, message_type_str):
        logger.debug(f"Sending dataset {dataset_name} as {message_type_str}...")

        csv_path = os.path.join(dataset_path, f"{dataset_name}.csv")
        if not os.path.exists(csv_path):
            logger.error(f"Dataset {dataset_name} not found at {csv_path}")
            return

        try:
            max_payload_size = self._max_batch_size - SIZE_OF_HEADER
            batch_number = 0

            with open(csv_path, newline='', encoding="utf-8") as csvfile:
                if dataset_name == "credits":
                    lines = csvfile.readlines()[1:]  # Skip header
                    current_payload = bytearray()

                    for line in lines:
                        line_bytes = line.encode("utf-8")
                        if len(line_bytes) > max_payload_size:
                            logger.debug(f"Fragmenting oversized line (size={len(line_bytes)})")
                            start = 0
                            while start < len(line_bytes):
                                safe_end = self._find_utf8_safe_split_point(line_bytes[start:], max_payload_size)
                                chunk = line_bytes[start:start + safe_end]

                                if current_payload and len(current_payload) + 1 + len(chunk) > max_payload_size:
                                    self._safe_send_batch(message_type_str, batch_number, current_payload, is_last=False)
                                    batch_number += 1
                                    current_payload = bytearray()

                                current_payload += chunk
                                start += safe_end

                        else:
                            extra_bytes = b"\n" if current_payload else b""
                            if len(current_payload) + len(extra_bytes) + len(line_bytes) > max_payload_size:
                                self._safe_send_batch(message_type_str, batch_number, current_payload, is_last=False)
                                batch_number += 1
                                current_payload = bytearray()

                            current_payload += line_bytes

                    if current_payload:
                        self._safe_send_batch(message_type_str, batch_number, current_payload, is_last=True)
                
                else:
                    reader = csv.reader(csvfile, quotechar='"', delimiter=',', skipinitialspace=True)
                    next(reader)  # Skip header
                    current_payload = bytearray()

                    for row in reader:
                        line = "\0".join(row)
                        encoded_line = (line + "\n").encode("utf-8")

                        if len(current_payload) + len(encoded_line) > max_payload_size:
                            self._safe_send_batch(message_type_str, batch_number, current_payload, is_last=False)
                            batch_number += 1
                            current_payload = bytearray()

                        current_payload += encoded_line

                    if current_payload:
                        self._safe_send_batch(message_type_str, batch_number, current_payload, is_last=True)

        except exceptions.ServerNotConnectedError as e:
            logger.error(f"Connection closed by server: {e}")
            self._connected = False
            raise exceptions.ServerNotConnectedError("Connection closed by server")

        except Exception as e:
            logger.error(f"Error reading/sending CSV: {e}")

    def _find_utf8_safe_split_point(self, data: bytes, max_len: int) -> int:
        """
        Find the largest index ≤ max_len where the data is a valid UTF-8 prefix.
        """
        if max_len >= len(data):
            return len(data)

        end = max_len
        while end > 0:
            try:
                data[:end].decode('utf-8')
                return end
            except UnicodeDecodeError:
                end -= 1
        return max_len  # fallback (shouldn't reach)

    def _safe_send_batch(self, *args, **kwargs):
        try:
            self._send_single_batch(*args, **kwargs)
        except exceptions.ServerNotConnectedError:
            logger.error("Connection closed by server during batch send.")
            self._connected = False
            raise

    def _send_single_batch(self, message_type_str, batch_number, payload: bytes, is_last: bool):
        tipo_de_mensaje = TIPO_MENSAJE[message_type_str]
        is_last_batch = 1 if is_last else 0
        header = struct.pack(">BI B I", tipo_de_mensaje, batch_number, is_last_batch, len(payload))

        if len(header) != SIZE_OF_HEADER:
            raise ValueError(f"Header size {len(header)} does not match expected {SIZE_OF_HEADER}")

        logger.debug(f"{message_type_str} - Sending batch {batch_number}")

        try:
            self.send_batch(header, payload)
        except exceptions.ServerNotConnectedError:
            logger.error(f"Connection closed by server")
            self._connected = False
            raise exceptions.ServerNotConnectedError("Connection closed by server")

    def send_batch(self, header: bytes, payload: bytes):
        try:
            batch_size = len(header) + len(payload)
            batch_data = header + payload
            if len(batch_data) > self._max_batch_size:
                raise ValueError(f"Batch size {len(batch_data)} exceeds max allowed {self._max_batch_size}")
            
            try:
                sender.send(self._socket, header)
                # logger.info(f"Header sent: {header}")
            except ConnectionError as e:
                logger.error(f"Connection closed by server")
                self._connected = False
                raise exceptions.ServerNotConnectedError("Connection closed by server")
            
            try:
                sender.send(self._socket, payload)
                # logger.info(f"Payload sent: {payload}\n(size={len(payload)})")
            except ConnectionError as e:
                logger.error(f"Connection closed by server")
                self._connected = False
                raise exceptions.ServerNotConnectedError("Connection closed by server")

        except Exception as e:
            if isinstance(e, exceptions.ServerNotConnectedError):
                raise
            logger.error(f"Error sending CSV batch: {e}")

    """
    def receive_confirmation(self):
    
        # Receives a confirmation message from the server.
        # The confirmation message is expected to be a 1-byte unsigned integer.

        logger.info("Awaiting confirmation from server...")
        try:
            data = receiver.receive_data(self._socket, SIZE_OF_UINT8)
            confirmation = int.from_bytes(data, byteorder="big")
            logger.debug(f"Received confirmation: {confirmation}")
            return confirmation
        except Exception as e:
            logger.error(f"Error receiving confirmation: {e}")
            return None
    """

    def receive_query_response(self) -> dict:
        logger.info("Awaiting query response from server...")
        # Recibir header
        try:
            header_bytes = receiver.receive_data(self._socket, SIZE_OF_HEADER_RESULTS)
        except ConnectionError as e:
            logger.error(f"Connection closed by server")
            self._connected = False
            raise exceptions.ServerNotConnectedError("Connection closed by server")
        
        if not header_bytes or len(header_bytes) != SIZE_OF_HEADER_RESULTS:
            return None

        tipo_mensaje, query_id, payload_len = struct.unpack(">BBI", header_bytes)

        if tipo_mensaje != TIPO_MENSAJE["RESULTS"]:
            logger.error(f"Unexpected message type: {tipo_mensaje}")
            return None
        logger.debug(f"Received message type: {TIPO_MENSAJE['RESULTS']}")
        logger.debug(f"Received query ID: {query_id}")
        logger.debug(f"Received payload length: {payload_len}")

        # Recibir payload
        try:
            payload_bytes = receiver.receive_data(self._socket, payload_len)
        except ConnectionError as e:
            logger.error(f"Connection closed by server")
            self._connected = False
            raise exceptions.ServerNotConnectedError("Connection closed by server")

        if not payload_bytes or len(payload_bytes) != payload_len:
            return None
        logger.debug(f"Received payload size: {len(payload_bytes)}")
        try:
            result = json.loads(payload_bytes.decode())
            logger.debug(f"Received JSON response: {result}")
            return result
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response")
            return None