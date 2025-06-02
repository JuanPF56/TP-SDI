"""
Protocol connecting client to server for sending datasets in batches.
This module implements the client-side protocol for sending datasets to a server in batches.
This includes handling connection, sending datasets, and receiving query responses.
"""

import socket

import os
import csv

import struct
import json

import common.receiver as receiver
import common.sender as sender
from common.protocol import (
    TIPO_MENSAJE,
    SIZE_OF_UUID,
    SIZE_OF_HEADER,
    SIZE_OF_HEADER_RESULTS,
    ProtocolError,
)

from common.logger import get_logger

logger = get_logger("Protocol Client")


TIMEOUT_RECEIVE_ID = 60
TIMEOUT_ANSWER_HEADER = 3600  # 1 hour (longest timeout for a query)
TIMEOUT_ANSWER_PAYLOAD = 3600


class ProtocolClient:
    def __init__(self, socket: socket.socket, max_batch_size):
        self._client_id = None
        self._socket = socket
        self._max_batch_size = max_batch_size
        self._connected = True

    def get_client_id(self):
        """
        Requests the server to assign a client ID and waits for the response.
        Returns:
            str: The assigned client ID.
        Raises:
            ServerNotConnectedError: If the server is not connected or times out.
            ProtocolError: If there is an error in the protocol communication.
        """
        logger.info("Waiting for server to assign client ID...")
        try:
            client_id_bytes = receiver.receive_data(
                self._socket, SIZE_OF_UUID, timeout=TIMEOUT_RECEIVE_ID
            )

            if not client_id_bytes or len(client_id_bytes) != SIZE_OF_UUID:
                logger.error("Failed to receive client ID from server")
                raise ProtocolError("Failed to receive client ID from server")

            self._client_id = client_id_bytes.decode("utf-8")
            logger.info("Client ID assigned: %s", self._client_id)
            return self._client_id

        except TimeoutError:
            logger.error("Timeout waiting for client ID from server")
            self._connected = False
            raise ServerNotConnectedError("Timeout waiting for client ID from server")

        except receiver.ReceiverError:
            logger.error("Receiver error while receiving client ID")
            self._connected = False
            raise ProtocolError("Receiver error while receiving client ID")

        except Exception as e:
            logger.error("Unexpected error receiving client ID: %s", e)
            self._connected = False
            raise ProtocolError("Unexpected error receiving client ID")

    def is_connected(self):
        """
        Checks if the client is still connected to the server.
        Returns:
            bool: True if connected, False otherwise.
        """
        return self._connected

    def send_dataset(self, dataset_path, dataset_name, message_type_str):
        """
        Sends a dataset to the server in batches.
        Args:
            dataset_path (str): Path to the directory containing the dataset.
            dataset_name (str): Name of the dataset file (without extension).
            message_type_str (str): Type of message to send (e.g., "BATCH_MOVIES").
        """
        logger.debug("Sending dataset %s as %s...", dataset_name, message_type_str)

        csv_path = os.path.join(dataset_path, f"{dataset_name}.csv")
        if not os.path.exists(csv_path):
            logger.error("Dataset %s not found at %s", dataset_name, csv_path)
            return

        try:
            max_payload_size = self._max_batch_size - SIZE_OF_HEADER
            batch_number = 0

            with open(csv_path, newline="", encoding="utf-8") as csvfile:
                if dataset_name == "credits":
                    lines = csvfile.readlines()[1:]  # Skip header
                    current_payload = bytearray()

                    for line in lines:
                        line_bytes = line.encode("utf-8")
                        if len(line_bytes) > max_payload_size:
                            logger.debug(
                                "Fragmenting oversized line (size=%d)", len(line_bytes)
                            )
                            start = 0
                            while start < len(line_bytes):
                                safe_end = self._find_utf8_safe_split_point(
                                    line_bytes[start:], max_payload_size
                                )
                                chunk = line_bytes[start : start + safe_end]

                                if (
                                    current_payload
                                    and len(current_payload) + 1 + len(chunk)
                                    > max_payload_size
                                ):
                                    self._safe_send_batch(
                                        message_type_str,
                                        batch_number,
                                        current_payload,
                                        is_last=False,
                                    )
                                    batch_number += 1
                                    current_payload = bytearray()

                                current_payload += chunk
                                start += safe_end

                        else:
                            extra_bytes = b"\n" if current_payload else b""
                            if (
                                len(current_payload)
                                + len(extra_bytes)
                                + len(line_bytes)
                                > max_payload_size
                            ):
                                self._safe_send_batch(
                                    message_type_str,
                                    batch_number,
                                    current_payload,
                                    is_last=False,
                                )
                                batch_number += 1
                                current_payload = bytearray()

                            current_payload += line_bytes

                    if current_payload:
                        self._safe_send_batch(
                            message_type_str,
                            batch_number,
                            current_payload,
                            is_last=True,
                        )

                else:
                    reader = csv.reader(
                        csvfile, quotechar='"', delimiter=",", skipinitialspace=True
                    )
                    next(reader)  # Skip header
                    current_payload = bytearray()

                    for row in reader:
                        line = "\0".join(row)
                        encoded_line = (line + "\n").encode("utf-8")

                        if len(current_payload) + len(encoded_line) > max_payload_size:
                            self._safe_send_batch(
                                message_type_str,
                                batch_number,
                                current_payload,
                                is_last=False,
                            )
                            batch_number += 1
                            current_payload = bytearray()

                        current_payload += encoded_line

                    if current_payload:
                        self._safe_send_batch(
                            message_type_str,
                            batch_number,
                            current_payload,
                            is_last=True,
                        )

        except ServerNotConnectedError as e:
            logger.error("Connection closed by server: %s", e)
            self._connected = False
            raise ServerNotConnectedError("Connection closed by server")

        except Exception as e:
            logger.error("Error reading/sending CSV: %s", e)
            raise

    def _find_utf8_safe_split_point(self, data: bytes, max_len: int) -> int:
        """
        Find the largest index ≤ max_len where the data is a valid UTF-8 prefix.
        """
        if max_len >= len(data):
            return len(data)

        end = max_len
        while end > 0:
            try:
                data[:end].decode("utf-8")
                return end
            except UnicodeDecodeError:
                end -= 1
        return max_len  # fallback (shouldn't reach)

    def _safe_send_batch(self, *args, **kwargs):
        try:
            self._send_single_batch(*args, **kwargs)
        except ServerNotConnectedError:
            logger.error("Connection closed by server during batch send.")
            self._connected = False
            raise

    def _send_single_batch(
        self,
        message_type_str,
        batch_number,
        payload: bytes,
        is_last: bool,
    ):
        # Prepare header
        tipo_de_mensaje = TIPO_MENSAJE[message_type_str]

        encoded_id = self._client_id.encode("utf-8")
        if len(encoded_id) != SIZE_OF_UUID:
            logger.error(
                "Client ID size %d does not match expected %d",
                len(encoded_id),
                SIZE_OF_UUID,
            )
            raise ValueError(
                f"Client ID size {len(encoded_id)} does not match expected {SIZE_OF_UUID}"
            )

        is_last_batch = 1 if is_last else 0

        header = struct.pack(
            ">B36sIBI",
            tipo_de_mensaje,
            encoded_id,
            batch_number,
            is_last_batch,
            len(payload),
        )

        if len(header) != SIZE_OF_HEADER:
            raise ValueError(
                f"Header size {len(header)} does not match expected {SIZE_OF_HEADER}"
            )

        logger.debug("%s - Sending batch %d", message_type_str, batch_number)

        try:
            self.send_batch(header, payload)

        except ServerNotConnectedError:
            logger.error("Connection closed by server while sending batch")
            self._connected = False
            raise ServerNotConnectedError(
                "Connection closed by server while sending batch"
            )

        except (ProtocolError, Exception) as e:
            logger.error("Error sending batch: %s", e)
            self._connected = False
            raise ProtocolError("Error sending batch")

    def send_batch(self, header: bytes, payload: bytes):
        try:
            batch_size = len(header) + len(payload)
            batch_data = header + payload
            if len(batch_data) > self._max_batch_size:
                raise ValueError(
                    f"Batch size {len(batch_data)} exceeds max allowed {self._max_batch_size}"
                )

            try:
                sender.send(self._socket, header)
                # logger.info(f"Header sent: {header}")
            except sender.SenderConnectionLostError:
                logger.error("Connection closed by server when sending batch header")
                self._connected = False
                raise ServerNotConnectedError(
                    "Connection closed by server when sending batch header"
                )

            try:
                sender.send(self._socket, payload)
                # logger.info(f"Payload sent: {payload}\n(size={len(payload)})")
            except sender.SenderConnectionLostError:
                logger.error("Connection closed by server when sending batch payload")
                self._connected = False
                raise ServerNotConnectedError(
                    "Connection closed by server when sending batch payload"
                )

        except Exception as e:
            raise ProtocolError(f"Error sending batch: {e}")

    def receive_query_response(self) -> dict | None:
        logger.info("Awaiting query response from server...")

        # Recibir header
        try:
            header_bytes = receiver.receive_data(
                self._socket, SIZE_OF_HEADER_RESULTS, timeout=TIMEOUT_ANSWER_HEADER
            )

        except TimeoutError:
            logger.error("Timeout waiting for header response from server")
            self._connected = False
            raise ServerNotConnectedError(
                "Timeout waiting for header response from server"
            )

        except receiver.ReceiverError:
            logger.error("Receiver error while receiving header response")
            self._connected = False
            raise ProtocolError("Receiver error while receiving header response")

        except Exception as e:
            logger.error(f"Unexpected error receiving header response: {e}")
            self._connected = False
            raise ProtocolError("Unexpected error receiving header response")

        if not header_bytes or len(header_bytes) != SIZE_OF_HEADER_RESULTS:
            return None

        tipo_mensaje, query_id, payload_len = struct.unpack(">BBI", header_bytes)

        if tipo_mensaje != TIPO_MENSAJE["RESULTS"]:
            logger.error("Unexpected message type: %s", tipo_mensaje)
            return None

        # Recibir payload
        try:
            payload_bytes = receiver.receive_data(
                self._socket, payload_len, timeout=TIMEOUT_ANSWER_PAYLOAD
            )

        except TimeoutError:
            logger.error("Timeout waiting for response payload from server")
            self._connected = False
            raise ServerNotConnectedError(
                "Timeout waiting for response payload from server"
            )

        except receiver.ReceiverError:
            logger.error("Receiver error while receiving response payload")
            self._connected = False
            raise ProtocolError("Receiver error while receiving response payload")

        except Exception as e:
            logger.error(f"Unexpected error receiving response payload: {e}")
            self._connected = False
            raise ProtocolError("Unexpected error receiving response payload")

        if not payload_bytes or len(payload_bytes) != payload_len:
            return None

        logger.debug("Received payload size: %d", len(payload_bytes))

        try:
            result_of_query = json.loads(payload_bytes.decode())
            logger.debug("Received JSON response: %s", result_of_query)

            # Armar la estructura respuesta:
            # {
            #     "query_id": "Q4",
            #     "results": { ... }
            # }
            return {
                "query_id": f"Q{query_id}",
                "results": result_of_query,
            }

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response")
            return None


# Exceptions for errors between client and server
class ServerNotConnectedError(Exception):
    """Exception raised when the server is not connected."""

    pass
