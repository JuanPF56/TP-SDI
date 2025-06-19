"""
ProtocolGateway
This module implements the ProtocolGateway class, which handles communication with a client over a socket.
"""

import socket
import struct
import json

import common.receiver as receiver
import common.sender as sender
from common.decoder import Decoder
from common.protocol import (
    SIZE_OF_HEADER,
    TIPO_MENSAJE,
    SIZE_OF_UUID,
    SIZE_OF_HEADER_RESULTS,
)

from common.logger import get_logger

logger = get_logger("Protocol Gateway")

TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600


class ProtocolGateway:
    def __init__(self, client_socket: socket.socket, client_id: str):
        self._client_id = client_id
        self._client_socket = client_socket
        self._decoder = Decoder()

    def client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._client_socket is not None and self._client_socket.fileno() != -1

    def stop_client(self) -> None:
        """
        Close the client socket
        """
        try:
            if self._client_socket:
                logger.info("Closing client socket")
                try:
                    self._client_socket.shutdown(socket.SHUT_RDWR)
                    logger.info("Client socket shut down")
                except OSError as e:
                    logger.error("Socket already shut down: %s", e)
                finally:
                    if self._client_socket:
                        self._client_socket.close()
                        logger.info("Client socket closed")
                        self._client_socket = None

        except Exception as e:
            logger.error("Failed to close client socket: %s", e)
            self._client_socket = None

    def send_client_id(self, client_id: str):
        """
        Send the client ID to the connected client.
        """
        try:
            encoded_client_id = client_id.encode("utf-8")
            client_id_len = len(encoded_client_id)
            if client_id_len != SIZE_OF_UUID:
                logger.error("Client ID length is not %d bytes", SIZE_OF_UUID)
                raise ValueError("Client ID length is not 36 bytes")

            sender.send(self._client_socket, encoded_client_id)

        except sender.SenderConnectionLostError as e:
            logger.error("Connection error while sending client ID: %s", e)
            self.stop_client()

        except sender.SenderError as e:
            logger.error("Connection error while sending client ID: %s", e)
            self.stop_client()

        except Exception as e:
            logger.error("Unexpected error while sending client ID: %s", e)
            self.stop_client()

    def receive_header(self) -> tuple | None:
        """
        Receive and unpack the header from the client.
        """
        try:
            header = receiver.receive_data(
                self._client_socket, SIZE_OF_HEADER, timeout=TIMEOUT_HEADER
            )

            if not header or len(header) != SIZE_OF_HEADER:
                logger.error("Invalid or incomplete header received")
                self.stop_client()
                return None

            (
                type_of_batch,
                encoded_id,
                current_batch,
                is_last_batch,
                payload_len,
            ) = struct.unpack(">B36sIBI", header)
            message_code = TIPO_MENSAJE_INVERSO.get(type_of_batch)

            if message_code is None:
                logger.error("Unknown message code: %s", type_of_batch)
                self.stop_client()
                return None

            return (
                message_code,
                encoded_id,
                current_batch,
                is_last_batch,
                payload_len,
            )

        except TimeoutError as e:
            logger.error("Timeout waiting for receiving header: %s", e)
            self.stop_client()
            return None

        except receiver.ReceiverError as e:
            logger.error("Connection error while receiving header: %s", e)
            self.stop_client()
            return None

        except Exception as e:
            logger.error("Unexpected error while receiving header: %s", e)
            self.stop_client()
            return None

    def receive_payload(self, payload_len: int) -> bytes | None:
        """
        Receive the payload from the client
        """
        if payload_len <= 0:
            logger.error("Payload length is zero")
            return None

        try:
            data = receiver.receive_data(
                self._client_socket, payload_len, timeout=TIMEOUT_PAYLOAD
            )

            if data is None:
                logger.error("No data received. Client may have disconnected.")
                self.stop_client()
                return None

            if len(data) != payload_len:
                logger.error(
                    "Expected %d bytes, but received %d bytes", payload_len, len(data)
                )
                self.stop_client()
                return None

            return data

        except TimeoutError as e:
            logger.error("Timeout waiting for receiving payload: %s", e)
            self.stop_client()
            return None

        except receiver.ReceiverError as e:
            logger.error("Connection error while receiving payload: %s", e)
            self.stop_client()
            return None

        except Exception as e:
            logger.error("Unexpected error while receiving payload: %s", e)
            self.stop_client()
            return None

    def process_payload(self, message_code: str, payload: bytes) -> list | None:
        """
        Process the payload
        """
        decoded_payload = payload.decode("utf-8")
        if message_code == "BATCH_MOVIES":
            movies_from_batch = self._decoder.decode_movies(decoded_payload)
            if not movies_from_batch:
                logger.error("No movies received or invalid format")
                return None

            for movie in movies_from_batch:
                movie.log_movie_info()
            return movies_from_batch

        elif message_code == "BATCH_CREDITS":
            credits_from_batch = self._decoder.decode_credits(decoded_payload)
            if not credits_from_batch:
                # logger.error("No credits received or incomplete data")
                return None
            else:
                logger.debug("Amount of received credits: %d", len(credits_from_batch))
                for credit in credits_from_batch:
                    credit.log_credit_info()
            return credits_from_batch

        elif message_code == "BATCH_RATINGS":
            ratings_from_batch = self._decoder.decode_ratings(decoded_payload)
            if not ratings_from_batch:
                logger.error("No ratings received or incomplete data")
                return None
            else:
                logger.debug("Amount of received ratings: %d", len(ratings_from_batch))
                for rating in ratings_from_batch:
                    rating.log_rating_info()
            return ratings_from_batch

        else:
            logger.error("Unknown message code: %s", message_code)
            return None

    def _build_result_message(self, result_data):
        tipo_de_mensaje = TIPO_MENSAJE["RESULTS"]

        # client_id not needed

        query_id_str = result_data.get("query", "Q0")  # fallback for missing query
        # Extract number from query_id_str (Q1 -> 1)
        query_id = int(query_id_str[1:])

        result_of_query = result_data.get("results", {})

        # Serialize payload
        payload = json.dumps(result_of_query).encode()
        payload_len = len(payload)

        # Header: tipo(1 byte), query_id(1 byte), payload_len(4 bytes)
        header = struct.pack(">BBI", tipo_de_mensaje, query_id, payload_len)
        if len(header) != SIZE_OF_HEADER_RESULTS:
            logger.error(
                "Header length is not %d bytes, got %d bytes",
                SIZE_OF_HEADER_RESULTS,
                len(header),
            )
            raise ValueError("Header length mismatch")

        full_message = header + payload

        return full_message

    def send_result(self, result_data: dict) -> None:
        """
        Send the result message to the client.
        result_data should be a dictionary with the following structure:
        {
            "client_id": "uuid-del-cliente",
            "query": "Q4",
            "results": { ... }
        }
        """
        message = self._build_result_message(result_data)

        try:
            logger.debug("Sending result message: %s", message)
            sender.send(self._client_socket, message)

        except sender.SenderConnectionLostError as e:
            logger.error("Connection error while sending result: %s", e)
            self.stop_client()

        except sender.SenderError as e:
            logger.error("Connection error while sending result: %s", e)
            self.stop_client()

        except Exception as e:
            logger.error("Unexpected error while sending result: %s", e)
            self.stop_client()
