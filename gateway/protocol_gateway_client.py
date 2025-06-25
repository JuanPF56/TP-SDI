"""
ProtocolGateway
This module implements the ProtocolGateway class, which handles communication with a client over a socket.
"""

import socket
import json
import threading
import common.receiver as receiver
import common.sender as sender
from common.decoder import Decoder
from common.protocol import (
    SIZE_OF_HEADER,
    TIPO_MENSAJE,
    SIZE_OF_HEADER_RESULTS,
    pack_result_header,
    unpack_header,
)
from common.logger import get_logger

logger = get_logger("Protocol Gateway")

TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

TIMEOUT_HEADER = 3600
TIMEOUT_PAYLOAD = 3600


class ProtocolGateway:
    def __init__(
        self,
        gateway_socket: socket.socket,
        client_id: str,
        shared_socket_lock: threading.Lock,
    ):
        self._client_id = client_id
        self._gateway_socket = gateway_socket
        self._socket_lock = shared_socket_lock
        self._decoder = Decoder()

    def gateway_is_connected(self) -> bool:
        """
        Check if the gateway is connected
        """
        return self._gateway_socket is not None and self._gateway_socket.fileno() != -1

    def process_payload(self, message_code: str, payload: bytes) -> list | None:
        """
        Process the payload
        """
        decoded_payload = payload.decode("utf-8")
        if message_code == TIPO_MENSAJE["BATCH_MOVIES"]:
            movies_from_batch = self._decoder.decode_movies(decoded_payload)
            if not movies_from_batch:
                logger.error("No movies received or invalid format")
                return None

            for movie in movies_from_batch:
                movie.log_movie_info()
            return movies_from_batch

        elif message_code == TIPO_MENSAJE["BATCH_CREDITS"]:
            credits_from_batch = self._decoder.decode_credits(decoded_payload)
            if not credits_from_batch:
                # logger.error("No credits received or incomplete data")
                return None
            else:
                logger.debug("Amount of received credits: %d", len(credits_from_batch))
                for credit in credits_from_batch:
                    credit.log_credit_info()
            return credits_from_batch

        elif message_code == TIPO_MENSAJE["BATCH_RATINGS"]:
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

    def _build_result_message(self, result_data) -> tuple:
        tipo_mensaje = TIPO_MENSAJE["RESULTS"]

        query_id_str = result_data.get("query", "Q0")  # fallback for missing query
        # Extract number from query_id_str (Q1 -> 1)
        query_id = int(query_id_str[1:])

        # Serialize payload
        payload = json.dumps(result_data).encode()
        payload_len = len(payload)

        # Header: tipo(1 byte), query_id(1 byte), payload_len(4 bytes)
        header = pack_result_header(tipo_mensaje, query_id, payload_len)
        if len(header) != SIZE_OF_HEADER_RESULTS:
            logger.error(
                "Header length is not %d bytes, got %d bytes",
                SIZE_OF_HEADER_RESULTS,
                len(header),
            )
            raise ValueError("Header length mismatch")

        return header, payload

    def send_result(self, result_data: dict) -> bool:
        """
        Send the result message to the client.
        result_data should be a dictionary with the following structure:
        {
            "client_id": "uuid-del-cliente",
            "query": "Q4",
            "results": { ... }
        }
        """
        try:
            header, payload = self._build_result_message(result_data)
            if len(header) != SIZE_OF_HEADER_RESULTS:
                logger.error(
                    "Header length is not %d bytes, got %d bytes",
                    SIZE_OF_HEADER_RESULTS,
                    len(header),
                )
                return False

            logger.debug(
                "Sending result message:\t Header: %s\t Payload: %s", header, payload
            )
            with self._socket_lock:
                sender.send(self._gateway_socket, header)
                sender.send(self._gateway_socket, payload)

            return True

        except sender.SenderConnectionLostError as e:
            logger.error("Connection error while sending result: %s", e)
            return False

        except sender.SenderError as e:
            logger.error("Connection error while sending result: %s", e)
            return False

        except Exception as e:
            logger.error("Unexpected error while sending result: %s", e)
            return False
