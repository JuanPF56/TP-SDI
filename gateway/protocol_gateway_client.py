import socket
import struct
import json

import common.receiver as receiver
import common.sender as sender

from common.logger import get_logger
logger = get_logger("Protocol Gateway")

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UINT8, TIPO_MENSAJE, SIZE_OF_UUID
TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

from common.decoder import Decoder
TIMEOUT_HEADER = 1
TIMEOUT_PAYLOAD = 5

class ProtocolGateway:
    def __init__(self, client_socket: socket.socket, client_id: str):
        self._client_id = client_id
        self._client_socket = client_socket
        self._decoder = Decoder()

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._client_socket is not None and self._client_socket.fileno() != -1
    
    def _stop_client(self) -> None:
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
                    logger.error(f"Socket already shut down")
                finally:
                    if self._client_socket:
                        self._client_socket.close()
                        logger.info("Client socket closed")
                        self._client_socket = None

        except Exception as e:
            logger.error(f"Failed to close client socket: {e}")
            self._client_socket = None

    def send_client_id(self, client_id: str):
        """
        Send the client ID to the connected client.
        """
        try:
            encoded_client_id = client_id.encode("utf-8")
            client_id_len = len(encoded_client_id)
            if client_id_len != SIZE_OF_UUID:
                logger.error(f"Client ID length is not {SIZE_OF_UUID} bytes")
                raise ValueError("Client ID length is not 36 bytes")
            
            sender.send(self._client_socket, encoded_client_id)

        except sender.SenderConnectionLostError as e:
            logger.error(f"Connection error while sending client ID: {e}")
            self._stop_client()
        
        except sender.SenderError as e:
            logger.error(f"Connection error while sending client ID: {e}")
            self._stop_client()

        except Exception as e:
            logger.error(f"Unexpected error while sending client ID: {e}")
            self._stop_client()

    def receive_amount_of_requests(self) -> int | None:
        """
        Receive the amount of requests from the client.
        """
        try:
            data = receiver.receive_data(
                self._client_socket,
                SIZE_OF_UINT8,
                timeout=TIMEOUT_HEADER
            )

            if data is None or len(data) != SIZE_OF_UINT8:
                logger.error("Invalid or incomplete data received")
                self._stop_client()
                return None

            amount_of_requests = struct.unpack(">B", data)[0]
            return amount_of_requests

        except TimeoutError:
            logger.error("Timeout waiting for amount of requests")
            self._stop_client()
            return None

        except receiver.ReceiverError as e:
            logger.error(f"Connection error while receiving amount of requests: {e}")
            self._stop_client()
            return None

        except Exception as e:
            logger.error(f"Unexpected error while receiving amount of requests: {e}")
            self._stop_client()
            return None

    def receive_header(self) -> tuple | None:
        """
        Receive and unpack the header from the client.
        """
        try:
            header = receiver.receive_data(
                self._client_socket,
                SIZE_OF_HEADER,
                timeout=TIMEOUT_HEADER
            )
            
            if not header or len(header) != SIZE_OF_HEADER:
                logger.error("Invalid or incomplete header received")
                self._stop_client()
                return None

            type_of_batch, encoded_id, request_number, current_batch, is_last_batch, payload_len = struct.unpack(">B36sBIBI", header)
            message_code = TIPO_MENSAJE_INVERSO.get(type_of_batch)

            if message_code is None:
                logger.error(f"Unknown message code: {type_of_batch}")
                self._stop_client()
                return None

            return message_code, encoded_id, request_number, current_batch, is_last_batch, payload_len

        except TimeoutError:
            logger.error(f"Timeout waiting for receiving header: {e}")
            self._stop_client()
            return None

        except receiver.ReceiverError as e:
            logger.error(f"Connection error while receiving header: {e}")
            self._stop_client()
            return None

        except Exception as e:
            logger.error(f"Unexpected error while receiving header: {e}")
            self._stop_client()
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
                self._client_socket,
                payload_len,
                timeout=TIMEOUT_PAYLOAD
            )

            if data is None:
                logger.error("No data received. Client may have disconnected.")
                self._stop_client()
                return None

            if len(data) != payload_len:
                logger.error(f"Expected {payload_len} bytes, but received {len(data)} bytes")
                self._stop_client()
                return None

            return data

        except TimeoutError:
            logger.error(f"Timeout waiting for receiving payload: {e}")
            self._stop_client()
            return None

        except receiver.ReceiverError as e:
            logger.error(f"Connection error while receiving payload: {e}")
            self._stop_client()
            return None

        except Exception as e:
            logger.error(f"Unexpected error while receiving payload: {e}")
            self._stop_client()
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
                logger.debug(f"Amount of received credits {len(credits_from_batch)}")
                for credit in credits_from_batch:
                    credit.log_credit_info()
            return credits_from_batch

        elif message_code == "BATCH_RATINGS":
            ratings_from_batch = self._decoder.decode_ratings(decoded_payload)
            if not ratings_from_batch:
                logger.error("No ratings received or incomplete data")
                return None
            else:
                logger.debug(f"Amount of received ratings {len(ratings_from_batch)}")
                for rating in ratings_from_batch:
                    rating.log_rating_info()
            return ratings_from_batch

        else:
            logger.error(f"Unknown message code: {message_code}")
            return None

    def build_result_message(self, result_data):
        """
        result_dict:
        {
            "client_id": "uuid-del-cliente",
            "request_number": 1,
            "query": "Q4",
            "results": { ... }
        }
        """
        tipo_de_mensaje = TIPO_MENSAJE["RESULTS"]

        # client_id not needed

        request_number = result_data.get("request_number", -1) # default to -1 if not found

        query_id_str = result_data.get("query", "Q0")  # fallback for missing query
        # Extract number from query_id_str (Q1 -> 1)
        query_id = int(query_id_str[1:])

        result_of_query = result_data.get("results", {})

        # Serialize payload
        payload = json.dumps(result_of_query).encode()
        payload_len = len(payload)

        # Header: tipo(1 byte), query_id(1 byte), payload_len(4 bytes)
        header = struct.pack(">BBBI", tipo_de_mensaje, request_number, query_id, payload_len)

        full_message = header + payload

        return full_message

    def send_result(self, result_data: dict) -> None:
        message = self.build_result_message(result_data)

        try:
            logger.debug(f"Sending result message: {message}")
            sender.send(self._client_socket, message)
        
        except sender.SenderConnectionLostError as e:
            logger.error(f"Connection error while sending result: {e}")
            self._stop_client()
            
        except sender.SenderError as e:
            logger.error(f"Connection error while sending result: {e}")
            self._stop_client()

        except Exception as e:
            logger.error(f"Unexpected error while sending result: {e}")
            self._stop_client()
    
