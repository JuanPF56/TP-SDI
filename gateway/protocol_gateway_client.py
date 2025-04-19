import socket
import struct

import common.receiver as receiver
import common.sender as sender

from common.logger import get_logger
logger = get_logger("Protocol Gateway")

from common.protocol import SIZE_OF_HEADER, SIZE_OF_UINT8, TIPO_MENSAJE
TIPO_MENSAJE_INVERSO = {v: k for k, v in TIPO_MENSAJE.items()}

from common.decoder import Decoder

class ProtocolGateway:
    def __init__(self, client_socket: socket.socket):
        self._client_socket = client_socket
        self._decoder = Decoder()

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._client_socket is not None
    
    def receive_header(self) -> tuple | None:
        try:
            header = receiver.receive_data(self._client_socket, SIZE_OF_HEADER)
            if not header or len(header) != SIZE_OF_HEADER:
                logger.error("Invalid or incomplete header received")
                return None

            type_of_batch, current_batch, is_last_batch, payload_len = struct.unpack(">BI B I", header)
            message_code = TIPO_MENSAJE_INVERSO.get(type_of_batch)

            if message_code is None:
                logger.error(f"Unknown message code: {type_of_batch}")
                return None

            return message_code, current_batch, is_last_batch, payload_len

        except Exception as e:
            logger.error(f"Error receiving header: {e}")
            return None
            
    def receive_payload(self, payload_len: int) -> bytes:
        """
        Receive the payload from the client
        """
        if payload_len > 0:
            data = receiver.receive_data(self._client_socket, payload_len)
            if not data or len(data) != payload_len:
                logger.error("Invalid or incomplete data received")
                return None
            return data
        else:
            logger.error("Payload length is zero")
            return None
    
    def process_payload(self, message_code: str, payload: bytes) -> None:
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
                # TODO: PASARLE A LA QUEUE

        elif message_code == "BATCH_CREDITS":
            credits_from_batch = self._decoder.decode_credits(decoded_payload)
            if not credits_from_batch:
                logger.error("No credits received or incomplete data")
                return None
            else:
                logger.info(f"Amount of received credits {len(credits_from_batch)}")
                for credit in credits_from_batch:
                    credit.log_credit_info()
                    # TODO: PASARLE A LA QUEUE

        elif message_code == "BATCH_RATINGS":
            ratings_from_batch = self._decoder.decode_ratings(decoded_payload)
            if not ratings_from_batch:
                logger.error("No ratings received or incomplete data")
                return None
            else:
                logger.info(f"Amount of received ratings {len(ratings_from_batch)}")
                for rating in ratings_from_batch:
                    rating.log_rating_info()
                    # TODO: PASARLE A LA QUEUE

        else:
            logger.error(f"Unknown message code: {message_code}")
            return None
    
    def send_confirmation(self, message_code: int) -> None:
        """
        Send confirmation to the client
        """
        sender.send(self._client_socket, message_code.to_bytes(SIZE_OF_UINT8, byteorder="big"))