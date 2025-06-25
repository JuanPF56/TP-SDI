"""
RawMessage class for handling raw messages in the gateway.
"""

from dataclasses import dataclass
from common.protocol import (
    unpack_header,
)
from common.logger import get_logger

logger = get_logger("RawMessage")


@dataclass
class RawMessage:
    def __init__(self, header: bytes, payload: bytes = None):
        self.header = header
        self.message_id = None
        self.tipo_mensaje = None
        self.encoded_id = None
        self.batch_number = None
        self.is_last_batch = None
        self.payload_length = None
        self.payload = payload

    def add_payload(self, payload: bytes):
        """
        Add payload to the raw message.
        """
        if self.payload is not None:
            logger.warning("Payload already exists, overwriting it.")
        self.payload = payload
        logger.debug("Payload added to raw message.")

    def unpack_header(self):
        """
        Unpack the header of the raw message.
        """
        if not self.header:
            logger.error("Header is empty, cannot unpack.")
            return None

        try:
            (
                self.message_id,
                self.tipo_mensaje,
                self.encoded_id,
                self.batch_number,
                self.is_last_batch,
                self.payload_length,
            ) = unpack_header(self.header)

            logger.debug("Header unpacked successfully.")

        except ValueError as e:
            logger.error(f"Error unpacking header: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error unpacking header: {e}")
            return None
