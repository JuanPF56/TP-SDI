"""This module defines the BatchMessage class used for handling batch messages in the gateway."""

from dataclasses import dataclass


@dataclass
class BatchMessage:
    message_id: int
    message_code: str
    client_id: str
    current_batch: int
    is_last_batch: bool
    processed_data: dict
