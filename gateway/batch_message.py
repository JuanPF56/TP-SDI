"""This module defines the BatchMessage class used for handling batch messages in the gateway."""

from typing import Union
from dataclasses import dataclass

from common.protocol import TIPO_MENSAJE
from common.classes.movie import Movie
from common.classes.credit import Credit, CastMember, CrewMember
from common.classes.rating import Rating
from common.parsers import parse_movie

from common.logger import get_logger

logger = get_logger("BatchMessage")


@dataclass
class BatchMessage:
    message_id: int
    message_code: int
    client_id: str
    current_batch: int
    is_last_batch: bool
    processed_data: Union[Movie, Credit, Rating]

    @staticmethod
    def from_json_with_casting(data: dict) -> "BatchMessage":
        message_code = data["message_code"]
        raw_data = data["processed_data"]

        if message_code == TIPO_MENSAJE["BATCH_MOVIES"]:
            processed = [parse_movie(m) for m in raw_data]
        elif message_code == TIPO_MENSAJE["BATCH_CREDITS"]:
            processed = [
                Credit(
                    cast=[CastMember(**c) for c in entry["cast"]],
                    crew=[CrewMember(**c) for c in entry["crew"]],
                    id=entry["id"],
                )
                for entry in raw_data
            ]
        elif message_code == TIPO_MENSAJE["BATCH_RATINGS"]:
            if isinstance(raw_data, list):
                processed = [Rating(**r) for r in raw_data]
            else:
                processed = Rating(**raw_data)
        else:
            raise ValueError(f"Unknown message code: {message_code}")

        return BatchMessage(
            message_id=data["message_id"],
            message_code=message_code,
            client_id=data["client_id"],
            current_batch=data["current_batch"],
            is_last_batch=data["is_last_batch"],
            processed_data=processed,
        )
