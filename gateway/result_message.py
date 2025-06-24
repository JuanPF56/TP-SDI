"""This module defines the AnswerMessage class used for handling answer messages in the gateway."""

from dataclasses import dataclass

from common.logger import get_logger

logger = get_logger("ResultMessage")


@dataclass
class ResultMessage:
    client_id: str
    query: int
    results: str

    @staticmethod
    def from_json_with_casting(data: dict) -> "ResultMessage":
        return ResultMessage(
            client_id=data["client_id"],
            query=data["query"],
            results=data["results"],
        )

    def to_dict(self):
        return {
            "client_id": self.client_id,
            "query": self.query,
            "results": self.results,
        }
