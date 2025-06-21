from dataclasses import dataclass

from common.logger import get_logger

logger = get_logger("Rating")

RATING_LINE_FIELDS = 4


@dataclass
class Rating:
    userId: int
    movieId: int
    rating: float
    timestamp: int

    def log_rating_info(self):
        logger.debug("Rating Info:")
        logger.debug(f"\tUser ID: {self.userId}")
        logger.debug(f"\tMovie ID: {self.movieId}")
        logger.debug(f"\tRating: {self.rating}")
        logger.debug(f"\tTimestamp: {self.timestamp}")
