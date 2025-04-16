import configparser
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Ratings")

ratings = {
    {
        "movie_id": 1,
        "rating": 5,
    },
    {
        "movie_id": 2,
        "rating": 4,
    },
    {
        "movie_id": 3,
        "rating": 3,
    },
    {
        "movie_id": 4,
        "rating": 2,
    },
    {
        "movie_id": 5,
        "rating": 1,
    },
    {
        "movie_id": 6,
        "rating": 5,
    },
    {
        "movie_id": 1,
        "rating": 4,
    },
    {
        "movie_id": 2,
        "rating": 5,
    },
    {
        "movie_id": 3,
        "rating": 4,
    },
    {
        "movie_id": 4,
        "rating": 3,
    },
    {
        "movie_id": 5,
        "rating": 2,
    },
    {
        "movie_id": 6,
        "rating": 1,
    },
}

class JoinBatchRatings(JoinBatchBase):
    def process(self):
        logger.info("Node is online")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()