import configparser
from common.logger import get_logger
from common.join_base import JoinBase

logger = get_logger("Join-Credits")

class JoinCredits(JoinBase):
    def perform_join(self, data, movies_by_id):
        joined_data = []
        for movie in data:
            movie_id = movie.get("id")
            if movie_id in movies_by_id:
                joined_data.append(movie)
        return joined_data

    def log_info(self, message):
        logger.info(message)

    def log_error(self, message):
        logger.error(message)

    def log_debug(self, message):
        logger.debug(message)

    def log_warning(self, message):
        logger.warning(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinCredits(config).process()
