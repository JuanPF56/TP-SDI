from typing import List

from common.classes.movie import Movie, MOVIE_LINE_FIELDS

from common.logger import get_logger
logger = get_logger("Decoder")

class Decoder:
    """
    Decoder class to decode messages from the protocol gateway
    """

    @staticmethod
    def decode_movies(data: str) -> List[Movie]:
        movies = []
        lines = data.strip().split("\n")

        for line in lines:
            fields = line.split("\0")
            if len(fields) != MOVIE_LINE_FIELDS:
                logger.warning(f"Ignoring line with unexpected number of fields: {len(fields)}")
                continue

            movie = Movie(*fields)
            movies.append(movie)

        return movies
    
    @staticmethod
    def decode_actors(data: str) -> list:
        """
        Decode actors from the data string
        """
        # Implement the decoding logic here
        return data.split(",")

    @staticmethod
    def decode_ratings(data: str) -> list:
        """
        Decode ratings from the data string
        """
        # Implement the decoding logic here
        return data.split(",")