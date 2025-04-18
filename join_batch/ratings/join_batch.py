import configparser
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Ratings")

ratings = [
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
]

class JoinBatchRatings(JoinBatchBase):
    def receive_batch(self):
        # TODO: Read ratings batch from RabbitMQ

        # Wait for the movies table to be received
        with self.movies_table_condition:
            self.movies_table_condition.wait()
        logger.info("Movies table received")
        logger.info("Movies table: %s", self.movies_table)

        # Perform the join operation (only keep cast for movies in the movies table)
        joined_data = []
        for movie in ratings:
            for movie_table in self.movies_table:
                if movie["movie_id"] == movie_table["id"]:
                    joined_data.append(movie)
                    break

        logger.info("Joined data: %s", joined_data)

        # TODO: Send the joined data to the next node in the pipeline
        
        # Q3 logic (average rating)
        ratings = {}
        for movie in joined_data:
            if movie["movie_id"] not in ratings:
                ratings[movie["movie_id"]] = []
            ratings[movie["movie_id"]].append(movie["rating"])
        for movie_id, ratings_list in ratings.items():
            avg_rating = sum(ratings_list) / len(ratings_list)
            logger.info(f"Movie ID: {movie_id}, Average Rating: {avg_rating}")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()