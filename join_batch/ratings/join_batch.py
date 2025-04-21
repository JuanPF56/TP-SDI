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
    {
        "movie_id": 7,
        "rating": 5,
    },
    {
        "movie_id": 8,
        "rating": 4,
    },
    {
        "movie_id": 9,
        "rating": 3,
    },
    {
        "movie_id": 10,
        "rating": 2,
    },
]

class JoinBatchRatings(JoinBatchBase):
    def receive_batch(self):
        # TODO: Read ratings batch from RabbitMQ

        # Wait for the movies table to be received
        self.movies_table_ready.wait()
        logger.info("Movies table received")
        logger.info("Movies table: %s", self.movies_table)

        # Perform the join operation (only keep ratings for movies in the movies table)
        joined_data = []
        for movie in ratings:
            for movie_table in self.movies_table:
                if movie["movie_id"] == movie_table["id"]:
                    joined_data.append(movie)
                    break

        logger.info("Joined data: %s", joined_data)

        # TODO: Send the joined data to the next node in the pipeline
        
        # Q3 logic (average rating)
        ratings_by_movie = {}
        for movie in joined_data:
            if movie["movie_id"] not in ratings_by_movie:
                ratings_by_movie[movie["movie_id"]] = []
            ratings_by_movie[movie["movie_id"]].append(movie["rating"])
        average_ratings = {}
        for movie_id, rating_list in ratings_by_movie.items():
            average_ratings[movie_id] = sum(rating_list) / len(rating_list)
        logger.info("Average ratings: %s", average_ratings)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()