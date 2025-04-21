import configparser
import json
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Ratings")

class JoinBatchRatings(JoinBatchBase):
    def process_batch(self, ch, method, properties, body):
        # Process the incoming message (cast batch)
        ratings_batch = body.decode('utf-8')
        ratings_batch = json.loads(ratings_batch)

        logger.info("Received ratings batch: %s", ratings_batch)
        '''
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
        '''


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()