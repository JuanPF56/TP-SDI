import configparser
import json
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Credits")

class JoinBatchCredits(JoinBatchBase):
    def process_batch(self, ch, method, properties, body):
        # Process the incoming message (cast batch)
        cast_batch = body.decode('utf-8')
        cast_batch = json.loads(cast_batch)

        logger.info("Received cast batch: %s", cast_batch)

        '''
        # Perform the join operation (only keep cast for movies in the movies table)
        joined_data = []
        for movie in cast:
            for movie_table in self.movies_table:
                if movie["movie_id"] == movie_table["id"]:
                    joined_data.append(movie)
                    break

        logger.info("Joined data: %s", joined_data)
        '''
        # TODO: Send the joined data to the next node in the pipeline

        '''
        # Q4 logic (count actor appearances)

        actors = {}

        for movie in joined_data:
            for actor in movie["cast"]:
                if actor not in actors:
                    actors[actor] = 0
                actors[actor] += 1

        # Sort actors by appearances
        actors = dict(sorted(actors.items(), key=lambda item: item[1], reverse=True))
        logger.info("Actors appearances: %s", actors)
        '''    

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()