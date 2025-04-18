import configparser
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Credits")

cast = {
    {
        "movie_id": 1,
        "cast": [ "Ricardo Darín", "Soledad Villamil", "Guillermo Francella" ],
    },
    {
        "movie_id": 2,
        "cast": [ "Antonio Gasalla", "China Zorrilla", "Luis Brandoni" ],
    },
    {
        "movie_id": 3,
        "cast": [ "Ricardo Darín", "Leonardo Sbaraglia", "Érica Rivas" ],
    },
    {
        "movie_id": 4,
        "cast": [ "Ricardo Darín", "Gastón Pauls", "Leticia Brédice" ],
    },
    {
        "movie_id": 5,
        "cast": [ "Ricardo Darín", "Luis Brandoni", "Chino Darín" ],
    },
    {
        "movie_id": 6,
        "cast": [ "Guillermo Francella", "Luis Brandoni", "Raúl Arévalo" ],
    },
    {
        "movie_id": 7,
        "cast": [ "Leonardo DiCaprio", "Kate Winslet", "Billy Zane" ],
    },
    {
        "movie_id": 8,
        "cast": [ "Robert Downey Jr.", "Chris Evans", "Scarlett Johansson" ],
    },
    {
        "movie_id": 9,
        "cast": [ "Tom Hanks", "Robin Wright", "Gary Sinise" ],
    },
    {
        "movie_id": 10,
        "cast": [ "Brad Pitt", "Angelina Jolie", "James McAvoy" ],
    },
}   

class JoinBatchCredits(JoinBatchBase):
    def receive_batch(self):
        # TODO: Read credits batch from RabbitMQ

        # Wait for the movies table to be received
        with self.movies_table_condition:
            self.movies_table_condition.wait()
        logger.info("Movies table received")
        logger.info("Movies table: %s", self.movies_table)

        # Perform the join operation (only keep cast for movies in the movies table)
        joined_data = []
        for movie in cast:
            for movie_table in self.movies_table:
                if movie["movie_id"] == movie_table["id"]:
                    joined_data.append(movie)
                    break

        logger.info("Joined data: %s", joined_data)

        # TODO: Send the joined data to the next node in the pipeline
        
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

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()