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
}   

class JoinBatchCredits(JoinBatchBase):
    def process(self):
        logger.info("Node is online")

        

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()