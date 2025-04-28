import configparser
import json
import os
import pika
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS" 

from common.logger import get_logger

logger = get_logger("Query-Arg-Prod-Ratings")

class ArgProdRatingsQuery:
    """
    Película de producción Argentina estrenada a partir del 2000, con mayor 
    y con menor promedio de rating.
    """
    def __init__(self, config):
        self.config = config
        self.movie_ratings = {}
        self.source_queue = self.config["DEFAULT"].get("movies_ratings_queue", "movies_ratings")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results")

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )   
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}



    def _calculate_and_publish_results(self):
        logger.info("Calculating results...")

        results = {
            "highest": {
                "title": None,
                "rating": 0
            },
            "lowest": {
                "title": None,
                "rating": float('inf')
            }
        }

        found_valid_ratings = False

        for movie_id, movie_data in self.movie_ratings.items():
            if movie_data["rating_count"] > 0:
                found_valid_ratings = True
                average_rating = movie_data["rating_sum"] / movie_data["rating_count"]
                if average_rating > results["highest"]["rating"]:
                    results["highest"]["rating"] = average_rating
                    results["highest"]["title"] = movie_data["original_title"]
                if average_rating < results["lowest"]["rating"]:
                    results["lowest"]["rating"] = average_rating
                    results["lowest"]["title"] = movie_data["original_title"]

        if not found_valid_ratings:
            logger.info("No ratings found for the requested movies.")
            # Send empty results to client
            results_msg = {
                "query": "Q3",
                "results": {}
            }
        else:
            results_msg = {
                "query": "Q3",
                "results": {
                    "highest": {
                        "title": results["highest"]["title"],
                        "rating": results["highest"]["rating"]
                    },
                    "lowest": {
                        "title": results["lowest"]["title"],
                        "rating": results["lowest"]["rating"]
                    }
                }
            }

        logger.info("RESULTS:" + str(results_msg))

        self.rabbitmq_processor.publish(
            queue=self.target_queue,
            message=results_msg
        )

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties and properties.type else "UNKNOWN"

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                return
            if node_id not in self.eos_flags:
                self.eos_flags[node_id] = True
                logger.info(f"EOS received for node {node_id}.")
            else:
                logger.warning(f"EOS message for node {node_id} already received. Ignoring duplicate.")
                return
            if len(self.eos_flags) == int(self.eos_to_await):
                logger.info("All nodes have sent EOS. Calculating results...")
                self._calculate_and_publish_results()
            self.rabbitmq_processor.acknowledge(method)
            return

        try:
            movies = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return
        
        for movie in movies:
            if movie.get("id") not in self.movie_ratings:
                self.movie_ratings[movie["id"]] = {
                    "original_title": movie["original_title"],
                    "rating_sum": 0,
                    "rating_count": 0,
                }
            self.movie_ratings[movie["id"]]["rating_sum"] += movie["rating"]
            self.movie_ratings[movie["id"]]["rating_count"] += 1
        
        self.rabbitmq_processor.acknowledge(method)


    def process(self):
        logger.info("Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdRatingsQuery(config)
    query.process()