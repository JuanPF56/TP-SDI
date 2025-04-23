import configparser
import json
import pika
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

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_ratings_queue", "movies_ratings")

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=input_queue)
        self.channel.queue_declare(queue=config["DEFAULT"]["results_queue"])


    def _calculate_and_publish_results(self):
        logger.info("Calculating results...")
        if not self.movie_ratings:
            logger.info("No movie ratings received.")
            return
        
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
        for movie_id, movie_data in self.movie_ratings.items():
            if movie_data["rating_count"] > 0:
                average_rating = movie_data["rating_sum"] / movie_data["rating_count"]
                if average_rating > results["highest"]["rating"]:
                    results["highest"]["rating"] = average_rating
                    results["highest"]["title"] = movie_data["original_title"]
                if average_rating < results["lowest"]["rating"]:
                    results["lowest"]["rating"] = average_rating
                    results["lowest"]["title"] = movie_data["original_title"]


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

        self.channel.basic_publish(
            exchange='',
            routing_key=self.config["DEFAULT"]["results_queue"],
            body=json.dumps(results_msg),
        )


    def process(self):
        logger.info("Node is online")

        def callback(ch, method, properties, body):
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == EOS_TYPE:
                logger.info("Received EOS message, stopping consumption.")
                self._calculate_and_publish_results()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                movies = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)
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
           
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=self.config["DEFAULT"]["movies_ratings_queue"], on_message_callback=callback)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.channel.stop_consuming()
        finally:
            self.connection.close()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdRatingsQuery(config)
    query.process()