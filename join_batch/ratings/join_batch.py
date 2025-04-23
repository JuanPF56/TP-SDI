import configparser
import json

import pika
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Ratings")

class JoinBatchRatings(JoinBatchBase):
    def process_batch(self, ch, method, properties, body):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            if msg_type == "EOS":
                logger.info("Received EOS message, stopping consumption.")
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.output_queue,
                    body=b'',
                    properties=pika.BasicProperties(type=msg_type)
                )
                ch.stop_consuming()
                return
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return

            # Build a set of movie IDs for fast lookup
            movies_by_id = {movie["id"]: movie for movie in self.movies_table}
            joined_data = []

            for movie in data:
                movie_id = movie.get("movie_id")
                if str(movie_id) in movies_by_id:
                    movie_title = movies_by_id[str(movie_id)]["original_title"]
                    joined_movie = {
                        "id": movie_id,
                        "original_title": movie_title,
                        "rating": movie["rating"],
                    }
                    joined_data.append(joined_movie)

            if not joined_data:
                logger.debug("No matching movies found in the movies table.")
                return
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps(joined_data).encode('utf-8'),
                properties=pika.BasicProperties(type=msg_type)
            )

        except pika.exceptions.StreamLostError as e:
            self.log_info(f"Stream lost, reconnecting: {e}")
            self.connection.close()
            self.channel.stop_consuming()
            self.connection, self.channel = self.__connect_to_rabbitmq()
            self.receive_batch()

        except Exception as e:
            self.log_info(f"[ERROR] Unexpected error in process_batch: {e}")

    def log_info(self, message):
        logger.info(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()
