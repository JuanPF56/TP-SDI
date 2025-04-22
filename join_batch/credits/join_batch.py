import configparser
import json

import pika
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Credits")

class JoinBatchCredits(JoinBatchBase):
    def process_batch(self, ch, method, properties, body):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == "EOS":
                logger.info("Received EOS message, stopping consumption.")
                ch.stop_consuming()
                return

            # Load the data from the incoming message
            try:
                decoded = json.loads(body)

                if isinstance(decoded, dict):
                    data = decoded.get("movies", [])
                    is_last = decoded.get("last", False)
                elif isinstance(decoded, list):
                    data = decoded
                    is_last = False
                else:
                    logger.warning(f"Unexpected JSON format: {decoded}")
                    return

            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return

            # Flatten movies_table if it’s a nested list
            if self.movies_table and isinstance(self.movies_table, list) and isinstance(self.movies_table[0], list):
                self.movies_table = [item for sublist in self.movies_table for item in sublist]
                logger.warning("Flattened nested movies_table structure.")

            # Build a set of movie IDs for fast lookup
            movie_ids = {movie["id"] for movie in self.movies_table if isinstance(movie, dict) and "id" in movie}

            joined_data = []

            for movie in data:
                movie_id = movie.get("id")
                if movie_id in movie_ids:
                    joined_data.append(movie)
                    logger.info(f"Joined movie: {movie}")

            if not joined_data:
                logger.info("No matching movies found in the movies table.")
                return
            else:
                logger.info("Joined data: %s", joined_data)

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
            logger.error(f"[ERROR] Unexpected error in process_batch: {e}")

    def log_info(self, message):
        logger.info(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()
