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
                ch.basic_ack(delivery_tag=method.delivery_tag)
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

            movies_by_id = {movie["id"]: movie for movie in self.movies_table}
            logger.info(f"movies_by_id: {len(movies_by_id)} movies")
            joined_data = []
            for movie in data:
                logger.info(f"Processing movie: {movie}")
                movie_id = movie.get("id")
                if movie_id in movies_by_id:
                    joined_data.append(movie)
                    logger.info(f"Joined movie: {movie}")
            if not joined_data:
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
            logger.error(f"[ERROR] Unexpected error in process_batch: {e}")

    def log_info(self, message):
        logger.info(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()
