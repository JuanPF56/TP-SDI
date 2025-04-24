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
                try:
                    data = json.loads(body)
                    node_id = data.get("node_id")
                except json.JSONDecodeError:
                    logger.error("Failed to decode EOS message")
                    return
                if node_id not in self._eos_flags:
                    self._eos_flags[node_id] = True
                    logger.info(f"EOS received for node {node_id}.")
                else:
                    logger.warning(f"EOS message for node {node_id} already received. Ignoring duplicate.")
                    return
                # Put the EOS message back to the queue for other nodes
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.input_queue,
                    body=json.dumps({"node_id": node_id}),
                    properties=pika.BasicProperties(type=msg_type)
                )
                if len(self._eos_flags) == int(self.eos_to_await):
                    logger.info("All nodes have sent EOS. Sending EOS to output queue.")
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.output_queue,
                        body=json.dumps({"node_id": self.node_id}),
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
