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
            # Load the data from the incoming message
            try:
                decoded = json.loads(body)

                if isinstance(decoded, list):
                    logger.debug(f"Received list: {decoded}")
                    data = decoded
                elif isinstance(decoded, dict):
                    logger.debug(f"Received dict: {decoded}")
                    data = [decoded]
                else:
                    logger.warning(f"Unexpected JSON format: {decoded}")
                    return

            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return

            movies_by_id = {movie["id"]: movie for movie in self.movies_table}
            joined_data = []
            for movie in data:
                movie_id = movie.get("id")
                if str(movie_id) in movies_by_id:
                    joined_data.append(movie)
            if not joined_data:
                return
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps(joined_data).encode('utf-8'),
                properties=pika.BasicProperties(type=msg_type)
            )
            logger.debug(f"Sent {len(joined_data)} movies to {self.output_queue}")

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
