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
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return

            # Perform the join operation (only keep cast for movies in the movies table)
            joined_data = []
            for movie in data:
                for movie_tab in self.movies_table:
                    if movie["id"] == movie_tab["id"]:
                        joined_data.append(movie)
                        break

            if not joined_data:
                logger.debug("No matching movies found in the movies table.")
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

    def log_info(self, message):
        logger.info(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchCredits(config).process()