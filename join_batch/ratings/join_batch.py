import configparser
import json

import pika
from common.logger import get_logger
from common.join_base import JoinBatchBase

logger = get_logger("JoinBatch-Ratings")

class JoinBatchRatings(JoinBatchBase):
    def process_batch(self, ch, method, properties, body):
        try:
            # Process the incoming message (cast batch)
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
            
            # Data is a single movie rating, not a batch
            # TODO: Handle batches vs single messages?
            data = [data]
            logger.info(f"Received movie ratings: {data}")
            
            # Perform the join operation (only keep ratings for movies in the movies table)
            joined_data = []
            for movie in data:
                for movie_tab in self.movies_table:
                    if movie["movie_id"] == movie_tab["id"]:
                        # Add the movie rating to the movie data
                        joined_movie = {
                            "id": movie_tab["id"],
                            "original_title": movie_tab["original_title"],
                            "rating": movie["rating"],
                        }
                        logger.info(f"Joined movie: {joined_movie}")
                        joined_data.append(joined_movie)
                        break

            if not joined_data:
                logger.debug("No matching movies found in the movies table.")
                return
            else:
                logger.debug("Joined data: %s", joined_data)
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps(joined_data).encode('utf-8'),
                properties=pika.BasicProperties(type=msg_type)
            )

        except pika.exceptions.StreamLostError as e:
            # Handle the connection loss and reconnect
            self.log_info(f"Stream lost, reconnecting: {e}")
            self.connection.close()
            self.channel.stop_consuming()
            self.connection, self.channel = self.__connect_to_rabbitmq()
            self.receive_batch()  # Restart the batch consumption
        
    def log_info(self, message):
        logger.info(message)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    JoinBatchRatings(config).process()