import configparser
import json
import pika

from common.logger import get_logger

logger = get_logger("Query-Arg-Spain-Genres")

class ArgSpainGenreQuery:
    def __init__(self, config):
        self.config = config
        self.results = []

    def process(self):
        """
        Reads from:
        - movies_arg_spain_2000s_queue (already filtered for Argentina + Spain and 2000s)

        Collects:
        - title
        - genres (list of names)

        Prints the results when an end-of-stream flag is received.
        """
        logger.info("Node is online")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        channel.queue_declare(queue=input_queue)

        def callback(ch, method, properties, body):
            try:
                movie = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            title = movie.get("title", "Unknown title")
            genres = [g.get("name") for g in movie.get("genres", []) if g.get("name")]
            self.results.append((title, genres))
            logger.debug(f"Buffered: {self.results}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        logger.info(f"Waiting for messages from '{input_queue}'...")
        channel.basic_consume(queue=input_queue, on_message_callback=callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            channel.stop_consuming()
        finally:
            connection.close()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgSpainGenreQuery(config)
    query.process()
