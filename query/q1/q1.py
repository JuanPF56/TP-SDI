import configparser
import json
import os
import pika
from common.logger import get_logger

EOS_TYPE = "EOS"

logger = get_logger("Query-Arg-Spain-Genres")


class ArgSpainGenreQuery:
    """
    Películas y sus géneros de los años 00' con producción Argentina y Española.
    """
    def __init__(self, config):
        self.config = config
        self.results = []

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        self.channel = self.connection.channel()

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}

        self.channel.queue_declare(queue=input_queue)
        self.channel.queue_declare(queue=config["DEFAULT"]["results_queue"])

    def _calculate_and_publish_results(self):
        results = {
            "query": "Q1",
            "results": self.results
        }
        logger.info("RESULTS:" + str(results))

        self.channel.basic_publish(
            exchange='',
            routing_key=self.config["DEFAULT"]["results_queue"],
            body=json.dumps(results),
        )

    def process_batch(self, movies_batch):
        """
        Process a batch of movies.
        """
        logger.debug(f"Processing batch of {len(movies_batch)} movies...")
        for movie in movies_batch:
            title = movie.get("original_title")
            genres = [g.get("name") for g in movie.get("genres", []) if g.get("name")]
            self.results.append((title, genres))

    def process(self):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after processing a batch.
        """
        logger.info("Node is online")

        def callback(ch, method, properties, body):
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
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                movies_batch = json.loads(body)
                if not isinstance(movies_batch, list):
                    logger.warning("❌ Expected a list (batch) of movies, skipping.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # Process the batch of movies
                self.process_batch(movies_batch)

            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=self.config["DEFAULT"]["movies_arg_spain_2000s_queue"], on_message_callback=callback)

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
    query = ArgSpainGenreQuery(config)
    query.process()
