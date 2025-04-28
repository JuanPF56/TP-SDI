import configparser
import json
import os
import pika
from common.logger import get_logger
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"

logger = get_logger("Query-Arg-Spain-Genres")


class ArgSpainGenreQuery:
    """
    Películas y sus géneros de los años 00' con producción Argentina y Española.
    """
    def __init__(self, config):
        self.config = config
        self.results = []

        self.source_queue = self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results_queue")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )   
        

    def _calculate_and_publish_results(self):
        results = {
            "query": "Q1",
            "results": self.results
        }
        logger.info("RESULTS:" + str(results))

        self.rabbitmq_processor.publish(
            queue=self.config["DEFAULT"]["results_queue"],
            message=results
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

    def callback(self, ch, method, properties, body, input_queue):
    
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
            self.rabbitmq_processor.acknowledge(method)
            return

        try:
            movies_batch = json.loads(body)
            if not isinstance(movies_batch, list):
                logger.warning("❌ Expected a list (batch) of movies, skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            # Process the batch of movies
            self.process_batch(movies_batch)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.rabbitmq_processor.acknowledge(method)


    def process(self):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after processing a batch.
        """
        logger.info("Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgSpainGenreQuery(config)
    query.process()
