import configparser
import json
import os
import pika
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS" 

from common.logger import get_logger

logger = get_logger("Query-Arg-Prod-Ratings")

class ArgProdActorsQuery:
    """
    Top 10 de actores con mayor participación en películas de producción 
    Argentina con fecha de estreno posterior al 2000.
    """
    def __init__(self, config):
        self.config = config
        self.actor_participations = {}

        self.source_queue = self.config["DEFAULT"].get("movies_credits_queue", "movies_credits")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )   
        


    def _calculate_and_publish_results(self):
        logger.info("Calculating results...")

        if not self.actor_participations:
            logger.info("No actors participations found in the requested movies.")

            # Send empty results to client
            results_msg = {
                "query": "Q4",
                "results": {
                    "actors": []
                }
            }
            
        else:
            # Sort actors by participation count in descending order and get the top 10
            sorted_actors = sorted(
                self.actor_participations.values(),
                key=lambda x: x["count"],
                reverse=True
            )[:10]

            results_msg = {
                "query": "Q4",
                "results": {
                    "actors": sorted_actors
                }
            }

        logger.info("RESULTS:" + str(results_msg))
        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"],
            message=results_msg
        )

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
                self.rabbitmq_processor.acknowledge(method)
                return
            if len(self.eos_flags) == int(self.eos_to_await):
                logger.info("All nodes have sent EOS. Calculating results...")
                self._calculate_and_publish_results()
            self.rabbitmq_processor.acknowledge(method)
            return
        try:
            movies = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return
        for movie in movies:
            if movie.get("cast") is None:
                logger.warning("❌ Skipping movie without cast")
                self.rabbitmq_processor.acknowledge(method)
                return
            
            for actor in movie["cast"]:
                if actor not in self.actor_participations:
                    self.actor_participations[actor] = {
                        "name": actor,
                        "count": 0
                    }
                self.actor_participations[actor]["count"] += 1

        self.rabbitmq_processor.acknowledge(method)


    def process(self):
        logger.info("Node is online")
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
    query = ArgProdActorsQuery(config)
    query.process()