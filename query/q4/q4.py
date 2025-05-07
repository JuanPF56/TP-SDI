import configparser
import json
import os
import pika
from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientManager
from collections import defaultdict
from common.client_state_manager import ClientState
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
        self.actor_participations = defaultdict(dict)

        self.source_queue = self.config["DEFAULT"].get("movies_credits_queue", "movies_credits")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.node_name = os.getenv("NODE_NAME", "unknown")
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )
        self.client_manager = ClientManager(
            expected_queues=self.source_queue,
            nodes_to_await=self.eos_to_await,
        )

    def _calculate_and_publish_results(self, client_state: ClientState):
        logger.info("Calculating results...")
        key = (client_state.client_id, client_state.request_id)

        if not self.actor_participations:
            logger.info("No actors participations found in the requested movies.")

            # Send empty results to client
            results_msg = {
                "client_id": client_state.client_id,
                "request_number": client_state.request_id,
                "query": "Q4",
                "results": {
                    "actors": []
                }
            }
            
        else:
            # Sort actors by participation count in descending order and get the top 10
            sorted_actors = sorted(
                self.actor_participations[key].values(),
                key=lambda x: x["count"],
                reverse=True
            )[:10]

            results_msg = {
                "client_id": client_state.client_id,
                "request_number": client_state.request_id,
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
        # TODO: Clean up memory

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties else "UNKNOWN"
        headers = properties.headers or {}

        client_id, request_number = headers.get("client_id"),  headers.get("request_number")

        if not client_id or request_number is None:
            logger.warning("❌ Missing client_id or request_number in headers. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        client_state = self.client_manager.add_client(client_id, request_number)
        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                return
            if not client_state.has_queue_received_eos_from_node(input_queue, node_id):
                client_state.mark_eos(input_queue, node_id)
                logger.info(f"EOS received for node {node_id}.")
            else:
                logger.warning(f"EOS message for node {node_id} already received. Ignoring duplicate.")
                self.rabbitmq_processor.acknowledge(method)
                return
            if client_state.has_received_all_eos(input_queue):
                logger.info("All nodes have sent EOS. Calculating results...")
                self._calculate_and_publish_results(client_state)   
            self.rabbitmq_processor.acknowledge(method)
            return
        try:
            movies = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return
        
        key = (client_id, request_number)
        
        for movie in movies:
            if movie.get("cast") is None:
                logger.warning("❌ Skipping movie without cast")
                self.rabbitmq_processor.acknowledge(method)
                return
            for actor in movie["cast"]:
                if actor not in self.actor_participations[key]:
                    self.actor_participations[key][actor] = {
                        "name": actor,
                        "count": 0
                    }
                self.actor_participations[key][actor]["count"] += 1

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