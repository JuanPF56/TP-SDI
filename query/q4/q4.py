import configparser
import json

from collections import defaultdict
from common.client_state_manager import ClientState
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger
logger = get_logger("Query-Arg-Prod-Ratings")

class ArgProdActorsQuery(QueryBase):
    """
    Top 10 de actores con mayor participación en películas de producción 
    Argentina con fecha de estreno posterior al 2000.
    """
    def __init__(self, config):
        source_queue = config["DEFAULT"].get("movies_credits_queue", "movies_credits")
        super().__init__(config, source_queue, logger_name="q4")

        self.actor_participations = defaultdict(dict)

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

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdActorsQuery(config)
    query.process()