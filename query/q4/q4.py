import configparser
import json

from collections import defaultdict
from common.duplicate_handler import DuplicateHandler
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Query-Arg-Prod-Ratings")


class ArgProdActorsQuery(QueryBase):
    """
    Top 10 de actores con mayor participación en películas de producción
    Argentina con fecha de estreno posterior al 2000.
    """

    def __init__(self, config):
        self.source_queue = config["DEFAULT"].get(
            "movies_credits_queue", "movies_credits"
        )
        super().__init__(config, self.source_queue, logger_name="q4")

        self.duplicate_handler = DuplicateHandler()

        self.actor_participations = defaultdict(dict)

    def _calculate_and_publish_results(self, client_id):
        logger.info("Calculating results...")
        key = client_id

        if not self.actor_participations:
            logger.info("No actors participations found in the requested movies.")

            # Send empty results to client
            results_msg = {
                "client_id": client_id,
                "query": "Q4",
                "results": {"actors": []},
            }

        else:
            # Sort actors by participation count in descending order of participations and then alfabetical
            # and get the top 10
            all_sorted_actors = sorted(
                self.actor_participations[key].values(),
                key=lambda x: (-x["count"], x["name"]),
            )
            logger.info("Top actors: %s", all_sorted_actors)

            sorted_actors = all_sorted_actors[:10]

            results_msg = {
                "client_id": client_id,
                "query": "Q4",
                "results": {"actors": sorted_actors},
            }

        logger.info("RESULTS: %s", results_msg)
        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"], message=results_msg
        )
        logger.debug(
            "LRU: Results published for client %s, %s",
            client_id,
            self.duplicate_handler.get_cache(client_id, self.source_queue),
        )

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties else "UNKNOWN"
        headers = properties.headers or {}

        client_id = headers.get("client_id")
        message_id = headers.get("message_id")
        sub_id = headers.get("sub_id")
        expected = headers.get("expected")
        message_key = f"{message_id}:{sub_id}/{expected}"

        if client_id is None:
            logger.warning("❌ Missing client_id in headers. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.client_manager.add_client(client_id)
        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                self.rabbitmq_processor.acknowledge(method)
                return
            if not self.client_manager.has_queue_received_eos_from_node(
                client_id, input_queue, node_id
            ):
                self.client_manager.mark_eos(client_id, input_queue, node_id)
                if self.client_manager.has_received_all_eos(client_id, input_queue):
                    logger.info("All nodes have sent EOS. Calculating results...")
                    self._calculate_and_publish_results(client_id)
            else:
                logger.warning(
                    "EOS message for node %s already received. Ignoring duplicate.",
                    node_id,
                )
            self.rabbitmq_processor.acknowledge(method)
            return

        if message_key is None:
            logger.error("Missing message_key in headers")
            self.rabbitmq_processor.acknowledge(method)
            return

        if self.duplicate_handler.is_duplicate(client_id, input_queue, message_key):
            logger.info(
                "Duplicate message detected: %s. Acknowledging without processing.",
                message_key,
            )
            self.rabbitmq_processor.acknowledge(method)
            return

        try:
            movies = json.loads(body)
            if isinstance(movies, dict):
                movies = [movies]
            elif not isinstance(movies, list):
                logger.warning("❌ Skipping invalid message format")
                self.rabbitmq_processor.acknowledge(method)
                return
        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        key = client_id

        for movie in movies:
            logger.info("Processing movie: %s", movie)
            if movie.get("cast") is None:
                logger.warning("❌ Skipping movie without cast")
                self.rabbitmq_processor.acknowledge(method)
                return
            for actor in movie["cast"]:
                if actor not in self.actor_participations[key]:
                    self.actor_participations[key][actor] = {"name": actor, "count": 0}
                self.actor_participations[key][actor]["count"] += 1

        self.duplicate_handler.add(client_id, input_queue, message_key)
        self.rabbitmq_processor.acknowledge(method)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdActorsQuery(config)
    query.process()
