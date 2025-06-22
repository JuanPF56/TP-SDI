import configparser
import json

from collections import defaultdict
from common.duplicate_handler import DuplicateHandler
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Query-Arg-Prod-Ratings")


class ArgProdRatingsQuery(QueryBase):
    """
    Película de producción Argentina estrenada a partir del 2000,
    con mayor y con menor promedio de rating.
    """

    def __init__(self, config):
        self.source_queue = config["DEFAULT"].get("movies_ratings_queue", "movies_ratings")
        super().__init__(config, self.source_queue, logger_name="q3")

        self.duplicate_handler = DuplicateHandler()

        self.movie_ratings = defaultdict(
            lambda: defaultdict(dict)
        )  # ratings[client_id, req_num][movie_id]

    def _calculate_and_publish_results(self, client_id):
        logger.info("Calculating results for client %s ...", client_id)

        results = {
            "highest": {"title": None, "rating": 0},
            "lowest": {"title": None, "rating": float("inf")},
        }

        found_valid_ratings = False
        client_movies = self.movie_ratings[client_id]

        for movie_id, movie_data in client_movies.items():
            if movie_data["rating_count"] > 0:
                found_valid_ratings = True
                logger.debug(
                    "Movie ID: %s, Ratings: %d, Sum: %d",
                    movie_id,
                    movie_data["rating_count"],
                    movie_data["rating_sum"],
                )
                avg = movie_data["rating_sum"] / movie_data["rating_count"]

                if avg > results["highest"]["rating"]:
                    results["highest"] = {
                        "title": movie_data["original_title"],
                        "rating": avg,
                    }
                if avg < results["lowest"]["rating"]:
                    results["lowest"] = {
                        "title": movie_data["original_title"],
                        "rating": avg,
                    }

        results_msg = {
            "client_id": client_id,
            "query": "Q3",
            "results": {} if not found_valid_ratings else results,
        }

        logger.info("RESULTS for client %s: %s", client_id, results_msg)
        self.rabbitmq_processor.publish(self.target_queue, results_msg)
        logger.debug("LRU: Results published for client %s, %s", client_id, self.duplicate_handler.get_cache(client_id, self.source_queue))

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties else "UNKNOWN"
        headers = properties.headers or {}

        client_id = headers.get("client_id")
        message_id = headers.get("message_id")

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
                logger.error("❌ Failed to decode EOS message.")
                self.rabbitmq_processor.acknowledge(method)
                return

            logger.info(
                "EOS received for node %s in queue %s, eos flags: %s",
                node_id,
                input_queue,
                self.client_manager.get_eos_flags(client_id),
            )

            if not self.client_manager.has_queue_received_eos_from_node(client_id, input_queue, node_id):
                self.client_manager.mark_eos(client_id, input_queue, node_id)
                logger.info("✅ EOS received from node %s.", node_id)
                if self.client_manager.has_received_all_eos(client_id, input_queue):
                    logger.info("✅ All EOS received. Proceeding to calculate results.")
                    self._calculate_and_publish_results(client_id)
            else:
                logger.warning("⚠️ Duplicate EOS from node %s. Ignored.", node_id)

            self.rabbitmq_processor.acknowledge(method)
            return
        
        if message_id is None:
            logger.error("Missing message_id in headers")
            self.rabbitmq_processor.acknowledge(method)
            return
        
        if self.duplicate_handler.is_duplicate(client_id, input_queue, message_id):
            logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
            self.rabbitmq_processor.acknowledge(method)
            return

        # Handle movie rating data
        try:
            movies = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("❌ Invalid JSON body. Skipping message.")
            self.rabbitmq_processor.acknowledge(method)
            return

        for movie in movies:
            movie_id = movie.get("id")
            if movie_id is None:
                continue

            movie_data = self.movie_ratings[client_id].setdefault(
                movie_id,
                {
                    "original_title": movie.get("original_title", "Unknown"),
                    "rating_sum": 0,
                    "rating_count": 0,
                },
            )

            movie_data["rating_sum"] += movie.get("rating", 0)
            movie_data["rating_count"] += 1

        self.duplicate_handler.add(client_id, input_queue, message_id)
        self.rabbitmq_processor.acknowledge(method)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdRatingsQuery(config)
    query.process()
