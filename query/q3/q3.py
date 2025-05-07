import configparser
import json

from collections import defaultdict
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger
logger = get_logger("Query-Arg-Prod-Ratings")

class ArgProdRatingsQuery(QueryBase):
    """
    Película de producción Argentina estrenada a partir del 2000,
    con mayor y con menor promedio de rating.
    """
    def __init__(self, config):
        source_queue = config["DEFAULT"].get("movies_ratings_queue", "movies_ratings")
        super().__init__(config, source_queue, logger_name="q3")

        self.movie_ratings = defaultdict(lambda: defaultdict(dict))  # ratings[client_id, req_num][movie_id]

    def _calculate_and_publish_results(self, client_id, request_number):
        logger.info(f"Calculating results for client {client_id}, request {request_number}...")

        results = {
            "highest": {"title": None, "rating": 0},
            "lowest": {"title": None, "rating": float("inf")}
        }
 
        found_valid_ratings = False
        client_movies = self.movie_ratings[client_id][request_number]

        for movie_id, movie_data in client_movies.items():
            if movie_data["rating_count"] > 0:
                found_valid_ratings = True
                avg = movie_data["rating_sum"] / movie_data["rating_count"]

                if avg > results["highest"]["rating"]:
                    results["highest"] = {"title": movie_data["original_title"], "rating": avg}
                if avg < results["lowest"]["rating"]:
                    results["lowest"] = {"title": movie_data["original_title"], "rating": avg}

        results_msg = {
            "client_id": client_id,
            "request_number": request_number,
            "query": "Q3",
            "results": {} if not found_valid_ratings else results
        }

        logger.info(f"RESULTS for client {client_id}, req {request_number}: {results_msg}")
        self.rabbitmq_processor.publish(self.target_queue, results_msg)
        del self.movie_ratings[client_id][request_number]
        #self.client_manager.remove_client(client_id, request_number)

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
                logger.error("❌ Failed to decode EOS message.")
                self.rabbitmq_processor.acknowledge(method)
                return
            
            logger.info(f"EOS received for node {node_id} in queue {input_queue}, eos flgas: {client_state.eos_flags}")

            if not client_state.has_queue_received_eos_from_node(input_queue, node_id):
                client_state.mark_eos(input_queue, node_id)
                logger.info(f"✅ EOS received from node {node_id}.")

            else:
                logger.warning(f"⚠️ Duplicate EOS from node {node_id}. Ignored.")
                self.rabbitmq_processor.acknowledge(method)
                return

            if client_state.has_received_all_eos(input_queue):
                logger.info("✅ All EOS received. Proceeding to calculate results.")
                self._calculate_and_publish_results(client_id, request_number)

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

            movie_data = self.movie_ratings[client_id][request_number].setdefault(movie_id, {
                "original_title": movie.get("original_title", "Unknown"),
                "rating_sum": 0,
                "rating_count": 0,
            })

            movie_data["rating_sum"] += movie.get("rating", 0)
            movie_data["rating_count"] += 1

        self.rabbitmq_processor.acknowledge(method)

    def process(self):
        logger.info("🎬 Query-Arg-Prod-Ratings Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("❌ Could not connect to RabbitMQ. Exiting.")
            return

        try:
            logger.info("📥 Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("👋 Interrupted. Shutting down.")
        finally:
            logger.info("🔌 Closing RabbitMQ connection.")
            self.rabbitmq_processor.stop_consuming()
            self.rabbitmq_processor.close()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = ArgProdRatingsQuery(config)
    query.process()
