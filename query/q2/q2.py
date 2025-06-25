import configparser
import json
from collections import defaultdict

from common.duplicate_handler import DuplicateHandler
from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Query-Top5-Solo-Country-Budgets")


class SoloCountryBudgetQuery(QueryBase):
    """
    Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.
    """

    def __init__(self, config):
        self.source_queue = config["DEFAULT"].get("movies_solo_queue", "movies_solo")
        super().__init__(config, self.source_queue, logger_name="q2")

        self.budget_by_country_by_request = defaultdict(lambda: defaultdict(int))

    def _calculate_and_publish_results(self, client_id):
        """
        Calculate the top 5 countries by budget.
        """
        key = client_id
        budget_by_country = self.budget_by_country_by_request[key]

        sorted_countries = sorted(
            budget_by_country.items(), key=lambda x: x[1], reverse=True
        )
        top_5 = sorted_countries[:5]

        results = {
            "client_id": client_id,
            "query": "Q2",
            "results": top_5,
        }

        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"], message=results
        )

        logger.debug("LRU: Results published for client %s, %s", client_id, self.duplicate_handler.get_cache(client_id, self.source_queue))

    def process_movie(self, movie, client_id):
        """
        Process a single movie.
        """
        production_countries = movie.get("production_countries", [])
        if not production_countries:
            return

        country = production_countries[0].get("name")
        if not country:
            return

        budget = movie.get("budget", 0)
        self.budget_by_country_by_request[client_id][country] += budget

        logger.debug(
            "Processed movie for client %s: country=%s budget=%s",
            client_id,
            country,
            budget,
        )

    def callback(self, ch, method, properties, body, input_queue):
        """
        Reads from:
        - movies_solo queue (already filtered for productions with a single country)

        Collects:
        - country
        - total budget

        At the end, logs and stores top 5 countries that invested the most (without co-producing).

        NEXT: When receiving the end of stream flag, publish the results to a results queue.
        """
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        headers = getattr(properties, "headers", {}) or {}

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
                logger.error("Failed to decode EOS message")
                self.rabbitmq_processor.acknowledge(method)
                return

            if not self.client_manager.has_queue_received_eos_from_node(client_id, input_queue, node_id):
                self.client_manager.mark_eos(client_id, input_queue, node_id)
                self.write_eos_to_file(client_id)
                logger.info("EOS received from node %s for request %s.", node_id, client_id)
                if self.client_manager.has_received_all_eos(client_id, input_queue):
                    logger.info("All EOS received for request %s.", client_id)
                    self._calculate_and_publish_results(client_id)
            else:
                logger.warning(
                    "Duplicated EOS from node %s for request %s. Ignoring.",
                    node_id,
                    client_id,
                )
            
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

        try:
            movie = json.loads(body)

            # Normalize to list
            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("❌ Unexpected movie format: %s, skipping.", type(movie))
                self.rabbitmq_processor.acknowledge(method)
                return

            for single_movie in movie:
                self.process_movie(single_movie, client_id)
                
            self._write_data_to_file(client_id, self.budget_by_country_by_request[client_id], "partial_results")

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.duplicate_handler.add(client_id, input_queue, message_id)
        self.rabbitmq_processor.acknowledge(method)
        
    def update_data(self, client_id, key, data):
        if key == "partial_results":
            if client_id not in self.budget_by_country_by_request:
                self.budget_by_country_by_request[client_id] = defaultdict(int)
            for country, budget in data.items():
                self.budget_by_country_by_request[client_id][country] += budget
            logger.info(
                "Updated partial results for client %s: %s",
                client_id,
                self.budget_by_country_by_request[client_id],
            )
        else:
            logger.warning("Unknown key for update_data: %s", key)
        
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = SoloCountryBudgetQuery(config)
    query.process()
