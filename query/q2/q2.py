import configparser
import json
from collections import defaultdict

from common.query_base import QueryBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Query-Top5-Solo-Country-Budgets")


class SoloCountryBudgetQuery(QueryBase):
    """
    Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.
    """

    def __init__(self, config):
        source_queue = config["DEFAULT"].get("movies_solo_queue", "movies_solo")
        super().__init__(config, source_queue, logger_name="q2")

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

        # Limpieza de memoria
        del self.budget_by_country_by_request[key]
        self.client_manager.remove_client(client_id)

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
        if not client_id:
            logger.warning("❌ Missing client_id in headers. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return

        client_state = self.client_manager.add_client(client_id)

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                self.rabbitmq_processor.acknowledge(method)
                return

            if client_state.has_queue_received_eos_from_node(input_queue, node_id):
                logger.warning(
                    "Duplicated EOS from node %s for request %s. Ignoring.",
                    node_id,
                    client_id,
                )
                self.rabbitmq_processor.acknowledge(method)
                return

            client_state.mark_eos(input_queue, node_id)
            logger.info("EOS received from node %s for request %s.", node_id, client_id)

            if client_state.has_received_all_eos(input_queue):
                logger.info("All EOS received for request %s.", client_id)
                self._calculate_and_publish_results(client_id)

            self.rabbitmq_processor.acknowledge(method)
            return

        # Normal message (single movie)
        try:
            movie = json.loads(body)
            if not isinstance(movie, dict):
                logger.info("❌ Expected a single movie object, skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            self.process_movie(movie, client_id)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
            return

        self.rabbitmq_processor.acknowledge(method)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = SoloCountryBudgetQuery(config)
    query.process()
