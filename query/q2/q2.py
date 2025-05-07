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


    def _calculate_and_publish_results(self, client_id, request_number):
        """
        Calculate the top 5 countries by budget.
        """
        key = (client_id, request_number)
        budget_by_country = self.budget_by_country_by_request[key]

        sorted_countries = sorted(budget_by_country.items(), key=lambda x: x[1], reverse=True)
        top_5 = sorted_countries[:5]

        results = {
            "client_id": client_id,
            "request_number": request_number,
            "query": "Q2",
            "results": top_5
        }

        logger.info(f"RESULTS for {key}: {results}")

        self.rabbitmq_processor.publish(
            target=self.config["DEFAULT"]["results_queue"],
            message=results
        )

        # Limpieza de memoria
        del self.budget_by_country_by_request[key]
        #self.client_manager.remove_client(client_id, request_number)


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
        headers = properties.headers or {}

        client_id, request_number = headers.get("client_id"), headers.get("request_number")
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
            
            if client_state.has_queue_received_eos_from_node(input_queue, node_id):
                logger.warning(f"Duplicated EOS from node {node_id} for request {client_id}-{request_number}. Ignoring.")
                return

            client_state.mark_eos(input_queue, node_id)
            logger.info(f"EOS received from node {node_id} for request {client_id}-{request_number}.")

            if client_state.has_received_all_eos(input_queue):
                logger.info(f"All EOS received for request {client_id}-{request_number}.")
                self._calculate_and_publish_results(client_id, request_number)

            self.rabbitmq_processor.acknowledge(method)
            return

        # Normal message (batch of movies)
        try:
            movies_batch = json.loads(body)
            if not isinstance(movies_batch, list):
                logger.info("❌ Expected a list (batch) of movies, skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return
            
            for movie in movies_batch:
                production_countries = movie.get("production_countries", [])
                if not production_countries:
                    continue

                country = production_countries[0].get("name")
                if not country:
                    continue

                budget = movie.get("budget", 0)
                self.budget_by_country_by_request[(client_id, request_number)][country] += budget

            self.rabbitmq_processor.acknowledge(method)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = SoloCountryBudgetQuery(config)
    query.process()