import configparser
import json
import os
from common.mom import RabbitMQProcessor
from collections import defaultdict
from common.logger import get_logger

EOS_TYPE = "EOS" 
logger = get_logger("Query-Top5-Solo-Country-Budgets")


class SoloCountryBudgetQuery:
    """
    Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países. 
    """
    def __init__(self, config):
        self.config = config

        self.source_queue = self.config["DEFAULT"].get("movies_solo_queue", "movies_solo")
        self.target_queue = self.config["DEFAULT"].get("results_queue", "results_queue")

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}

        self.budget_by_country_by_request = defaultdict(lambda: defaultdict(int))
        self.eos_flags_by_request = defaultdict(set)

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queue
        )   


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
        del self.eos_flags_by_request[key]


    def callback(self, ch, method, properties, body, input_queue):
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        headers = properties.headers or {}

        client_id = headers.get("client_id")
        request_number = headers.get("request_number")
        if not client_id or request_number is None:
            logger.warning("❌ Missing client_id or request_number in headers. Skipping.")
            self.rabbitmq_processor.acknowledge(method)
            return
        
        key = (client_id, request_number)

        if msg_type == EOS_TYPE:
            try:
                data = json.loads(body)
                node_id = data.get("node_id")
            except json.JSONDecodeError:
                logger.error("Failed to decode EOS message")
                return
            
            if node_id in self.eos_flags_by_request[key]:
                logger.warning(f"Duplicated EOS from node {node_id} for request {key}")
                return

            self.eos_flags_by_request[key].add(node_id)
            logger.info(f"EOS received from node {node_id} for request {key}")

            if len(self.eos_flags_by_request[key]) == self.eos_to_await:
                logger.info(f"All EOS received for request {key}. Calculating results.")
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
                self.budget_by_country_by_request[key][country] += budget

            self.rabbitmq_processor.acknowledge(method)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)


    def process(self):
        """
        Reads from:
        - movies_solo queue (already filtered for productions with a single country)

        Collects:
        - country
        - total budget

        At the end, logs and stores top 5 countries that invested the most (without co-producing).

        NEXT: When receiving the end of stream flag, publish the results to a results queue.
        """
        logger.info("Node is online")


        for key, value in self.config["DEFAULT"].items():
            logger.info(f"{key}: {value}")

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
    query = SoloCountryBudgetQuery(config)
    query.process()