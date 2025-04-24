import configparser
import json
import os
import pika

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
        self.budget_by_country = defaultdict(int)
        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_solo_queue", "movies_solo")

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        self.channel = self.connection.channel()

        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.eos_flags = {}

        self.channel.queue_declare(queue=input_queue)
        self.channel.queue_declare(queue=config["DEFAULT"]["results_queue"])



    def _calculate_and_publish_results(self):
        """
        Calculate the top 5 countries by budget.
        """
        sorted_countries = sorted(self.budget_by_country.items(), key=lambda x: x[1], reverse=True)
        top_5 = sorted_countries[:5]
        results = {
            "query": "Q2",
            "results": top_5
        }
        logger.info("RESULTS:" + str(results))
        self.channel.basic_publish(
            exchange='',
            routing_key=self.config["DEFAULT"]["results_queue"],
            body=json.dumps(results),
        )


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

        def callback(ch, method, properties, body):
            try:
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
                        return
                    if len(self.eos_flags) == int(self.eos_to_await):
                        logger.info("All nodes have sent EOS. Calculating results...")
                        self._calculate_and_publish_results()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                movies_batch = json.loads(body)
                if not isinstance(movies_batch, list):
                    logger.info("❌ Expected a list (batch) of movies, skipping.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                for movie in movies_batch:
                    production_countries = movie.get("production_countries", [])
                    if not production_countries:
                        continue

                    country = production_countries[0].get("name")
                    if not country:
                        continue

                    budget = movie.get("budget", 0)
                    self.budget_by_country[country] += budget

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=self.config["DEFAULT"]["movies_solo_queue"], on_message_callback=callback)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.channel.stop_consuming()
        finally:
            self.connection.close()



if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    query = SoloCountryBudgetQuery(config)
    query.process()