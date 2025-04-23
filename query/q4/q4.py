import configparser
import json
import pika
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
        self.actor_participations = {}

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_credits_queue", "movies_credits")

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=input_queue)
        self.channel.queue_declare(queue=config["DEFAULT"]["results_queue"])


    def _calculate_and_publish_results(self):
        logger.info("Calculating results...")

        if not self.actor_participations:
            # TODO: Send empty result to client
            logger.info("No actor participations received.")
            return

        # Sort actors by participation count in descending order
        # and get the top 10

        sorted_actors = sorted(self.actor_participations.values(), key=lambda x: x["count"], reverse=True)[:10]        

        results_msg = {
            "query": "Q4",
            "results": {
                "actors": sorted_actors
            }
        }

        logger.info("RESULTS:" + str(results_msg))

        self.channel.basic_publish(
            exchange='',
            routing_key=self.config["DEFAULT"]["results_queue"],
            body=json.dumps(results_msg),
        )


    def process(self):
        logger.info("Node is online")

        def callback(ch, method, properties, body):
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            if msg_type == EOS_TYPE:
                logger.info("Received EOS message, stopping consumption.")
                self._calculate_and_publish_results()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                movies = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            for movie in movies:
                if movie.get("cast") is None:
                    logger.warning("❌ Skipping movie without cast")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                
                for actor in movie["cast"]:
                    if actor not in self.actor_participations:
                        self.actor_participations[actor] = {
                            "name": actor,
                            "count": 0
                        }
                    self.actor_participations[actor]["count"] += 1

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=self.config["DEFAULT"]["movies_credits_queue"], on_message_callback=callback)

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
    query = ArgProdActorsQuery(config)
    query.process()