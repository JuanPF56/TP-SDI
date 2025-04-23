import configparser
import json
import pika
from datetime import datetime
from common.logger import get_logger
from common.filter_base import FilterBase

EOS_TYPE = "EOS" 
logger = get_logger("Filter-Year")

class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self._eos_flags = {}
        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))
        self.processed_batch = {
            "arg_post_2000": [],
            "arg_spain_2000s": []
        }

    def _mark_eos_received(self, msg_type, output_queues, channel):
        """
        Mark the end of stream (EOS) for the given message type and propagate to target queues.
        """
        if msg_type in self._eos_flags:
            logger.info(f"EOS already received for {msg_type}")
            return
        self._eos_flags[msg_type] = True

        for queue in output_queues.values():
            channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=b'',
                properties=pika.BasicProperties(type=msg_type)
            )
            logger.info(f"EOS message sent to {queue}")

    def process(self):
        """
        Reads from:
        - movies_argentina
        - movies_arg_spain

        Writes to:
        - movies_arg_post_2000: Argentine-only movies after 2000
        - movies_arg_spain_2000s: Argentina+Spain movies between 2000-2009
        """
        logger.info("Node is online")
        logger.info("Configuration loaded successfully")
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"{key}: {value}")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")

        input_queues = {
            "argentina": self.config["DEFAULT"].get("movies_argentina_queue", "movies_argentina"),
            "arg_spain": self.config["DEFAULT"].get("movies_arg_spain_queue", "movies_arg_spain")
        }

        output_queues = {
            "arg_post_2000": self.config["DEFAULT"].get("movies_arg_post_2000_queue", "movies_arg_post_2000"),
            "arg_spain_2000s": self.config["DEFAULT"].get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        for queue in list(input_queues.values()) + list(output_queues.values()):
            channel.queue_declare(queue=queue)

        def callback(ch, method, properties, body):
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == EOS_TYPE:
                if len(self.processed_batch["arg_post_2000"]) > 0:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["arg_post_2000"],
                        body=json.dumps(self.processed_batch["arg_post_2000"]),
                        properties=pika.BasicProperties(type="batch")
                    )
                    self.processed_batch["arg_post_2000"] = []
                if len(self.processed_batch["arg_spain_2000s"]) > 0:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["arg_spain_2000s"],
                        body=json.dumps(self.processed_batch["arg_spain_2000s"]),
                        properties=pika.BasicProperties(type="batch")
                    )
                    self.processed_batch["arg_spain_2000s"] = []
                self._mark_eos_received(msg_type, output_queues, channel)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                movies_batch = json.loads(body)
                if not isinstance(movies_batch, list):
                    logger.warning("❌ Expected a list (batch) of movies, skipping.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                for movie in movies_batch:
                    title = movie.get("original_title")
                    date_str = movie.get("release_date", "")
                    release_year = self.extract_year(date_str)

                    logger.debug(f"Processing '{title}' released in {release_year} from queue '{method.routing_key}'")

                    if method.routing_key == input_queues["argentina"]:
                        if release_year and release_year > 2000:
                            self.processed_batch["arg_post_2000"].append(movie)
                            logger.debug(f"Prepared for {output_queues['arg_post_2000']}")
                    elif method.routing_key == input_queues["arg_spain"]:
                        if release_year and 2000 <= release_year <= 2009:
                            self.processed_batch["arg_spain_2000s"].append(movie)
                            logger.debug(f"Prepared for {output_queues['arg_spain_2000s']}")
                    else:
                        logger.warning("Unknown source queue")

                logger.info(f"Processed {len(movies_batch)} movies from queue '{method.routing_key}'")
                # Publish the processed batch to the output queues
                if self.processed_batch["arg_post_2000"] and len(self.processed_batch["arg_post_2000"]) >= self.batch_size:
                    # Publish the whole batch as a single message
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["arg_post_2000"],
                        body=json.dumps(self.processed_batch["arg_post_2000"]),
                        properties=pika.BasicProperties(type=msg_type)
                    )
                    self.processed_batch["arg_post_2000"] = []  # Clear the batch after sending
                    logger.debug(f"Sent entire batch to {output_queues['arg_post_2000']}")

                if self.processed_batch["arg_spain_2000s"] and len(self.processed_batch["arg_spain_2000s"]) >= self.batch_size:
                    # Publish the whole batch as a single message
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["arg_spain_2000s"],
                        body=json.dumps(self.processed_batch["arg_spain_2000s"]),
                        properties=pika.BasicProperties(type=msg_type)
                    )
                    self.processed_batch["arg_spain_2000s"] = []  # Clear the batch after sending
                    logger.debug(f"Sent entire batch to {output_queues['arg_spain_2000s']}")

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except json.JSONDecodeError:
                logger.warning("❌ Skipping invalid JSON")
                ch.basic_ack(delivery_tag=method.delivery_tag)

        for queue in input_queues.values():
            logger.info(f"Waiting for messages from '{queue}'...")
            channel.basic_consume(queue=queue, on_message_callback=callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            channel.stop_consuming()
        finally:
            connection.close()

    def extract_year(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").year
        except Exception as e:
            logger.warning(f"Invalid release_date '{date_str}': {e}")
            return None

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    year_filter = YearFilter(config)
    year_filter.process()
