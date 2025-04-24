import configparser
import json
import os
import pika

from common.logger import get_logger
from common.filter_base import FilterBase

EOS_TYPE = "EOS" 

logger = get_logger("Filter-Production")

class ProductionFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self._eos_flags = {}
        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))
        self.batch_arg = []
        self.batch_solo = []
        self.batch_arg_spain = []
        
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))

    def _mark_eos_received(self, body, channel, input_queue):
        """
        Mark the end of stream (EOS) for the given node
        """
        try:
            data = json.loads(body)
            node_id = data.get("node_id")
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
        
        # Send EOS back to input queue for other production nodes
        channel.basic_publish(
            exchange='',
            routing_key=input_queue,
            body=json.dumps({"node_id": node_id}),
            properties=pika.BasicProperties(type=EOS_TYPE)
        )

        logger.info(f"EOS received for Cleanup node {node_id}")
        self._eos_flags[node_id] = True

    def _send_eos(self, output_queues, channel):
        """
        Propagate the end of stream (EOS) to all output queues if all nodes have sent EOS.
        """
        logger.info(f"Checking EOS flags: {self._eos_flags}")
        logger.info(f"EOS to await: {self.eos_to_await}")
        logger.info(f"EOS flags length: {len(self._eos_flags)}")
        if all(self._eos_flags.get(node) for node in self._eos_flags) and len(self._eos_flags) == int(self.eos_to_await):
            logger.info("All nodes have sent EOS. Sending EOS to output queues.")
            for queue in output_queues.values():
                channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=json.dumps({"node_id": self.node_id}),
                    properties=pika.BasicProperties(type=EOS_TYPE)
                )
                logger.info(f"EOS message sent to {queue}")

    def process(self):
        """
        Main processing function for the ProductionFilter.

        It filters movies based on their production countries and sends them to the respective queues:
        - movies_argentina: for movies produced in Argentina
        - movies_solo: for movies produced in only one country
        - movies_arg_spain: for movies produced in both Argentina and Spain
        """
        logger.info("Node is online")
        logger.info("Configuration loaded successfully")
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"{key}: {value}")

        rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        input_queue = self.config["DEFAULT"].get("movies_clean_queue", "movies_clean")
        output_queues = {
            "movies_argentina": self.config["DEFAULT"].get("movies_argentina_queue", "movies_argentina"),
            "movies_solo": self.config["DEFAULT"].get("movies_solo_queue", "movies_solo"),
            "movies_arg_spain": self.config["DEFAULT"].get("movies_arg_spain_queue", "movies_arg_spain")
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        channel.queue_declare(queue=input_queue)
        for queue in output_queues.values():
            channel.queue_declare(queue=queue)

        def callback(ch, method, properties, body):
            """
            Callback function to process batched messages from the input queue.
            Filters movies by production countries and sends them in batches to the appropriate queues.
            """
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == EOS_TYPE:
                self._mark_eos_received(body, channel, input_queue)
                if len(self.batch_arg) > 0:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_argentina"],
                        body=json.dumps(self.batch_arg)
                    )
                    self.batch_arg.clear()
                if len(self.batch_solo) > 0:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_solo"],
                        body=json.dumps(self.batch_solo)
                    )
                    self.batch_solo.clear()
                if len(self.batch_arg_spain) > 0:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_arg_spain"],
                        body=json.dumps(self.batch_arg_spain)
                    )
                    self.batch_arg_spain.clear()
                self._send_eos(output_queues, channel)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                movies_batch = json.loads(body)
                if not isinstance(movies_batch, list):
                    logger.warning("Expected a list of movies (batch), skipping.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                for movie in movies_batch:
                    country_dicts = movie.get("production_countries", [])
                    country_names = [c.get("name") for c in country_dicts if "name" in c]

                    logger.debug(f"Processing movie: {movie.get('original_title')}")
                    logger.debug(f"Production countries: {country_names}")

                    if "Argentina" in country_names:
                        self.batch_arg.append(movie)

                    if len(country_names) == 1:
                        self.batch_solo.append(movie)

                    if "Argentina" in country_names and "Spain" in country_names:
                        self.batch_arg_spain.append(movie)

                # Publish non-empty batches
                if self.batch_arg and len(self.batch_arg) >= self.batch_size:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_argentina"],
                        body=json.dumps(self.batch_arg)
                    )
                    self.batch_arg.clear()
                    logger.debug(f"Sent batch to {output_queues['movies_argentina']}")

                if self.batch_solo and len(self.batch_solo) >= self.batch_size:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_solo"],
                        body=json.dumps(self.batch_solo)
                    )
                    self.batch_solo.clear()
                    logger.debug(f"Sent batch to {output_queues['movies_solo']}")

                if self.batch_arg_spain and len(self.batch_arg_spain) >= self.batch_size:
                    channel.basic_publish(
                        exchange='',
                        routing_key=output_queues["movies_arg_spain"],
                        body=json.dumps(self.batch_arg_spain)
                    )
                    self.batch_arg_spain.clear()
                    logger.debug(f"Sent batch to {output_queues['movies_arg_spain']}")

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                logger.error(f"Failed to process batch: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)        
        
        logger.info(f"Waiting for messages from '{input_queue}'...")
        channel.basic_consume(queue=input_queue, on_message_callback=callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            channel.stop_consuming()
        finally:
            connection.close()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    production_filter = ProductionFilter(config)
    production_filter.process()
