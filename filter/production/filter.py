import configparser
import json
import os
import pika

from common.logger import get_logger
from common.filter_base import FilterBase
from common.mom import RabbitMQProcessor

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
        self.source_queues = [self.config["DEFAULT"].get("movies_clean_queue", "movies_clean")]
    
        self.target_queues = {
            self.source_queues[0]: [
                self.config["DEFAULT"].get("movies_argentina_queue", "movies_argentina"),
                self.config["DEFAULT"].get("movies_solo_queue", "movies_solo"),
                self.config["DEFAULT"].get("movies_arg_spain_queue", "movies_arg_spain")
            ]
        }

        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queues
        )   
        

    def _mark_eos_received(self, body, input_queue):
        """
        Mark the end of stream (EOS) for the given node
        """
        try:
            data = json.loads(body)
            node_id = data.get("node_id")
            count = data.get("count", 0)
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
            
        if node_id not in self._eos_flags:
            count +=1
        
        logger.debug(f"EOS count for node {node_id}: {count}")
        # If this isn't the last node, send the EOS message back to the input queue
        if count < self.nodes_of_type:
            # Send EOS back to input queue for other production nodes
            self.rabbitmq_processor.publish(
                queue=input_queue,
                message={"node_id": node_id, "count": count},
                msg_type=EOS_TYPE,
            )
        

        logger.debug(f"EOS received for Cleanup node {node_id}")
        self._eos_flags[node_id] = True

    def _send_eos(self):
        """
        Propagate the end of stream (EOS) to all output queues if all nodes have sent EOS.
        """
        logger.debug(f"Checking EOS flags: {self._eos_flags}")
        logger.debug(f"EOS to await: {self.eos_to_await}")
        logger.debug(f"EOS flags length: {len(self._eos_flags)}")
        if all(self._eos_flags.get(node) for node in self._eos_flags) and len(self._eos_flags) == int(self.eos_to_await):
            logger.info("All nodes have sent EOS. Sending EOS to output queues.")
            for queue_list in self.target_queues.values():
                for queue in queue_list:
                    self.rabbitmq_processor.publish(
                        queue=queue,
                        message={"node_id": self.node_id, "count": 0},
                        msg_type=EOS_TYPE,
                    )
                    logger.debug(f"EOS message sent to {queue}")
            self.rabbitmq_processor.stop_consuming()

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to process batched messages from the input queue.
        Filters movies by production countries and sends them in batches to the appropriate queues.
        """
        msg_type = properties.type if properties and properties.type else "UNKNOWN"

        if msg_type == EOS_TYPE:
            self._mark_eos_received(body, queue_name)
            if len(self.batch_arg) > 0:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][0],
                    message=self.batch_arg,
                )
                self.batch_arg.clear()
            if len(self.batch_solo) > 0:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][1],
                    message=self.batch_solo,
                )
                self.batch_solo.clear()
            if len(self.batch_arg_spain) > 0:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][2],
                    message=self.batch_arg_spain,
                )
                self.batch_arg_spain.clear()
            self._send_eos()
            self.rabbitmq_processor.acknowledge(method)
            return

        try:
            movies_batch = json.loads(body)
            if not isinstance(movies_batch, list):
                logger.warning("Expected a list of movies (batch), skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return
            for movie in movies_batch:
                country_dicts = movie.get("production_countries", [])
                country_names = [c.get("name") for c in country_dicts if "name" in c]

                logger.debug(f"Production countries: {country_names}")

                if "Argentina" in country_names:
                    self.batch_arg.append(movie)

                if len(country_names) == 1:
                    self.batch_solo.append(movie)

                if "Argentina" in country_names and "Spain" in country_names:
                    self.batch_arg_spain.append(movie)

            # Publish non-empty batches
            if self.batch_arg and len(self.batch_arg) >= self.batch_size:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][0],
                    message=self.batch_arg,
                )
                self.batch_arg.clear()
                logger.debug(f"Sent batch to {self.target_queues[queue_name][0]}")

            if self.batch_solo and len(self.batch_solo) >= self.batch_size:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][1],
                    message=self.batch_solo,
                )
                self.batch_solo.clear()
                logger.debug(f"Sent batch to {self.target_queues[queue_name][1]}")

            if self.batch_arg_spain and len(self.batch_arg_spain) >= self.batch_size:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[queue_name][2],
                    message=self.batch_arg_spain,
                )   
                self.batch_arg_spain.clear()
                logger.debug(f"Sent batch to {self.target_queues[queue_name][2]}")

            # Acknowledge the message after processing
            self.rabbitmq_processor.acknowledge(method)

        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            self.rabbitmq_processor.acknowledge(method)     


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
    production_filter = ProductionFilter(config)
    production_filter.process()
