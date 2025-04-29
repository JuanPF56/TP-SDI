import configparser
import json
import os

from common.logger import get_logger
logger = get_logger("Filter-Cleanup")

from common.filter_base import FilterBase
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS" 

class CleanupFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the CleanupFilter with the provided configuration.
        Args:
            config (ConfigParser): Configuration object containing RabbitMQ settings.
        
        Variables:
            source_queues (list): List of source queues to consume from (raw data).
            target_queues (dict): Dictionary mapping source queues to target queues (cleaned data).
            rabbitmq_host (str): Hostname of the RabbitMQ server.
            connection (pika.BlockingConnection): Connection object for RabbitMQ.
        """
        super().__init__(config)

        self.config = config

        self.source_queues = [
            self.config["DEFAULT"].get("movies_raw_queue", "movies_raw"),
            self.config["DEFAULT"].get("ratings_raw_queue", "ratings_raw"),
            self.config["DEFAULT"].get("credits_raw_queue", "credits_raw"),
        ]

        self.target_queues = {
            self.source_queues[0]: [
            self.config["DEFAULT"].get("movies_clean_for_production_queue", "movies_clean_for_production"),
            self.config["DEFAULT"].get("movies_clean_for_sentiment_queue", "movies_clean_for_sentiment"),
            ],
            self.source_queues[1]: self.config["DEFAULT"].get("ratings_clean_queue", "ratings_clean"),
            self.source_queues[2]: self.config["DEFAULT"].get("credits_clean_queue", "credits_clean"),
        }
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))

        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))
        self._eos_flags = {q: False for q in self.source_queues}

        self.batch = []
        # Instanciamos RabbitMQProcessor
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queues
        )   
        


    def clean_movie(self, data):
        """
        Callback function to process movie data to clean it.
        """
        required_fields = [
            "id", "original_title", "release_date", "budget",
            "revenue", "production_countries", "genres", "overview"
        ]
        if not all(data.get(field) is not None for field in required_fields):
            return None
        return {field: data[field] for field in required_fields}

    def clean_rating(self, data):
        """
        Callback function to process rating data to clean it.
        """
        required_fields = ["userId", "movieId", "rating"]
        if not all(data.get(field) is not None for field in required_fields):
            logger.debug(f"Skipping invalid rating data: {data}")
            return None
        return {
            "movie_id": data["movieId"],
            "rating": data["rating"]
        }

    def clean_credit(self, data):
        """
        Callback function to process credit data to clean it.
        """
        required_fields = ["id", "cast"]
        if not all(data.get(field) is not None for field in required_fields):
            logger.debug(f"Skipping invalid credit data: {data}")
            return None
        
        cast = []
        if data["cast"]:
            for actor in data["cast"]:
                if actor.get("name"):
                    cast.append(actor["name"])

        return {
            "id": data["id"],
            "cast": cast,
        }

    def _mark_eos_received(self, body, queue_name, msg_type):
        """
        Mark the end-of-stream (EOS) flag for the specified queue.
        Up the count of the EOS message, if it is not the last node of type put it back to the input queue.
        If all source queues have received EOS, forward the EOS to the target queues.
        Args:
            body (str): The message body received from RabbitMQ.
            queue_name (str): The name of the source queue that received EOS.
            msg_type (str): The type of message received (should be "EOS").
        """
        try:
            if body:
                data = json.loads(body)
                count = data.get("count", 0)
            else:
                count = 0
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
        
        # Send EOS back to the input queue for other cleanup nodes
        # only if this is not the last node of type
        if not self._eos_flags.get(queue_name):
            logger.debug(f"EOS marked for source queue: {queue_name}")
            self._eos_flags[queue_name] = True
            count += 1
        
        logger.debug(f"EOS count for queue {queue_name}: {count}")
        if count < self.nodes_of_type:
            logger.debug(f"Sending EOS back to input queue: {queue_name}")
            self.rabbitmq_processor.publish(
                queue=queue_name,
                message={"node_id": self.node_id, "count": count},
                msg_type=msg_type
            )

        targets = self.target_queues.get(queue_name)
        if targets:
            if isinstance(targets, list):
                for target in targets:
                    self.rabbitmq_processor.publish(
                        queue=target,
                        message={"node_id": self.node_id, "count": 0},
                        msg_type=msg_type
                    )
                    
                    logger.debug(f"EOS sent to target queue: {target}")
            else:
                self.rabbitmq_processor.publish(
                    queue=targets,
                    message={"node_id": self.node_id, "count": 0},
                    msg_type=msg_type
                )
                logger.debug(f"EOS sent to target queue: {targets}")
        
        if all(self._eos_flags.values()):
            logger.info("All source queues have sent EOS. Sending EOS to target queues.")
            self.rabbitmq_processor.stop_consuming()

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to handle incoming messages from RabbitMQ.
        Handles EOS and batch message processing/publishing.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            
            # Handle EOS signal
            if msg_type == EOS_TYPE:
                logger.debug(f"Received EOS from {queue_name}")
                if len(self.batch) > 0:
                    logger.warning("Batch not empty when EOS received. Publishing remaining batch.")
                    target_queues = self.target_queues[queue_name]
                    if not isinstance(target_queues, list):
                        target_queues = [target_queues]
                    for target_queue in target_queues:
                        self.rabbitmq_processor.publish(
                            queue=target_queue,
                            message=self.batch,
                        )
                    self.batch.clear()
                self._mark_eos_received(body, queue_name, msg_type)
                self.rabbitmq_processor.acknowledge(method)
                return

            # Decode and validate input
            data_batch = json.loads(body)
            if not isinstance(data_batch, list):
                logger.warning(f"Expected a batch (list), got {type(data_batch)}. Skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            # Select correct cleanup function
            if queue_name == self.source_queues[0]:
                self.batch.extend([self.clean_movie(d) for d in data_batch])
            elif queue_name == self.source_queues[1]:
                self.batch.extend([self.clean_rating(d) for d in data_batch])
            elif queue_name == self.source_queues[2]:
                self.batch.extend([self.clean_credit(d) for d in data_batch])
            else:
                logger.warning(f"Unknown queue name: {queue_name}. Skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            # TODO: Use a constant for batch size and assign different values for
            # different queues
            batch_sz = 10000 if queue_name == self.source_queues[1] else \
                50 if queue_name == self.source_queues[0] else self.batch_size

            if self.batch and len(self.batch) >= batch_sz:
                # Support single or multiple target queues
                target_queues = self.target_queues[queue_name]
                if not isinstance(target_queues, list):
                    target_queues = [target_queues]
            
                for target_queue in target_queues:
                    self.rabbitmq_processor.publish(
                        queue=target_queue,
                        message=self.batch,
                        msg_type=msg_type
                    )
                    
                self.batch.clear()
            self.rabbitmq_processor.acknowledge(method)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in message from {queue_name}: {e}")
            logger.error(f"Raw message: {body[:100]}...")
        except Exception as e:
            logger.error(f"Error processing message from {queue_name}: {e}")


    def process(self):
        logger.info("CleanupFilter is starting up")
        
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"Config: {key}: {value}")
            
        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return
        
        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Graceful shutdown on SIGINT")
            self.rabbitmq_processor.close()  # Use the processor's close method
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            self.rabbitmq_processor.close()  # Use the processor's close method


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    filter_instance = CleanupFilter(config)
    filter_instance.process()