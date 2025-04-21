import configparser
import json
import pika
import time

from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Cleanup")

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
            channel (pika.channel.Channel): Channel object for RabbitMQ.
        """
        super().__init__(config)

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

        self.rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        self.connection = None
        self.channel = None
        self._eos_flags = {q: False for q in self.source_queues}

        
    def connect_to_rabbitmq(self):
        """
        Establish a connection to RabbitMQ and declare the necessary queues.
        """
        try:
            logger.info(f"Attempting to connect to RabbitMQ at {self.rabbitmq_host}")

            # Retry connection logic. Heartbeat and timeout settings are set to avoid connection issues
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.rabbitmq_host,
                heartbeat=600,
                blocked_connection_timeout=300
            ))
            self.channel = self.connection.channel()

            logger.info("Connected to RabbitMQ")

            for queue in self.source_queues:
                self.channel.queue_declare(queue=queue)

            for target in self.target_queues.values():
                if isinstance(target, list):
                    for queue in target:
                        self.channel.queue_declare(queue=queue)
                else:
                    self.channel.queue_declare(queue=target)

            return True
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during RabbitMQ connection: {e}")
            return False

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

    def _mark_eos_received(self, queue_name, msg_type):
        """
        Mark the end-of-stream (EOS) flag for the specified queue.
        If all source queues have received EOS, forward the EOS to the target queues.
        Args:
            queue_name (str): The name of the source queue that received EOS.
            msg_type (str): The type of message received (should be "EOS").
        """
        self._eos_flags[queue_name] = True
        logger.info(f"EOS marked for source queue: {queue_name}")

        targets = self.target_queues.get(queue_name)
        if targets:
            if isinstance(targets, list):
                for target in targets:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=target,
                        body=b'',
                        properties=pika.BasicProperties(type=msg_type)
                    )
                    logger.info(f"EOS sent to target queue: {target}")
            else:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=targets,
                    body=b'',
                    properties=pika.BasicProperties(type=msg_type)
                )
                logger.info(f"EOS sent to target queue: {targets}")

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to handle incoming messages from RabbitMQ.
        Handles EOS and batch message processing/publishing.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            logger.debug(f"Received message from {queue_name}, type: {msg_type}")
            
            # Handle EOS signal
            if msg_type == "EOS":
                logger.info(f"Received EOS from {queue_name}")
                self._mark_eos_received(queue_name, msg_type)
                return

            # Decode and validate input
            data_batch = json.loads(body)
            if not isinstance(data_batch, list):
                logger.warning(f"Expected a batch (list), got {type(data_batch)}. Skipping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Select correct cleanup function
            if queue_name == self.source_queues[0]:
                cleaned_batch = [self.clean_movie(d) for d in data_batch]
            elif queue_name == self.source_queues[1]:
                cleaned_batch = [self.clean_rating(d) for d in data_batch]
            elif queue_name == self.source_queues[2]:
                cleaned_batch = [self.clean_credit(d) for d in data_batch]
            else:
                logger.warning(f"Unknown queue name: {queue_name}. Skipping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Filter out failed cleans
            cleaned_batch = [c for c in cleaned_batch if c]

            if cleaned_batch:
                # Support single or multiple target queues
                target_queues = self.target_queues[queue_name]
                if not isinstance(target_queues, list):
                    target_queues = [target_queues]

                for target_queue in target_queues:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=target_queue,
                        body=json.dumps(cleaned_batch)
                    )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in message from {queue_name}: {e}")
            logger.error(f"Raw message: {body[:100]}...")
        except Exception as e:
            logger.error(f"Error processing message from {queue_name}: {e}")


    def process(self):

        """
        Main processing loop for the CleanupFilter. 
        This function sets up the RabbitMQ connection and starts consuming messages.
        It handles the connection, message consumption, and cleanup on shutdown.
        """
        logger.info("CleanupFilter is starting up")
        
        for key, value in self.config["DEFAULT"].items():
            logger.info(f"Config: {key}: {value}")
            
        if not self.connect_to_rabbitmq():
            logger.error("Exiting process due to connection failure")
            return
            
        logger.info(f"Setting up consumers for queues: {self.source_queues}")
        
        for queue in self.source_queues:
            logger.info(f"Setting up consumer for queue: {queue}")
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=lambda ch, method, props, body, q=queue: self.callback(ch, method, props, body, q),
                auto_ack=False
            )

        logger.info("All consumers set up. Waiting for messages...")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Graceful shutdown on SIGINT")
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            if self.connection and self.connection.is_open:
                self.connection.close()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    filter_instance = CleanupFilter(config)
    filter_instance.process()