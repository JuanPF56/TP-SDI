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
            self.source_queues[0]: self.config["DEFAULT"].get("movies_clean_queue", "movies_clean"),
            self.source_queues[1]: self.config["DEFAULT"].get("ratings_clean_queue", "ratings_clean"),
            self.source_queues[2]: self.config["DEFAULT"].get("credits_clean_queue", "credits_clean"),
        }

        self.rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
        self.connection = None
        self.channel = None
        
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

            for queue in self.source_queues + list(self.target_queues.values()):
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=queue)

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
        if not data.get("title") or data.get("budget") is None:
            logger.debug(f"Skipping invalid movie data: {data}")
            return None
        return {
            "title": data["title"],
            "release_date": data.get("release_date"),
            "budget": data.get("budget"),
            "revenue": data.get("revenue"),
            "production_countries": data.get("production_countries"),
            "genres": data.get("genres")
        }

    def clean_rating(self, data):
        """
        Callback function to process rating data to clean it.
        """
        if not data.get("userId") or not data.get("movieId") or data.get("rating") is None:
            logger.debug(f"Skipping invalid rating data: {data}")
            return None
        return {
            "userId": data["userId"],
            "movieId": data["movieId"],
            "rating": data["rating"]
        }

    def clean_credit(self, data):
        """
        Callback function to process credit data to clean it.
        """
        if not data.get("movieId") or not data.get("cast"):
            logger.debug(f"Skipping invalid credit data: {data}")
            return None
        return {
            "movieId": data["movieId"],
            "cast": data["cast"],
            "crew": data.get("crew", [])
        }

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to handle incoming messages from RabbitMQ.
        Args:
            ch (pika.channel.Channel): The channel object.
            method (pika.spec.Basic.Deliver): Delivery method.
            properties (pika.spec.BasicProperties): Message properties.
            body (bytes): The message body.
            queue_name (str): The name of the source queue.
        """
        try:
            logger.info(f"Received message from {queue_name}, length: {len(body)}")
            data = json.loads(body)
            cleaned = None

            if queue_name == self.source_queues[0]:
                cleaned = self.clean_movie(data)
            elif queue_name == self.source_queues[1]:
                cleaned = self.clean_rating(data)
            elif queue_name == self.source_queues[2]:
                cleaned = self.clean_credit(data)

            if cleaned:
                logger.info(f"Publishing cleaned data to {self.target_queues[queue_name]}")
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.target_queues[queue_name],
                    body=json.dumps(cleaned)
                )
            else:
                logger.info(f"Skipped invalid data from {queue_name}")
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
                auto_ack=True
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