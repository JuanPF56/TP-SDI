import configparser
import json
import pika
import time

from common.logger import get_logger
from common.filter_base import FilterBase

logger = get_logger("Filter-Cleanup")

class CleanupFilter(FilterBase):
    def __init__(self, config):
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
        try:
            logger.info(f"Attempting to connect to RabbitMQ at {self.rabbitmq_host}")
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


    def callback(self, ch, method, properties, body, queue_name):
        #tengo que comletar}
        pass

    def process(self):
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