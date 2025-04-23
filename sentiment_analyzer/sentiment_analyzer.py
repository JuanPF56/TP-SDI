import json
import pika
import time
import os
from configparser import ConfigParser
from transformers import pipeline
from common.logger import get_logger

EOS_TYPE = "EOS"
SECONDS_TO_HEARTBEAT = 600
logger = get_logger("SentimentAnalyzer")


class SentimentAnalyzer:
    def __init__(self, config_path: str = "config.ini"):
        self._load_config(config_path)
        self.sentiment_pipeline = pipeline("sentiment-analysis")
        self.batch_negative = []
        self.batch_positive = []
        self.connection = None
        self.channel = None
        self._connect_to_rabbitmq()

    def _load_config(self, path: str):
        config = ConfigParser()
        config.read(path)
        self.rabbitmq_host = config["RABBITMQ"]["host"]
        self.source_queue = config["QUEUES"]["movies_clean_queue"]
        self.positive_queue = config["QUEUES"]["positive_movies_queue"]
        self.negative_queue = config["QUEUES"]["negative_movies_queue"]
        self.batch_size = int(config["DEFAULT"].get("batch_size", 200))

    def _connect_to_rabbitmq(self):
        delay = 2
        while True:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.rabbitmq_host,
                        heartbeat=SECONDS_TO_HEARTBEAT
                    )
                )
                self.channel = self.connection.channel()
                self._declare_queues()
                logger.info(f"Process {os.getpid()} connected to RabbitMQ at {self.rabbitmq_host}")
                break

            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Process {os.getpid()} RabbitMQ connection error. Retrying in {delay} seconds...")

            except Exception as e:
                logger.warning(f"Unexpected connection error: {e}")

            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, 60)

    def _declare_queues(self):
        self.channel.queue_declare(queue=self.source_queue)
        self.channel.queue_declare(queue=self.positive_queue)
        self.channel.queue_declare(queue=self.negative_queue)

    def analyze_sentiment(self, text: str) -> str:
        if not text or not text.strip():
            logger.debug("Received empty or whitespace-only text for sentiment analysis.")
            return "neutral"
        try:
            result = self.sentiment_pipeline(text, truncation=True)[0]
            label = result["label"].lower()

            if label in {"positive", "negative"}:
                logger.debug(f"Sentiment analysis result: {label} for text: {text[:50]}...")
                return label
            else:
                logger.debug(f"Unexpected sentiment label '{label}' for text: {text[:50]}...")
                return "neutral"
        except Exception as e:
            logger.error(f"Error during sentiment analysis: {e}")
            return "neutral"

    def _mark_eos_received(self, msg_type):
        logger.info(f"Received EOS message of type '{msg_type}'.")
        self.channel.basic_publish(
            exchange='',
            routing_key=self.positive_queue,
            body=b'',
            properties=pika.BasicProperties(type=msg_type)
        )
        self.channel.basic_publish(
            exchange='',
            routing_key=self.negative_queue,
            body=b'',
            properties=pika.BasicProperties(type=msg_type)
        )
        logger.info("Sent EOS message to both queues.")

    def callback(self, ch, method, properties, body):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == EOS_TYPE:
                if len(self.batch_positive) > 0:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.positive_queue,
                        body=json.dumps(self.batch_positive)
                    )
                    logger.debug(f"Sent {len(self.batch_positive)} positive movies to {self.positive_queue}")
                    self.batch_positive = []
                if len(self.batch_negative) > 0:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.negative_queue,
                        body=json.dumps(self.batch_negative)
                    )
                    logger.debug(f"Sent {len(self.batch_negative)} negative movies to {self.negative_queue}")
                    self.batch_negative = []
                self._mark_eos_received(msg_type)
                return

            movies_batch = json.loads(body)
            for movie in movies_batch:
                sentiment = self.analyze_sentiment(movie.get("overview"))

                if sentiment == "neutral":
                    logger.debug(f"Ignoring neutral/empty overview for '{movie.get('original_title')}'")
                    continue

                if sentiment == "positive":
                    self.batch_positive.append(movie)
                elif sentiment == "negative":
                    self.batch_negative.append(movie)

            if len(self.batch_positive) >= self.batch_size:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.positive_queue,
                    body=json.dumps(self.batch_positive)
                )
                logger.debug(f"Sent batch of {len(self.batch_positive)} positive movies.")
                self.batch_positive = []

            if len(self.batch_negative) >= self.batch_size:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.negative_queue,
                    body=json.dumps(self.batch_negative)
                )
                logger.debug(f"Sent batch of {len(self.batch_negative)} negative movies.")
                self.batch_negative = []

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except pika.exceptions.StreamLostError as e:
            logger.error(f"Stream lost, reconnecting: {e}")
            self._reconnect_and_restart()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"AMQP connection lost, reconnecting: {e}")
            self._reconnect_and_restart()

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _reconnect_and_restart(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
        self._connect_to_rabbitmq()
        self.run()

    def run(self):
        logger.info("Waiting for clean movies...")
        self.channel.basic_consume(
            queue=self.source_queue,
            on_message_callback=self.callback,
            auto_ack=False
        )
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection lost during consuming: {e}")
            self._reconnect_and_restart()


if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.run()
