import json
import pika
import time
from configparser import ConfigParser
from dataclasses import asdict
from transformers import pipeline
from common.logger import get_logger
EOS_TYPE = "EOS" 

logger = get_logger("SentimentAnalyzer")


class SentimentAnalyzer:
    def __init__(self, config_path: str = "config.ini"):
        self._load_config(config_path)
        self.sentiment_pipeline = pipeline("sentiment-analysis")
        self._connect_to_rabbitmq()

    def _load_config(self, path: str):
        config = ConfigParser()
        config.read(path)
        self.rabbitmq_host = config["RABBITMQ"]["host"]
        self.source_queue = config["QUEUES"]["movies_clean_queue"]
        self.positive_queue = config["QUEUES"]["positive_movies_queue"]
        self.negative_queue = config["QUEUES"]["negative_movies_queue"]

    def _connect_to_rabbitmq(self):
        for i in range(10):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
                self.channel = connection.channel()
                self.channel.queue_declare(queue=self.source_queue)
                self.channel.queue_declare(queue=self.positive_queue)
                self.channel.queue_declare(queue=self.negative_queue)
                logger.info("Connected to RabbitMQ successfully.")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Connection to RabbitMQ failed ({i+1}/10): {e}")
                time.sleep(5)
        logger.error("Failed to connect to RabbitMQ after multiple attempts.")
        raise pika.exceptions.AMQPConnectionError

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
                self._mark_eos_received(msg_type)
                return

            movies_batch = json.loads(body)
            positive_movies = []
            negative_movies = []

            for movie in movies_batch:
                sentiment = self.analyze_sentiment(movie.get("overview"))

                if sentiment == "neutral":
                    logger.debug(f"Ignoring neutral/empty overview for '{movie.get('original_title')}'")
                    continue

                if sentiment == "positive":
                    positive_movies.append(movie)
                elif sentiment == "negative":
                    negative_movies.append(movie)

            if positive_movies:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.positive_queue,
                    body=json.dumps(positive_movies)
                )
                logger.debug(f"Sent {len(positive_movies)} positive movies to {self.positive_queue}")

            if negative_movies:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.negative_queue,
                    body=json.dumps(negative_movies)
                )
    

            self.channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        logger.info("Waiting for clean movies...")
        self.channel.basic_consume(queue=self.source_queue, on_message_callback=self.callback)
        self.channel.start_consuming()


if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.run()
