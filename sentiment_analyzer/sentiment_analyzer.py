# sentiment_analyzer.py

import json
import pika
from configparser import ConfigParser
from dataclasses import asdict
from transformers import pipeline
from common.logger import get_logger

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
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
        self.channel = connection.channel()
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

    def callback(self, ch, method, properties, body):
        pass
    def run(self):
        logger.info("Waiting for clean movies...")
        self.channel.basic_consume(queue=self.source_queue, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()


if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.run()
