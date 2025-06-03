import json
import signal
import pika
import os
from configparser import ConfigParser
from textblob import TextBlob
from collections import defaultdict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.eos_handling import handle_eos
from common.logger import get_logger

logger = get_logger("SentimentAnalyzer")
from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientState
from common.client_state_manager import ClientManager

EOS_TYPE = "EOS"
SECONDS_TO_HEARTBEAT = 600
MAX_WORKERS = os.cpu_count() or 4


class SentimentAnalyzer:
    def __init__(self, config_path: str = "config.ini"):
        self.batch_negative = defaultdict(list)
        self.batch_positive = defaultdict(list)
        self.lock = threading.Lock()
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.node_name = os.getenv("NODE_NAME", "unknown")

        self.config = ConfigParser()
        self.config.read(config_path)
        self.source_queue = self.config["QUEUES"]["movies_clean_queue"]
        self.target_queues = [
            self.config["QUEUES"]["positive_movies_queue"],
            self.config["QUEUES"]["negative_movies_queue"],
        ]
        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queues,
        )

        self.client_manager = ClientManager(
            expected_queues=self.source_queue,
            nodes_to_await=self.eos_to_await,
        )

        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            if self.rabbitmq_processor:
                logger.info("Stopping message consumption...")
                self.rabbitmq_processor.stop_consuming()
                logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.close()
        except Exception as e:
            logger.error("Error closing connection: %s", e)

    def analyze_sentiment(self, text: str) -> str:
        if not text or not text.strip():
            logger.debug(
                "Received empty or whitespace-only text for sentiment analysis."
            )
            return "neutral"
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            logger.debug(
                "Text sentiment polarity: %s for text: %s...", polarity, text[:50]
            )

            if polarity > 0.1:
                return "positive"
            elif polarity < -0.1:
                return "negative"
            else:
                return "neutral"
        except Exception as e:
            logger.error("Error during sentiment analysis: %s", e)
            return "neutral"

    def _handle_eos(
        self, input_queue, body, method, headers, client_state: ClientState
    ):
        if client_state:
            key = client_state.client_id
            with self.lock:
                if len(self.batch_positive[key]) > 0:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[0],
                        message=self.batch_positive[key],
                        headers=headers,
                    )
                    logger.debug(
                        "Sent %d positive movies to %s",
                        len(self.batch_positive[key]),
                        self.target_queues[0],
                    )
                    self.batch_positive[key] = []

                if len(self.batch_negative[key]) > 0:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[1],
                        message=self.batch_negative[key],
                        headers=headers,
                    )
                    logger.debug(
                        "Sent %d negative movies to %s",
                        len(self.batch_negative[key]),
                        self.target_queues[1],
                    )
                    self.batch_negative[key] = []
        handle_eos(
            body,
            self.node_id,
            input_queue,
            self.source_queue,
            headers,
            self.nodes_of_type,
            self.rabbitmq_processor,
            client_state,
            target_queues=self.target_queues,
        )
        self._free_resources(client_state)

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.source_queue):
            self.client_manager.remove_client(client_state.client_id)

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id")

            if not client_id:
                logger.error("Missing client_id in headers")
                self.rabbitmq_processor.acknowledge(method)
                return

            key = client_id
            client_state = self.client_manager.add_client(
                client_id, msg_type == EOS_TYPE
            )

            if msg_type == EOS_TYPE:
                self._handle_eos(input_queue, body, method, headers, client_state)
                return

            movies_batch = json.loads(body)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_movie = {
                    executor.submit(
                        self.analyze_sentiment, movie.get("overview")
                    ): movie
                    for movie in movies_batch
                }

                for future in as_completed(future_to_movie):
                    movie = future_to_movie[future]
                    try:
                        sentiment = future.result()
                        logger.debug(
                            "[Worker] Analyzed '%s' → %s",
                            movie.get("original_title"),
                            sentiment,
                        )

                        if sentiment == "neutral":
                            logger.debug(
                                "[Worker] Movie '%s' is neutral, skipping.",
                                movie.get("original_title"),
                            )
                            continue
                        elif sentiment == "positive":
                            logger.debug(
                                "[Worker] Movie '%s' is positive.",
                                movie.get("original_title"),
                            )
                            with self.lock:
                                self.batch_positive[key].append(movie)
                        elif sentiment == "negative":
                            logger.debug(
                                "[Worker] Movie '%s' is negative.",
                                movie.get("original_title"),
                            )
                            with self.lock:
                                self.batch_negative[key].append(movie)

                    except Exception as e:
                        logger.error(
                            "[Worker] Error analyzing sentiment for movie '%s': %s",
                            movie.get("original_title"),
                            e,
                        )
                        logger.error(
                            "[Worker] Error analyzing sentiment for movie '%s': %s",
                            movie.get("original_title"),
                            e,
                        )

            with self.lock:
                if len(self.batch_positive[key]) >= self.batch_size:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[0],
                        message=self.batch_positive[key],
                        headers=headers,
                    )
                    logger.debug(
                        "Sent batch of %d positive movies to %s",
                        len(self.batch_positive[key]),
                        self.target_queues[0],
                    )
                    self.batch_positive[key] = []

                if len(self.batch_negative[key]) >= self.batch_size:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[1],
                        message=self.batch_negative[key],
                        headers=headers,
                    )
                    logger.debug(
                        "Sent batch of %d negative movies to %s",
                        len(self.batch_negative[key]),
                        self.target_queues[1],
                    )
                    self.batch_negative[key] = []

        except pika.exceptions.StreamLostError as e:
            logger.error("Stream lost, reconnecting: %s", e)
            self.rabbitmq_processor.reconnect_and_restart(self.callback)

        except pika.exceptions.AMQPConnectionError as e:
            logger.error("AMQP connection lost, reconnecting: %s", e)
            self.rabbitmq_processor.reconnect_and_restart(self.callback)

        except Exception as e:
            logger.error("Error processing message: %s", e)

        finally:
            self.rabbitmq_processor.acknowledge(method)

    def process(self):
        """
        Reads from the input queue (movies_arg_spain_2000s_queue).

        Collects:
        - title
        - genres (list of names)

        Publishes the results after processing a batch.
        """
        logger.info("Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("Connection lost during consuming: %s", e)
            self.rabbitmq_processor.reconnect_and_restart(self.callback)
        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")


if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.process()
