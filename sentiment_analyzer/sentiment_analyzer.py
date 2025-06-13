import json
import multiprocessing
import signal
import pika
import os
from configparser import ConfigParser
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

from common.eos_handling import handle_eos
from common.logger import get_logger
from common.master import MasterLogic
from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientState, ClientManager

logger = get_logger("SentimentAnalyzer")

EOS_TYPE = "EOS"
SECONDS_TO_HEARTBEAT = 600
MAX_WORKERS = os.cpu_count() or 4


class SentimentAnalyzer:
    def __init__(self, config_path: str = "config.ini"):
        self.lock = threading.Lock()
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.node_name = os.getenv("NODE_NAME", "unknown")

        self.config = ConfigParser()
        self.config.read(config_path)
        self.clean_batch_queue = self.config["QUEUES"]["movies_clean_queue"]
        self.source_queue = self.clean_batch_queue + "_node_" + str(self.node_id)
        self.target_queues = [
            self.config["QUEUES"]["positive_movies_queue"],
            self.config["QUEUES"]["negative_movies_queue"],
        ]

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queues,
        )

        self.client_manager = ClientManager(
            expected_queues=self.source_queue,
            nodes_to_await=self.eos_to_await,
        )
        
        self.manager = multiprocessing.Manager()
        self.master_logic = MasterLogic(
            config=self.config,
            manager=self.manager,
            node_id=self.node_id,
            nodes_of_type=self.nodes_of_type,
            clean_queues=self.clean_batch_queue,
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
            os.kill(self.master_logic.pid, signal.SIGINT)
            self.master_logic.join()
            self.manager.shutdown()            
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
        # No batch clearing needed here, just handle eos and free client resources
        handle_eos(
            body,
            self.node_id,
            input_queue,
            self.source_queue,
            headers,
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
            client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

            if msg_type == EOS_TYPE:
                self._handle_eos(input_queue, body, method, headers, client_state)
                return

            movies = json.loads(body)

            # Normalize to list
            if isinstance(movies, dict):
                movies = [movies]
            elif not isinstance(movies, list):
                logger.warning("❌ Unexpected movie format: %s, skipping.", type(movies))
                self.rabbitmq_processor.acknowledge(method)
                return

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self.analyze_sentiment, movie.get("overview")): movie
                    for movie in movies
                }

                for future in futures:
                    movie = futures[future]
                    try:
                        sentiment = future.result()
                        title = movie.get("original_title")
                        logger.debug("[Worker] Analyzed '%s' → %s", title, sentiment)

                        if sentiment == "neutral":
                            logger.debug("[Worker] Movie '%s' is neutral, skipping.", title)
                        else:
                            target_queue = self.target_queues[0] if sentiment == "positive" else self.target_queues[1]
                            self.rabbitmq_processor.publish(
                                target=target_queue,
                                message=movie,  # still send as single object
                                headers=headers,
                            )
                            logger.debug("[Worker] Published movie '%s' to %s queue.", title, target_queue)

                    except Exception as e:
                        logger.error("[Worker] Error analyzing sentiment for movie '%s': %s", movie.get("original_title"), e)
                        
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

        Publishes the results immediately after processing each movie.
        """
        logger.info("Node is online")

        if not self.rabbitmq_processor.connect():
            logger.error("Error al conectar a RabbitMQ. Saliendo.")
            return

        try:
            # Start the master logic process
            self.master_logic.start()

            # TODO: Leader election logic
            # For now, we assume the node with the highest node_id is the leader
            if self.node_id == self.nodes_of_type:
                logger.info("This node is the leader. Starting master logic...")
                self.master_logic.toggle_leader()
            
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
