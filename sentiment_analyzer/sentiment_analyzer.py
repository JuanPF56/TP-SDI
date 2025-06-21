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
from common.duplicate_handler import DuplicateHandler
from common.leader_election import LeaderElector

from common.election_logic import election_logic, recover_node
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
        self.first_run = True

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

        self.manager = multiprocessing.Manager()
        self.master_logic_started_event = self.manager.Event()
        self.lock = self.manager.Lock()

        self.client_manager = ClientManager(
            expected_queues=self.source_queue,
            manager=self.manager,
            lock=self.lock,
            nodes_to_await=self.eos_to_await,
        )
        
        self.master_logic = MasterLogic(
            config=self.config,
            manager=self.manager,
            node_id=self.node_id,
            nodes_of_type=self.nodes_of_type,
            clean_queues=self.clean_batch_queue,
            client_manager=self.client_manager,
            started_event=self.master_logic_started_event
        )
        
        self.duplicate_handler = DuplicateHandler()

        self.election_port = int(os.getenv("ELECTION_PORT", 9001))
        self.peers = os.getenv("PEERS", "")  # del estilo: "filter_cleanup_1:9001,filter_cleanup_2:9002"
        self.node_name = os.getenv("NODE_NAME")
        self.elector = LeaderElector(self.node_id, self.peers, self.election_port, self._election_logic)

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
    
    def _election_logic(self, leader_id):
        election_logic(
            self,
            leader_id=leader_id,
            leader_queues=self.clean_batch_queue,
        )

    def read_storage(self):
        self.client_manager.read_storage()
        self.client_manager.check_all_eos_received(
            self.config, self.node_id, self.clean_batch_queue, self.target_queues
        )

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
        queue = input_queue.split("_node_")[0]
        handle_eos(
            body,
            self.node_id,
            queue,
            queue,
            headers,
            self.rabbitmq_processor,
            client_state,
            self.master_logic.is_leader(),
            target_queues=self.target_queues,
        )

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.source_queue):
            self.client_manager.remove_client(client_state.client_id)

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id")
            message_id = headers.get("message_id")

            if client_id is None:
                logger.error("Missing client_id in headers")
                return

            key = client_id
            client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

            if msg_type == EOS_TYPE:
                self._handle_eos(input_queue, body, method, headers, client_state)
                return
            
            if message_id is None:
                logger.error("Missing message_id in headers")
                return
            
            if self.duplicate_handler.is_duplicate(client_id, input_queue, message_id):
                logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
                return

            movies = json.loads(body)

            # Normalize to list
            if isinstance(movies, dict):
                movies = [movies]
            elif not isinstance(movies, list):
                logger.warning("❌ Unexpected movie format: %s, skipping.", type(movies))
                return

            sentiment_batches = {
                "positive": [],
                "negative": [],
            }

            for movie in movies:
                try:
                    sentiment = self.analyze_sentiment(movie.get("overview", ""))
                    title = movie.get("original_title")
                    logger.debug("[Worker] Analyzed '%s' → %s", title, sentiment)

                    if sentiment == "neutral":
                        logger.debug("[Worker] Movie '%s' is neutral, skipping.", title)
                    elif sentiment in ("positive", "negative"):
                        sentiment_batches[sentiment].append(movie)
                    else:
                        logger.warning("[Worker] Unknown sentiment '%s' for movie '%s'", sentiment, title)

                except Exception as e:
                    logger.error("[Worker] Error analyzing sentiment for movie '%s': %s", movie.get("original_title"), e)

            for sentiment, movies_batch in sentiment_batches.items():
                if movies_batch:
                    target_queue = self.target_queues[0] if sentiment == "positive" else self.target_queues[1]
                    self.rabbitmq_processor.publish(
                        target=target_queue,
                        message=movies_batch,
                        headers=headers,
                    )

            self.duplicate_handler.add(client_id, input_queue, message_id)
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

            self.elector.start_election()
            recover_node(self, self.clean_batch_queue)
            
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
