import json
import pika
import os
import threading
from configparser import ConfigParser
from transformers import pipeline
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict

from common.logger import get_logger
logger = get_logger("SentimentAnalyzer")

from common.mom import RabbitMQProcessor
from common.client_state_manager import ClientState, ClientManager

EOS_TYPE = "EOS"
SECONDS_TO_HEARTBEAT = 600
MAX_WORKERS = os.cpu_count() or 4

# Global variable for model in each process
sentiment_pipe = None

def init_sentiment_model():
    global sentiment_pipe
    sentiment_pipe = pipeline("sentiment-analysis")

def analyze_sentiment_process(text: str) -> str:
    if not text or not text.strip():
        return "neutral"
    try:
        pipe = pipeline("sentiment-analysis")
        result = pipe(text, truncation=True)[0]
        label = result["label"].lower()
        logger.debug(f"Sentiment analysis result: {label} for text: {text[:50]}...")
        return label if label in {"positive", "negative"} else "neutral"
    except Exception:
        return "neutral"

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
            self.config["QUEUES"]["negative_movies_queue"]
        ]
        self.batch_size = int(self.config["DEFAULT"].get("batch_size", 200))

        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queue,
            target_queues=self.target_queues
        )
        
        self.client_manager = ClientManager(
            expected_queues=self.source_queue,
            nodes_to_await=self.eos_to_await,
        )

        self.executor = ProcessPoolExecutor(
            max_workers=MAX_WORKERS,
            initializer=init_sentiment_model
        )

    def _mark_eos_received(self, body, channel, headers, client_state: ClientState):
        try:
            data = json.loads(body)
            node_id = data.get("node_id")
            count = data.get("count", 0)
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return      
        if not client_state.has_queue_received_eos_from_node(self.source_queue, node_id):
            count += 1
            client_state.mark_eos(self.source_queue, node_id)
            logger.debug(f"EOS received for node {node_id}.")

        logger.debug(f"EOS count for node {node_id}: {count}")
        # If this isn't the last node, send the EOS message back to the source queue
        if count < self.nodes_of_type:
            # Send EOS back to the source queue for other sentiment analyzers
            self.rabbitmq_processor.publish(
                target=self.source_queue,
                message={"node_id": node_id, "count": count},
                msg_type=EOS_TYPE,
                headers=headers
            )
        
    def _send_eos(self, msg_type, headers, client_state: ClientState):
        if client_state.has_received_all_eos(self.source_queue):
            logger.info("All nodes have sent EOS. Sending EOS to both queues.")
            for queue in self.target_queues:
                self.rabbitmq_processor.publish(
                    target=queue,
                    message={"node_id": self.node_id},
                    msg_type=msg_type,
                    headers=headers,
                )
            logger.debug("Sent EOS message to both queues.")

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id, request_number = headers.get("client_id"), headers.get("request_number")

            if not client_id or not request_number:
                logger.error("Missing client_id or request_number in headers")
                self.rabbitmq_processor.acknowledge(method)
                return
        
            client_state = self.client_manager.add_client(client_id, request_number)
            key = (client_id, request_number)
            if msg_type == EOS_TYPE:
                self._mark_eos_received(body, ch, headers, client_state)
                with self.lock:
                    if len(self.batch_positive[key]) > 0:
                        self.rabbitmq_processor.publish(
                            target=self.target_queues[0],
                            message=self.batch_positive[key],
                            headers=headers,
                        )
                        logger.info(f"Sent {len(self.batch_positive[key])} positive movies to {self.target_queues[0]}")
                        self.batch_positive[key] = []

                    if len(self.batch_negative[key]) > 0:
                        self.rabbitmq_processor.publish(
                            target=self.target_queues[1],
                            message=self.batch_negative[key],
                            headers=headers,
                        )
                        logger.info(f"Sent {len(self.batch_negative[key])} negative movies to {self.target_queues[1]}")
                        self.batch_negative[key] = []

                self._send_eos(msg_type, headers, client_state)
                return


            movies_batch = json.loads(body)
            future_to_movie = {
                self.executor.submit(analyze_sentiment_process, movie.get("overview")): movie
                for movie in movies_batch
            }
            for future in as_completed(future_to_movie):
                movie = future_to_movie[future]
                try:
                    sentiment = future.result()
                    logger.debug(f"[Worker] Analyzed '{movie.get('original_title')}' → {sentiment}")

                    if sentiment == "neutral":
                        continue
                    elif sentiment == "positive":
                        with self.lock:
                            self.batch_positive[key].append(movie)
                    elif sentiment == "negative":
                        with self.lock:
                            self.batch_negative[key].append(movie)

                except Exception as e:
                    logger.error(f"Error analyzing sentiment for movie '{movie.get('original_title')}': {e}")


            with self.lock:
                if len(self.batch_positive[key]) >= self.batch_size:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[0],
                        message=self.batch_positive[key],
                        headers=headers,
                    )
                    logger.info(f"Sent batch of {len(self.batch_positive[key])} positive movies.")
                    self.batch_positive[key] = []

                if len(self.batch_negative[key]) >= self.batch_size:
                    self.rabbitmq_processor.publish(
                        target=self.target_queues[1],
                        message=self.batch_negative[key],
                        headers=headers,
                    )
                    logger.info(f"Sent batch of {len(self.batch_negative[key])} negative movies.")
                    self.batch_negative[key] = []

        except pika.exceptions.StreamLostError as e:
            logger.error(f"Stream lost, reconnecting: {e}")
            self.rabbitmq_processor.reconnect_and_restart(self.callback)

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"AMQP connection lost, reconnecting: {e}")
            self.rabbitmq_processor.reconnect_and_restart(self.callback)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
        
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
        
        # Tell gateway that this node is online
        self._notify_gateway()

        try:
            logger.info("Starting message consumption...")
            self.rabbitmq_processor.consume(self.callback)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self.rabbitmq_processor.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection lost during consuming: {e}")
            self.rabbitmq_processor.reconnect_and_restart(self.callback)
        finally:
            logger.info("Closing RabbitMQ connection...")
            self.rabbitmq_processor.close()
            logger.info("Connection closed.")

    def _notify_gateway(self):
        self.rabbitmq_processor.channel.queue_declare(queue='nodes_ready', durable=False, arguments={'x-max-priority': 10})
        self.rabbitmq_processor.publish(
            target='nodes_ready',
            message=self.node_name
        )
        logger.info(f"Sent ready signal to 'nodes_ready' for {self.node_name}")

if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.process()
