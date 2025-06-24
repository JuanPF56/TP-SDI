import threading
import json
import logging

from client_registry import ClientRegistry
from result_message import ResultMessage

from common.mom import RabbitMQProcessor
from common.logger import get_logger

logger = get_logger("Result Dispatcher")
logger.setLevel(logging.INFO)

BASE_COOL_DOWN_TIME = 0.5  # seconds
MAX_COOL_DOWN_TIME = 60  # seconds

QUERYS_TO_ANSWER = 5


class ResultDispatcher(threading.Thread):
    def __init__(self, config, clients_connected: ClientRegistry):
        # Initialize the thread
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        self.config = config
        self.results_queue = self.config["DEFAULT"]["results_queue"]
        self.rabbitmq_host = self.config["DEFAULT"]["rabbitmq_host"]

        self._clients_connected = clients_connected
        self.broker = RabbitMQProcessor(
            config=config,
            source_queues=self.results_queue,
            target_queues=[],  # Not publishing in this component
            rabbitmq_host=self.rabbitmq_host,
        )
        connected = self.broker.connect()
        if not connected:
            raise RuntimeError("Could not connect to RabbitMQ")

    def _handle_message(self, channel, method, properties, body, queue_name=None):
        try:
            result_data = json.loads(body)
            client_id = result_data.get("client_id")

            if not client_id:
                logger.warning("Missing client_id in result.")
                # No ack para que lo reintente otro consumidor
                return

            client = self._clients_connected.get_by_uuid(client_id)
            if client:
                result_message = ResultMessage.from_json_with_casting(result_data)
                client.send_result(result_message)
                logger.info("Dispatched result to client %s", client_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.debug(
                    "Client %s not found or disconnected. Not ACKing.", client_id
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logger.error("Error processing result: %s", e)
            # En caso de error también no hacer ack para reintento

    def stop(self):
        """
        Stop the Result Dispatcher thread and clean up resources.
        """
        self._stop_flag.set()
        self.broker.stop_consuming_threadsafe()
        self.broker.close()
        logger.info("Result Dispatcher stopped.")

    def run(self):
        logger.info("Result Dispatcher started.")
        cool_down_time = BASE_COOL_DOWN_TIME

        while not self._stop_flag.is_set():
            try:
                if self._clients_connected.has_any():
                    logger.debug("Clients connected. Starting to consume results.")
                    self.broker.consume(self._handle_message)
                    # Si consume, se queda bloqueado en consume() hasta que se detenga,
                    # por lo tanto salimos del loop.
                    break
                else:
                    logger.debug("No clients connected. Waiting before rechecking.")
                    self._stop_flag.wait(timeout=cool_down_time)
                    cool_down_time = min(MAX_COOL_DOWN_TIME, cool_down_time * 2)
            except Exception as e:
                logger.error("Error in ResultDispatcher loop: %s", e)
                self._stop_flag.wait(timeout=cool_down_time)

        self.stop()

    def _get_next_result(self) -> tuple:
        # logger.info("Waiting for next result...")
        try:
            method, properties, body = self.broker.channel.basic_get(
                queue=self.results_queue, auto_ack=False
            )
            return method, properties, body

        except Exception as e:
            logger.error("Error getting result from queue: %s", e)
            return None, None, None
