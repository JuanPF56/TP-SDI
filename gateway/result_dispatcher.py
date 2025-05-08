import threading
import time
import json

from common.logger import get_logger
logger = get_logger("Result Dispatcher")

from client_registry import ClientRegistry
from common.mom import RabbitMQProcessor

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
            rabbitmq_host=self.rabbitmq_host
        )
        connected = self.broker.connect()
        if not connected:
            raise RuntimeError("Could not connect to RabbitMQ")

    def stop(self):
        self._stop_flag.set()
        self.broker.stop_consuming()
        self.broker.close()
        logger.info("Result Dispatcher stopped.")

    def run(self):
        logger.info(f"Result Dispatcher started.")
        try:
            base_cool_down = BASE_COOL_DOWN_TIME
            max_cool_down = MAX_COOL_DOWN_TIME
            cool_down_time = base_cool_down

            while not self._stop_flag.is_set():
                method, properties, body = self._get_next_result()
                if body:
                    try:
                        result_data = json.loads(body)
                        logger.debug(f"Received result: {result_data}")
                        """
                        Expected result format:
                        {
                            "client_id": "uuid-del-cliente",
                            "request_number": 1,
                            "query": "Q4",
                            "results": { ... }
                        }
                        """
                        client_id = result_data.get("client_id")
                        if not client_id:
                            logger.warning("Missing client_id in result.")
                            if method:
                                self.broker.acknowledge(method)
                            continue

                        client = self._clients_connected.get_by_uuid(client_id)
                        if client and client._client_is_connected():
                            client.send_result(result_data)
                            logger.debug(f"Dispatched result to client {client_id}")
                        else:
                            logger.warning(f"Client {client_id} not found or disconnected.")

                        # Reset the cool down time if a result is processed successfully
                        cool_down_time = base_cool_down

                    except Exception as e:
                        logger.error(f"Error processing result: {e}")
                        
                    finally:
                        if method:
                            self.broker.acknowledge(method)
                else:
                    logger.debug("No result to process, sleeping...")
                    time.sleep(cool_down_time)
                    # Increase the cool down time exponentially
                    cool_down_time = min(cool_down_time * 2, max_cool_down)

        except Exception as e:
            logger.error(f"Error in ResultDispatcher: {e}")

        finally:
            self.stop()

    def _get_next_result(self) -> tuple:
        # logger.info("Waiting for next result...")
        try:
            method, properties, body = self.broker.channel.basic_get(queue=self.results_queue, auto_ack=False)
            return method, properties, body

        except Exception as e:
            logger.error(f"Error getting result from queue: {e}")
            return None, None, None
            