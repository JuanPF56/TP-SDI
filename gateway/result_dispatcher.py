import threading
import time
import json
import pika

from common.logger import get_logger
logger = get_logger("Result Dispatcher")

from client_registry import ClientRegistry

COOL_DOWN_TIME = 0.5  # seconds

QUERYS_TO_ANSWER = 5

class ResultDispatcher(threading.Thread):
    def __init__(self, rabbitmq_host, results_queue, clients_connected: ClientRegistry):
        # Initialize the thread
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        self._clients_connected = clients_connected

        # RabbitMQ connection parameters
        self.rabbitmq_host = rabbitmq_host
        self.channel = None
        self.connection = None
        self.results_queue = results_queue
        self._clients_connected = clients_connected

    def stop(self):
        self._stop_flag.set()
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("Result Dispatcher stopped.")

    def run(self):
        logger.info(f"Result Dispatcher started.")
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.results_queue)

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
                                self._ack(method)
                            continue

                        client = self._clients_connected.get_by_uuid(client_id)
                        if client and client._client_is_connected():
                            client.send_result(result_data)
                            logger.debug(f"Dispatched result to client {client_id}")
                        else:
                            logger.warning(f"Client {client_id} not found or disconnected.")

                    except Exception as e:
                        logger.error(f"Error processing result: {e}")
                    finally:
                        if method:
                            self._ack(method)
                else:
                    time.sleep(COOL_DOWN_TIME)

        except Exception as e:
            logger.error(f"Error in ResultDispatcher: {e}")

        finally:
            self.stop()

    def _get_next_result(self):
        return self.channel.basic_get(queue=self.results_queue, auto_ack=False)

    def _ack(self, method):
        if method:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)