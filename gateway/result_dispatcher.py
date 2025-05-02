import threading
import time
import json
import pika

from common.logger import get_logger
logger = get_logger("Result Dispatcher")

COOL_DOWN_TIME = 0.5  # seconds

QUERYS_TO_ANSWER = 5

class ResultDispatcher(threading.Thread):
    def __init__(self, rabbitmq_host, results_queue):
        # Initialize the thread
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        # RabbitMQ connection parameters
        self.rabbitmq_host = rabbitmq_host
        self.channel = None
        self.connection = None
        self.results_queue = results_queue

    def stop(self):
        self._stop_flag.set()
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()

    def run(self):
        logger.info("Result Dispatcher started.")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.results_queue)

        while not self._stop_flag.is_set():
            method, properties, body = self._get_next_result()
            if body:
                try:
                    result_data = json.loads(body)
                    logger.debug(f"Received result: {result_data}")
                    # TODO: Aca hay que ver con que formato se lo pasamos a los senders para clientes, y como viene de la cola de results
                    # En principio necesito el UUID para buscarlo, despeus el request_number para saber de que consulta es, y por ultimo {query_id, result}
                    """
                    for client in self.connected_clients:
                        if client._client_is_connected():
                            client.send_result(result_data)
                            self._results_send += 1
                    if self._results_send >= self._results_expected:
                        logger.info(f"Sent {self._results_send} results to clients.")
                        self._stop_flag.set()  # Stop after sending expected results
                    """  
                except Exception as e:
                    logger.error(f"Error processing result: {e}")
                finally:
                    if method:
                        self.channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                time.sleep(COOL_DOWN_TIME)

    def _get_next_result(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.results_queue, auto_ack=False)
        if method_frame:
            return method_frame, header_frame, body
        return None, None, None

