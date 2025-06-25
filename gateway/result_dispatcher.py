import threading
import json
import logging
import os
import queue

from client_registry import ClientRegistry
from result_queue import ResultQueue
from protocol_gateway_proxy import ProtocolGatewayProxy

from common.mom import RabbitMQProcessor
from common.logger import get_logger

logger = get_logger("Result Dispatcher")
logger.setLevel(logging.INFO)

BASE_COOL_DOWN_TIME = 0.5  # seconds
MAX_COOL_DOWN_TIME = 60  # seconds
RESULT_PROCESSING_TIMEOUT = 30  # seconds

QUERYS_TO_ANSWER = 5


class ResultDispatcher(threading.Thread):
    def __init__(
        self,
        config,
        clients_connected: ClientRegistry,
        gateway_protocol: ProtocolGatewayProxy,
    ):
        # Initialize the thread
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        self.config = config
        self.results_queue = self.config["DEFAULT"]["results_queue"]
        self.rabbitmq_host = self.config["DEFAULT"]["rabbitmq_host"]
        self.node_name = os.getenv("NODE_NAME", "unknown")

        self._clients_connected = clients_connected
        self.gateway_protocol = gateway_protocol

        # Initialize persistent result queue
        self._result_queue = ResultQueue(f"/tmp/gateway_{self.node_name}_results")

        # Start result sender thread
        self._result_sender_thread = None
        self._start_result_sender()

        self.broker = RabbitMQProcessor(
            config=config,
            source_queues=self.results_queue,
            target_queues=[],  # Not publishing in this component
            rabbitmq_host=self.rabbitmq_host,
        )
        connected = self.broker.connect()
        if not connected:
            raise RuntimeError("Could not connect to RabbitMQ")

    def _start_result_sender(self):
        """Start the result sender thread that processes queued results"""
        self._result_sender_thread = threading.Thread(
            target=self._process_result_queue, name="ResultSender", daemon=True
        )
        self._result_sender_thread.start()
        logger.info("Result sender thread started")

    def _process_result_queue(self):
        """Process results from the persistent queue and send them to clients"""
        logger.info("Result sender started")

        while not self._stop_flag.is_set():
            try:
                # Get result from queue with timeout
                result_id, result_entry = self._result_queue.get(timeout=1.0)

                client_id = result_entry["client_id"]
                result_data = result_entry["result_data"]

                logger.debug(
                    "Processing queued result %s for client %s", result_id, client_id
                )

                # Try to send the result
                success = self.send_result(client_id, result_data)

                if success:
                    # Mark as done and remove from disk
                    self._result_queue.task_done(result_id)
                    logger.debug(
                        "Successfully sent result %s to client %s", result_id, client_id
                    )
                else:
                    # Put it back in the queue for retry (at the end)
                    logger.warning(
                        "Failed to send result %s to client %s, will retry",
                        result_id,
                        client_id,
                    )
                    # Mark as done to avoid infinite loop
                    self._result_queue.task_done(result_id)

            except queue.Empty:
                # Timeout waiting for result, continue loop
                continue
            except Exception as e:
                logger.error("Error in result sender: %s", e)
                continue

        logger.info("Result sender stopped")

    def send_result(self, client_id: str, result_data: dict) -> bool:
        try:
            self.gateway_protocol.send_result(result_data)
            client = self._clients_connected.get_by_uuid(client_id)
            client.add_sent_answer()
            return True
        except Exception as e:
            logger.error("Error sending result: %s", e)
            return False

    def _handle_message(self, channel, method, properties, body, queue_name=None):
        """Handle incoming result messages from RabbitMQ"""
        try:
            result_data = json.loads(body)
            client_id = result_data.get("client_id")
            client = self._clients_connected.get_by_uuid(client_id)
            if not client:
                logger.debug("Client %s not found, ignoring result", client_id)
                # No ack para que lo reintente otro consumidor
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            # Queue the result for processing instead of sending immediately
            try:
                result_id = self._result_queue.put(client_id, result_data)
                logger.debug("Queued result %s for client %s", result_id, client_id)

                # ACK the message since we've successfully queued it
                channel.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                logger.error("Error queuing result for client %s: %s", client_id, e)
                # Don't ACK if we couldn't queue it
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except Exception as e:
            logger.error("Error processing result message: %s", e)

    def stop(self):
        """
        Stop the Result Dispatcher thread and clean up resources.
        """
        logger.info("Stopping Result Dispatcher...")

        # Stop the main thread
        self._stop_flag.set()

        # Wait for result sender thread to finish
        if self._result_sender_thread and self._result_sender_thread.is_alive():
            logger.info("Waiting for result sender to finish...")
            self._result_sender_thread.join(timeout=RESULT_PROCESSING_TIMEOUT)
            if self._result_sender_thread.is_alive():
                logger.warning("Result sender did not finish within timeout")

        # Stop RabbitMQ consumer
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

        logger.info("Result Dispatcher main loop finished.")

    def get_queued_results_count(self) -> int:
        """Get the number of results currently queued"""
        return self._result_queue.qsize()
