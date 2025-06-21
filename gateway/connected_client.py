"""
This module defines the ConnectedClient class, which represents a client connected to the protocol gateway.
"""

import socket
import threading
from dataclasses import asdict, is_dataclass

from protocol_gateway_client import ProtocolGateway
from batch_message import BatchMessage
from result_message import ResultMessage
from storage import (
    save_batch_to_disk,
    safe_delete_batch_file,
    load_batches_from_disk,
    load_results_from_disk,
    save_result_to_disk,
    delete_result_file,
)


from common.protocol import IS_LAST_BATCH_FLAG, TIPO_MENSAJE
from common.mom import RabbitMQProcessor
from common.logger import get_logger

logger = get_logger("ConnectedClient")


DATASETS_PER_REQUEST = 3
QUERYS_PER_REQUEST = 5


class ConnectedClient(threading.Thread):
    """
    Class representing a connected client.
    """

    def __init__(
        self,
        client_id: str,
        gateway_socket: socket.socket,
        config,
        shared_socket_lock: threading.Lock,
    ):
        """
        Initialize the ConnectedClient instance.

        :param protocol_gateway: The ProtocolGateway instance associated with this client.
        :param client_id: The unique identifier for this client.
        """
        super().__init__()
        self.config = config
        self._client_id = client_id
        self._gateway_socket = gateway_socket
        self._protocol_gateway = ProtocolGateway(
            gateway_socket, client_id, shared_socket_lock
        )
        self.was_closed = False

        self.store_limit = int(self.config["DEFAULT"].get("STORE_LIMIT", 1))
        self.accumulated_batches = 0
        self.batches_stored = []

        self.recovery_mode = self.config.getboolean(
            "DEFAULT", "RECOVERY_MODE", fallback=True
        )
        if self.recovery_mode:
            logger.info("Recovery mode is enabled for client %s", self._client_id)
            load_batches_from_disk(self._client_id)
        else:
            logger.info("Recovery mode is disabled for client %s", self._client_id)

        self._expected_datasets_to_receive_per_request = DATASETS_PER_REQUEST
        self._received_datasets = 0

        self._expected_answers_to_send_per_request = QUERYS_PER_REQUEST

        self._processed_datasets_from_request = 0

        self.sent_results = load_results_from_disk(self._client_id)
        self._sent_answers = len(self.sent_results)

        self._sent_answers_lock = threading.Lock()

        self.broker = None

        self._running = True
        self._stop_flag = threading.Event()

        self._condition = threading.Condition()

        self.broker = RabbitMQProcessor(
            config=self.config,
            source_queues=[],  # Connected client does not consume messages, so empty list
            target_queues=[
                self.config["DEFAULT"]["movies_raw_queue"],
                self.config["DEFAULT"]["credits_raw_queue"],
                self.config["DEFAULT"]["ratings_raw_queue"],
            ],
            rabbitmq_host=self.config["DEFAULT"]["rabbitmq_host"],
        )
        connected = self.broker.connect()
        if not connected:
            raise RuntimeError("Failed to connect to RabbitMQ.")

    def all_answers_sent(self) -> bool:
        """
        Check if all expected answers have been sent.

        :return: True if all expected answers have been sent, False otherwise.
        """
        with self._sent_answers_lock:
            all_sent = self._sent_answers == self._expected_answers_to_send_per_request
            logger.debug(
                "All answers sent: %s (Sent: %d, Expected: %d)",
                all_sent,
                self._sent_answers,
                self._expected_answers_to_send_per_request,
            )
            return all_sent

    def add_sent_answer(self):
        """
        Increment the number of sent answers.
        """
        with self._sent_answers_lock:
            self._sent_answers += 1
            logger.debug("Sent answers: %d", self._sent_answers)

            # Notify when all responses are sent
            if self._sent_answers == self._expected_answers_to_send_per_request:
                with self._condition:
                    logger.debug("All responses sent, notifying.")
                    self._condition.notify_all()

    def get_sent_answers(self):
        """
        Get the number of sent answers.
        """
        with self._sent_answers_lock:
            return self._sent_answers

    def get_client_id(self):
        """
        Get the unique identifier for this client.

        :return: The unique identifier for this client.
        """
        return self._client_id

    def _stop_client(self) -> None:
        """
        Close the client socket
        """
        if self.was_closed:
            return

        logger.info("Stopping client %s", self._client_id)
        self._stop_flag.set()
        self._running = False

        if self.broker:
            try:
                self.broker.close()
            except Exception as e:
                logger.warning("Error al cerrar broker: %s", e)

        self.was_closed = True

    def run(self):
        """
        Run the connected client thread.
        """
        try:
            while (
                self._running and not self._stop_flag.is_set() and not self.was_closed
            ):
                # Wait for all responses to be sent
                with self._condition:
                    while (
                        self._sent_answers != self._expected_answers_to_send_per_request
                    ):
                        logger.info("Waiting for all responses to be sent.")
                        self._condition.wait()  # Wait for notification

                # If all responses have been sent, close the client
                logger.info("All answers have been sent, closing client.")
                self._stop_client()

        except Exception as e:
            logger.error("Unexpected error in client %s: %s", self._client_id, e)
            return

    def process_batch(
        self, message_id, message_code, current_batch, is_last_batch, payload_len
    ) -> bool:
        try:
            payload = self._protocol_gateway.receive_payload(payload_len)
            if not payload or len(payload) != payload_len:
                logger.error("Failed to receive full payload")
                self._stop_client()
                return False

            logger.debug("Received payload of length %d", len(payload))

            processed_data = self._protocol_gateway.process_payload(
                message_code, payload
            )
            if processed_data is None:
                if message_code == TIPO_MENSAJE["BATCH_CREDITS"]:
                    # May be a partial batch
                    logger.warning(
                        "Received partial batch for credits, skipping processing."
                    )
                    return True
                else:
                    logger.error("Failed to process payload")
                    return False

            if self.recovery_mode:
                new_batch = BatchMessage(
                    message_id=message_id,
                    message_code=message_code,
                    client_id=self._client_id,
                    current_batch=current_batch,
                    is_last_batch=is_last_batch,
                    processed_data=processed_data,
                )
                self.batches_stored.append(new_batch)
                save_batch_to_disk(new_batch)
                self.accumulated_batches += 1

                if (
                    self.accumulated_batches >= self.store_limit
                    or is_last_batch == IS_LAST_BATCH_FLAG
                ):
                    logger.debug(
                        "Accumulated %d batches, publishing to queue",
                        self.accumulated_batches,
                    )
                    for i, batch in enumerate(self.batches_stored):
                        self._publish_batch(batch)
                    self.batches_stored = []
                    self.accumulated_batches = 0

            else:
                self._publish_message(
                    message_id,
                    message_code,
                    self._client_id,
                    current_batch,
                    is_last_batch,
                    processed_data,
                )

            return True

        except (socket.error, socket.timeout) as e:
            logger.error("Socket error raised: %s, from client %s", e, self._client_id)
            self._stop_client()
            return False

        except Exception as e:
            logger.error(
                "Unexpected error in _processs_request: %s, from client %s",
                e,
                self._client_id,
            )
            self._stop_client()
            return False

    def _publish_batch(self, batch_message: BatchMessage):
        try:
            queue_key = self._get_queue_key(batch_message.message_code)
            message_type = self._get_message_type(batch_message.message_code)

            if queue_key:
                headers = {
                    "client_id": batch_message.client_id,
                    "message_id": batch_message.message_id,
                }
                for i, item in enumerate(batch_message.processed_data):
                    if not is_dataclass(item):
                        logger.warning(
                            "Item #%d in processed_data is not a dataclass. Type: %s, Value: %s",
                            i,
                            type(item),
                            item,
                        )
                batch_payload = [
                    asdict(item) if is_dataclass(item) else item
                    for item in batch_message.processed_data
                ]
                success = self.broker.publish(
                    target=queue_key,
                    message=batch_payload,
                    msg_type=message_type,
                    headers=headers,
                )

                if success:
                    safe_delete_batch_file(batch_message)

                if batch_message.is_last_batch == IS_LAST_BATCH_FLAG:
                    success = self.broker.publish(
                        target=queue_key,
                        message={},  # Empty message to indicate end of batch
                        msg_type="EOS",
                        headers=headers,
                        priority=1,
                    )
                    self._processed_datasets_from_request += 1

                if not success:
                    logger.error("Failed to publish batch to queue %s", queue_key)
                    raise Exception("Failed to publish batch to queue")

        except (TypeError, ValueError) as e:
            logger.error("Error serializing data to JSON: %s", e)
            logger.error(batch_message.processed_data)

        except Exception as e:
            logger.error(
                "Unexpected error in _publish_batch: %s, from client %s",
                e,
                self._client_id,
            )
            self._stop_client()
            return

    def _publish_message(
        self,
        message_id,
        message_code,
        client_id,
        current_batch,
        is_last_batch,
        processed_data,
    ):
        try:
            queue_key = self._get_queue_key(message_code)

            if queue_key:
                headers = {"client_id": client_id, "message_id": message_id}
                batch_payload = [asdict(item) for item in processed_data]
                success = self.broker.publish(
                    target=queue_key,
                    message=batch_payload,
                    msg_type=message_code,
                    headers=headers,
                )

                if is_last_batch == IS_LAST_BATCH_FLAG:
                    success = self.broker.publish(
                        target=queue_key,
                        message={},  # Empty message to indicate end of batch
                        msg_type="EOS",
                        headers=headers,
                        priority=1,
                    )
                    self._processed_datasets_from_request += 1

                if not success:
                    logger.error("Failed to publish message to queue %s", queue_key)
                    raise Exception("Failed to publish message to queue")

        except (TypeError, ValueError) as e:
            logger.error("Error serializing data to JSON: %s", e)
            logger.error(processed_data)

        except Exception as e:
            logger.error(
                "Unexpected error in _publish_message: %s, from client %s",
                e,
                self._client_id,
            )
            self._stop_client()
            return

    def _get_queue_key(self, message_code):
        if message_code == TIPO_MENSAJE["BATCH_MOVIES"]:
            return self.config["DEFAULT"]["movies_raw_queue"]
        elif message_code == TIPO_MENSAJE["BATCH_CREDITS"]:
            return self.config["DEFAULT"]["credits_raw_queue"]
        elif message_code == TIPO_MENSAJE["BATCH_RATINGS"]:
            return self.config["DEFAULT"]["ratings_raw_queue"]
        return None

    def _get_message_type(self, message_code) -> str | None:
        if message_code == TIPO_MENSAJE["BATCH_MOVIES"]:
            return "BATCH_MOVIES"
        elif message_code == TIPO_MENSAJE["BATCH_CREDITS"]:
            return "BATCH_CREDITS"
        elif message_code == TIPO_MENSAJE["BATCH_RATINGS"]:
            return "BATCH_RATINGS"
        return None

    def send_result(self, result_message: ResultMessage):
        """
        Send the result data to the client.
        """
        try:
            logger.info(
                "Sending result to client %s: %s", self._client_id, result_message
            )

            # Guardar en disco antes de intentar enviar
            save_result_to_disk(result_message, self._client_id)

            self._protocol_gateway.send_result(result_message.to_dict())

            self.add_sent_answer()

            # Si se envió bien, eliminar archivo
            delete_result_file(self._client_id, result_message.query)

        except Exception as e:
            logger.error("Error sending result to client %s: %s", self._client_id, e)
            self._stop_client()

    def send_all_stored_results(self):
        """
        Resend all stored results to the client.
        This is useful for recovery mode to ensure all results are sent.
        """
        logger.info("Resending all stored results for client %s", self._client_id)
        for saved_result in self.sent_results:
            try:
                self._protocol_gateway.send_result(saved_result.to_dict())
                self.add_sent_answer()
                delete_result_file(self._client_id, saved_result.query)
            except Exception as e:
                logger.error("Error resending saved result: %s", e)
                break
