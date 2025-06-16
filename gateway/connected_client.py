"""
This module defines the ConnectedClient class, which represents a client connected to the protocol gateway.
"""

import os
import json
import socket
import threading
from dataclasses import asdict, is_dataclass

from protocol_gateway_client import ProtocolGateway
from batch_message import BatchMessage

from common.protocol import TIPO_MENSAJE, IS_LAST_BATCH_FLAG
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
        self, client_id: str, client_socket: socket.socket, client_addr, config
    ):
        """
        Initialize the ConnectedClient instance.

        :param protocol_gateway: The ProtocolGateway instance associated with this client.
        :param client_id: The unique identifier for this client.
        """
        super().__init__()
        self.config = config
        self._client_id = client_id
        self._client_socket = client_socket
        self._client_addr = client_addr
        self._protocol_gateway = ProtocolGateway(client_socket, client_id)
        self.was_closed = False

        self.store_limit = int(self.config["DEFAULT"].get("STORE_LIMIT", 1))
        self.accumulated_batches = 0
        self.batches_stored = []

        self._expected_datasets_to_receive_per_request = DATASETS_PER_REQUEST
        self._received_datasets = 0

        self._expected_answers_to_send_per_request = QUERYS_PER_REQUEST

        self._processed_datasets_from_request = 0

        self._sent_answers = 0
        self._sent_answers_lock = threading.Lock()

        self.broker = None

        self._running = True
        self._stop_flag = threading.Event()

        self._condition = threading.Condition()

        self._load_batches_from_disk()

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

    def client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._protocol_gateway.client_is_connected()

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

        try:
            self._protocol_gateway.stop_client()
        except Exception as e:
            logger.warning("Error al detener protocolo gateway: %s", e)

        self.was_closed = True

    def run(self):
        """
        Run the connected client thread.
        """
        logger.info("Connected client %s started.", self._client_id)
        try:
            self._protocol_gateway.send_client_id(self._client_id)

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

            while (
                self._running and not self._stop_flag.is_set() and not self.was_closed
            ):
                while (
                    self._processed_datasets_from_request
                    < self._expected_datasets_to_receive_per_request
                ):
                    if self._process_request() is False:
                        logger.error("Failed to process request")
                        self._protocol_gateway.stop_client()
                        return

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

        except OSError as e:
            if not self._protocol_gateway.client_is_connected():
                logger.info("Client %s disconnected: %s", self._client_id, e)
                return

        except Exception as e:
            logger.error("Unexpected error in client %s: %s", self._client_id, e)
            self._protocol_gateway.stop_client()
            return

    def _process_request(self) -> bool:
        try:
            header = self._protocol_gateway.receive_header()
            if header is None:
                logger.error("Header is None")
                self._protocol_gateway.stop_client()
                return False

            (
                message_code,
                encoded_id,
                current_batch,
                is_last_batch,
                payload_len,
            ) = header
            logger.debug("Message code: %s", message_code)

            if message_code not in TIPO_MENSAJE:
                logger.error("Invalid message code: %s", message_code)
                self._protocol_gateway.stop_client()
                return False

            if message_code == "DISCONNECT":
                logger.info("Client requested disconnection.")
                self._protocol_gateway.stop_client()
                return True

            client_id = encoded_id.decode("utf-8")
            logger.debug(
                "client %s - %s - Receiving batch %s",
                client_id,
                message_code,
                current_batch,
            )

            payload = self._protocol_gateway.receive_payload(payload_len)
            if not payload or len(payload) != payload_len:
                logger.error("Failed to receive full payload")
                self._protocol_gateway.stop_client()
                return False

            logger.debug("Received payload of length %d", len(payload))

            processed_data = self._protocol_gateway.process_payload(
                message_code, payload
            )
            if processed_data is None:
                if message_code == "BATCH_CREDITS":
                    # May be a partial batch
                    logger.warning(
                        "Received partial batch for credits, skipping processing."
                    )
                    return True
                else:
                    logger.error("Failed to process payload")
                    return False

            new_batch = BatchMessage(
                message_code=message_code,
                client_id=client_id,
                current_batch=current_batch,
                is_last_batch=is_last_batch,
                processed_data=processed_data,
            )
            self.batches_stored.append(new_batch)
            self._save_batch_to_disk(new_batch)
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

            return True

        except (socket.error, socket.timeout) as e:
            logger.error("Socket error raised: %s, from client %s", e, self._client_id)
            self._protocol_gateway.stop_client()
            return False

        except Exception as e:
            logger.error(
                "Unexpected error in _processs_request: %s, from client %s",
                e,
                self._client_id,
            )
            self._protocol_gateway.stop_client()
            return False

    def _publish_batch(self, batch_message: BatchMessage):
        try:
            queue_key = self._get_queue_key(batch_message.message_code)

            if queue_key:
                headers = {
                    "client_id": batch_message.client_id,
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
                    msg_type=batch_message.message_code,
                    headers=headers,
                )

                if success:
                    try:
                        filename = f"{batch_message.message_code}_{batch_message.current_batch}.json"
                        path = os.path.join(
                            "storage", batch_message.client_id, filename
                        )
                        if os.path.exists(path):
                            os.remove(path)
                    except Exception as e:
                        logger.warning("Error deleting stored batch file: %s", e)

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
            self._protocol_gateway.stop_client()
            return

    # def _publish_message(
    #     self,
    #     message_code,
    #     client_id,
    #     current_batch,
    #     is_last_batch,
    #     processed_data,
    # ):
    #     try:
    #         queue_key = self._get_queue_key(message_code)

    #         if queue_key:
    #             headers = {
    #                 "client_id": client_id,
    #             }
    #             batch_payload = [asdict(item) for item in processed_data]
    #             success = self.broker.publish(
    #                 target=queue_key,
    #                 message=batch_payload,
    #                 msg_type=message_code,
    #                 headers=headers,
    #             )

    #             if is_last_batch == IS_LAST_BATCH_FLAG:
    #                 success = self.broker.publish(
    #                     target=queue_key,
    #                     message={},  # Empty message to indicate end of batch
    #                     msg_type="EOS",
    #                     headers=headers,
    #                     priority=1,
    #                 )
    #                 self._processed_datasets_from_request += 1

    #             if not success:
    #                 logger.error("Failed to publish message to queue %s", queue_key)
    #                 raise Exception("Failed to publish message to queue")

    #     except (TypeError, ValueError) as e:
    #         logger.error("Error serializing data to JSON: %s", e)
    #         logger.error(processed_data)

    #     except Exception as e:
    #         logger.error(
    #             "Unexpected error in _publish_message: %s, from client %s",
    #             e,
    #             self._client_id,
    #         )
    #         self._protocol_gateway.stop_client()
    #         return

    def _get_queue_key(self, message_code):
        if message_code == "BATCH_MOVIES":
            return self.config["DEFAULT"]["movies_raw_queue"]
        elif message_code == "BATCH_CREDITS":
            return self.config["DEFAULT"]["credits_raw_queue"]
        elif message_code == "BATCH_RATINGS":
            return self.config["DEFAULT"]["ratings_raw_queue"]
        return None

    def send_result(self, result_data):
        """
        Send the result data to the client.
        """
        try:
            logger.debug(
                "Sending result to client %s: %s", self._client_id, result_data
            )
            self._protocol_gateway.send_result(result_data)
            self.add_sent_answer()

        except Exception as e:
            logger.error("Error sending result to client %s: %s", self._client_id, e)
            self._protocol_gateway.stop_client()

    def _save_batch_to_disk(self, batch: BatchMessage):
        try:
            client_dir = os.path.join("storage", batch.client_id)
            os.makedirs(client_dir, exist_ok=True)
            filename = os.path.join(
                client_dir, f"{batch.message_code}_{batch.current_batch}.json"
            )
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(asdict(batch), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(
                "Error saving batch to disk for client %s: %s", batch.client_id, e
            )

    def _load_batches_from_disk(self):
        client_dir = os.path.join("storage", self._client_id)
        if not os.path.isdir(client_dir):
            return

        files = sorted(os.listdir(client_dir))  # ordena por batch
        for filename in files:
            if filename.endswith(".json"):
                path = os.path.join(client_dir, filename)
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    batch = BatchMessage(**data)
                    self.batches_stored.append(batch)
                    self.accumulated_batches += 1
