"""
This module defines the ConnectedClient class, which represents a client connected to the protocol gateway.
"""

import os
import json
import socket
import threading
from dataclasses import asdict, is_dataclass
import fcntl  # For file locking on Unix systems
import time
from pathlib import Path

from protocol_gateway_client import ProtocolGateway
from batch_message import BatchMessage

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
            self._load_batches_from_disk()
        else:
            logger.info("Recovery mode is disabled for client %s", self._client_id)

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
                    self._safe_delete_batch_file(batch_message)

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

    def send_result(self, result_data):
        """
        Send the result data to the client.
        """
        try:
            logger.info("Sending result to client %s: %s", self._client_id, result_data)
            self._protocol_gateway.send_result(result_data)
            self.add_sent_answer()

        except Exception as e:
            logger.error("Error sending result to client %s: %s", self._client_id, e)
            self._stop_client()

    def _save_batch_to_disk(self, batch: BatchMessage):
        """
        Save batch to disk with improved error handling and atomic writes.
        """
        try:
            client_dir = Path("storage") / batch.client_id
            client_dir.mkdir(parents=True, exist_ok=True)

            final_path = client_dir / f"{batch.message_code}_{batch.current_batch}.json"

            # Create a unique temporary file in the same directory
            temp_path = (
                client_dir
                / f".tmp_{batch.message_code}_{batch.current_batch}_{int(time.time() * 1000)}.json"
            )

            try:
                # Write to temporary file with proper error handling
                with open(temp_path, "w", encoding="utf-8") as tmp_file:
                    # Use file locking if available (Unix systems)
                    try:
                        fcntl.flock(tmp_file.fileno(), fcntl.LOCK_EX)
                    except (AttributeError, OSError):
                        # fcntl not available on Windows or file locking failed
                        pass

                    json.dump(
                        custom_asdict(batch),
                        tmp_file,
                        ensure_ascii=False,
                        indent=2,
                        separators=(",", ": "),  # Consistent formatting
                    )
                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())  # Force write to disk

                # Atomically replace the final file
                if os.name == "nt":  # Windows
                    # On Windows, remove the target file first if it exists
                    if final_path.exists():
                        final_path.unlink()
                    temp_path.rename(final_path)
                else:  # Unix-like systems
                    temp_path.rename(final_path)

                logger.debug("Successfully saved batch to disk: %s", final_path)

            except Exception as e:
                # Clean up temporary file if something went wrong
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass
                raise e

        except Exception as e:
            logger.error(
                "Error saving batch to disk for client %s: %s", batch.client_id, e
            )
            raise

    def _safe_delete_batch_file(self, batch_message: BatchMessage):
        """
        Safely delete batch file with retry logic.
        """
        filename = f"{batch_message.message_code}_{batch_message.current_batch}.json"
        path = Path("storage") / batch_message.client_id / filename

        if not path.exists():
            return

        max_retries = 3
        for attempt in range(max_retries):
            try:
                path.unlink()
                logger.debug("Successfully deleted batch file: %s", path)
                return
            except OSError as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        "Failed to delete batch file %s (attempt %d/%d): %s",
                        path,
                        attempt + 1,
                        max_retries,
                        e,
                    )
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(
                        "Failed to delete batch file %s after %d attempts: %s",
                        path,
                        max_retries,
                        e,
                    )

    def _load_batches_from_disk(self):
        """
        Load batches from disk with improved error handling and corruption detection.
        """
        try:
            client_dir = Path("storage") / self._client_id
            if not client_dir.is_dir():
                logger.info("No storage directory found for client %s", self._client_id)
                return

            # Get all JSON files, excluding temporary files
            json_files = [
                f
                for f in client_dir.iterdir()
                if f.is_file()
                and f.suffix == ".json"
                and not f.name.startswith(".tmp_")
            ]

            # Sort files to ensure consistent processing order
            json_files.sort(key=lambda x: x.name)

            loaded_count = 0
            error_count = 0

            for file_path in json_files:
                try:
                    # Check if file is empty or too small
                    if file_path.stat().st_size < 10:  # Minimum viable JSON size
                        logger.warning(
                            "File %s is too small, likely corrupted. Skipping.",
                            file_path,
                        )
                        self._move_corrupted_file(file_path)
                        error_count += 1
                        continue

                    with open(file_path, "r", encoding="utf-8") as f:
                        # Try to lock file for reading if possible
                        try:
                            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                        except (AttributeError, OSError):
                            pass

                        # Read and parse JSON
                        try:
                            data = json.load(f)
                        except json.JSONDecodeError as e:
                            logger.error(
                                "JSON decode error in file %s: %s", file_path, e
                            )
                            self._move_corrupted_file(file_path)
                            error_count += 1
                            continue

                    # Validate the data structure
                    if not self._validate_batch_data(data):
                        logger.error(
                            "Invalid batch data structure in file %s", file_path
                        )
                        self._move_corrupted_file(file_path)
                        error_count += 1
                        continue

                    # Try to reconstruct the BatchMessage
                    try:
                        batch = BatchMessage.from_json_with_casting(data)
                        self.batches_stored.append(batch)
                        self.accumulated_batches += 1
                        loaded_count += 1
                        logger.debug(
                            "Successfully loaded batch from %s", file_path.name
                        )

                    except Exception as e:
                        logger.error(
                            "Error reconstructing batch from %s: %s", file_path, e
                        )
                        self._move_corrupted_file(file_path)
                        error_count += 1
                        continue

                except Exception as e:
                    logger.error("Unexpected error loading file %s: %s", file_path, e)
                    error_count += 1
                    continue

            logger.info(
                "Batch loading complete for client %s: %d loaded, %d errors",
                self._client_id,
                loaded_count,
                error_count,
            )

        except Exception as e:
            logger.error(
                "Critical error loading batches from disk for client %s: %s",
                self._client_id,
                e,
            )

    def _validate_batch_data(self, data):
        """
        Validate that the loaded data has the expected structure for a BatchMessage.
        """
        if not isinstance(data, dict):
            return False

        required_fields = [
            "message_id",
            "message_code",
            "client_id",
            "current_batch",
            "is_last_batch",
        ]

        for field in required_fields:
            if field not in data:
                logger.warning("Missing required field '%s' in batch data", field)
                return False

        # Additional validation can be added here
        return True

    def _move_corrupted_file(self, file_path: Path):
        """
        Move corrupted files to a separate directory for inspection.
        """
        try:
            corrupted_dir = file_path.parent / "corrupted"
            corrupted_dir.mkdir(exist_ok=True)

            # Generate unique name to avoid conflicts
            timestamp = int(time.time() * 1000)
            new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            corrupted_path = corrupted_dir / new_name

            file_path.rename(corrupted_path)
            logger.info("Moved corrupted file %s to %s", file_path, corrupted_path)

        except Exception as e:
            logger.error("Failed to move corrupted file %s: %s", file_path, e)
            # If we can't move it, try to delete it as last resort
            try:
                file_path.unlink()
                logger.warning("Deleted corrupted file %s", file_path)
            except Exception:
                logger.error("Failed to delete corrupted file %s", file_path)


def custom_asdict(obj):
    """
    Custom serialization function with better error handling.
    """
    try:
        if isinstance(obj, list):
            return [custom_asdict(i) for i in obj]
        elif hasattr(obj, "__dict__") or hasattr(obj, "__dataclass_fields__"):
            return {k: custom_asdict(v) for k, v in asdict(obj).items()}
        else:
            return obj
    except Exception as e:
        logger.error("Error in custom_asdict serialization: %s", e)
        # Return a safe representation
        return str(obj)
