import socket
import threading
import signal
from dataclasses import asdict

from common.logger import get_logger
logger = get_logger("ConnectedClient")

from protocol_gateway_client import ProtocolGateway
from common.protocol import TIPO_MENSAJE, IS_LAST_BATCH_FLAG
from common.mom import RabbitMQProcessor

DATASETS_PER_REQUEST = 3
QUERYS_PER_REQUEST = 5

class ConnectedClient(threading.Thread):
    """
    Class representing a connected client.
    """

    def __init__(self, client_id: str, client_socket: socket.socket, client_addr, broker: RabbitMQProcessor, config):
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

        self._expected_datasets_to_receive_per_request = DATASETS_PER_REQUEST
        self._received_datasets = 0

        self._expected_answers_to_send_per_request = QUERYS_PER_REQUEST

        self._requests_to_process = 0
        self._sent_answers = 0

        self.broker = broker

        self._running = True
        self._stop_flag = threading.Event()

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def get_client_id(self):
        """
        Get the unique identifier for this client.

        :return: The unique identifier for this client.
        """
        return self._client_id

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._protocol_gateway._client_is_connected()
    
    def _signal_handler(self, signum, frame):
        logger.info(f"Signal received, stopping client {self._client_id}")
        self._stop_client()

    def _stop_client(self) -> None:
        """
        Close the client socket
        """
        logger.info(f"Stopping client {self._client_id}")
        self._stop_flag.set()
        self._running = False
        if self.broker:
            self.broker.close()
            logger.info("Broker connection closed.")
        self._protocol_gateway._stop_client()
        self.was_closed = True

    def run(self):
        """
        Run the connected client thread.
        """
        logger.info(f"Connected client {self._client_id} started.")
        try:
            self._protocol_gateway.send_client_id(self._client_id)
            self._requests_to_process = self._protocol_gateway.receive_amount_of_requests()
            if self._requests_to_process is None:
                logger.error("Failed to receive amount of requests")
                self._protocol_gateway._stop_client()
                return
            
            logger.debug(f"Client {self._client_id} requested {self._requests_to_process} requests.")

            while self._running and not self._stop_flag.is_set() and not self.was_closed:
                logger.debug(f"Waiting incoming datasets from {self._client_id}")
                header = self._protocol_gateway.receive_header()
                if header is None:
                    logger.error("Header is None")
                    self._protocol_gateway._stop_client()
                    break

                message_code, encoded_id, request_number, current_batch, is_last_batch, payload_len = header
                logger.debug(f"Message code: {message_code}")
                if message_code not in TIPO_MENSAJE:
                    logger.error(f"Invalid message code: {message_code}")
                    self._protocol_gateway._stop_client()
                    break

                if message_code == "DISCONNECT":
                    logger.info("Client requested disconnection.")
                    self._protocol_gateway._stop_client()
                    break

                client_id = encoded_id.decode("utf-8")

                logger.debug(f"client {client_id} - {message_code} - Receiving batch {current_batch}")
                payload = self._protocol_gateway.receive_payload(payload_len)
                if not payload or len(payload) != payload_len:
                    logger.error("Failed to receive full payload")
                    self._protocol_gateway._stop_client()
                    break
                logger.debug(f"Received payload of length {len(payload)}")

                processed_data = self._protocol_gateway.process_payload(message_code, payload)
                if processed_data is None:
                    if message_code == "BATCH_CREDITS":
                        # May be a partial batch
                        continue
                    else:
                        logger.error("Failed to process payload")
                        break

                try:
                    queue_key = None
                    if message_code == "BATCH_MOVIES":
                        queue_key = self.config["DEFAULT"]["movies_raw_queue"]
                    elif message_code == "BATCH_CREDITS":
                        queue_key = self.config["DEFAULT"]["credits_raw_queue"]
                    elif message_code == "BATCH_RATINGS":
                        queue_key = self.config["DEFAULT"]["ratings_raw_queue"]

                    if queue_key:
                        batch_payload = [asdict(item) for item in processed_data]
                        self.broker.publish(
                            target=queue_key,
                            message=batch_payload,
                            msg_type=message_code
                        )

                        if is_last_batch == IS_LAST_BATCH_FLAG:
                            self.broker.publish(
                                target=queue_key,
                                message={}, # Empty message to indicate end of batch
                                msg_type="EOS"
                            )

                except (TypeError, ValueError) as e:
                    logger.error(f"Error serializing data to JSON: {e}")
                    logger.error(processed_data)
                    break

                if is_last_batch == IS_LAST_BATCH_FLAG:
                    if message_code == "BATCH_MOVIES":
                        total_lines = self._protocol_gateway._decoder.get_decoded_movies()
                        dataset_name = "movies"
                    elif message_code == "BATCH_CREDITS":
                        total_lines = self._protocol_gateway._decoder.get_decoded_credits()
                        dataset_name = "credits"
                    elif message_code == "BATCH_RATINGS":
                        total_lines = self._protocol_gateway._decoder.get_decoded_ratings()
                        dataset_name = "ratings"

                    logger.info(f"Received {total_lines} lines from {dataset_name}")
                    self._received_datasets += 1

                if self._received_datasets == self._expected_datasets_to_receive_per_request:
                    logger.info("All datasets received, processing queries.")
                    break

            if self._sent_answers == (self._requests_to_process * self._expected_answers_to_send_per_request):
                logger.info("All answers to all requests have been sent, stopping client.")
                self._stop_client()
                return

        except OSError as e:
            if not self._protocol_gateway._client_is_connected():
                logger.info(f"Client {self._client_id} disconnected: {e}")
                return

        except Exception as e:
            logger.error(f"Unexpected error in client {self._client_id}: {e}")
            return
        

    def send_result(self, result_data):
        """
        Send the result data to the client.
        """
        logger.debug(f"Sending result to client {self._client_id}: {result_data}")
        self._protocol_gateway.send_result(result_data)
        self._sent_answers += 1