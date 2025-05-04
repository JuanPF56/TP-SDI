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

    def __init__(self, client_id: str, client_socket: socket.socket, client_addr, config):
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

        self._requests_to_be_processed = 0
        self._processed_datasets_from_request = 0

        self._sent_answers = 0
        self._sent_answers_lock = threading.Lock()

        self.broker = None

        self._running = True
        self._stop_flag = threading.Event()

        self._condition = threading.Condition()

    def add_sent_answer(self):
        """
        Increment the number of sent answers.
        """
        with self._sent_answers_lock:
            self._sent_answers += 1
            logger.debug(f"Sent answers: {self._sent_answers}")

            # Notify when all responses are sent
            if self._sent_answers == (self._requests_to_be_processed * self._expected_answers_to_send_per_request):
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

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._protocol_gateway._client_is_connected()

    def _stop_client(self) -> None:
        """
        Close the client socket
        """
        if self.was_closed:
            return

        logger.info(f"Stopping client {self._client_id}")
        self._stop_flag.set()
        self._running = False

        if self.broker:
            try:
                self.broker.close()
            except Exception as e:
                logger.warning(f"Error al cerrar broker: {e}")

        try:
            self._protocol_gateway._stop_client()
        except Exception as e:
            logger.warning(f"Error al detener protocolo gateway: {e}")

        self.was_closed = True

    def run(self):
        """
        Run the connected client thread.
        """
        logger.info(f"Connected client {self._client_id} started.")
        try:
            self._protocol_gateway.send_client_id(self._client_id)
            self._requests_to_be_processed = self._protocol_gateway.receive_amount_of_requests()
            if self._requests_to_be_processed is None:
                logger.error("Failed to receive amount of requests")
                self._protocol_gateway._stop_client()
                return
            
            logger.debug(f"Client {self._client_id} requested {self._requests_to_be_processed} requests.")
            self.broker = RabbitMQProcessor(
                config=self.config,
                source_queues=[],  # Connected client does not consume messages, so empty list
                target_queues=[
                    self.config["DEFAULT"]["movies_raw_queue"],
                    self.config["DEFAULT"]["credits_raw_queue"],
                    self.config["DEFAULT"]["ratings_raw_queue"]
                ],
                rabbitmq_host=self.config["DEFAULT"]["rabbitmq_host"]
            )
            connected = self.broker.connect()
            if not connected:
                raise RuntimeError("Failed to connect to RabbitMQ.")

            while self._running and not self._stop_flag.is_set() and not self.was_closed:
                for i in range(self._requests_to_be_processed):
                    logger.info(f"Processing datasets from request {i + 1} of {self._requests_to_be_processed}")
                    while self._processed_datasets_from_request < self._expected_datasets_to_receive_per_request:
                        if self._process_request() is False:
                            logger.error("Failed to process request")
                            self._protocol_gateway._stop_client()
                            return

                # Wait for all responses to be sent
                with self._condition:
                    while self._sent_answers != (self._requests_to_be_processed * self._expected_answers_to_send_per_request):
                        logger.info("Waiting for all responses to be sent.")
                        self._condition.wait()  # Wait for notification

                # If all responses have been sent, close the client
                logger.info("All answers have been sent, closing client.")
                self._stop_client()

        except OSError as e:
            if not self._protocol_gateway._client_is_connected():
                logger.info(f"Client {self._client_id} disconnected: {e}")
                return

        except Exception as e:
            logger.error(f"Unexpected error in client {self._client_id}: {e}")
            self._protocol_gateway._stop_client()
            return
        
    def _process_request(self) -> bool:
        try:
            header = self._protocol_gateway.receive_header()
            if header is None:
                logger.error("Header is None")
                self._protocol_gateway._stop_client()
                return False

            message_code, encoded_id, request_number, current_batch, is_last_batch, payload_len = header
            logger.debug(f"Message code: {message_code}")
            
            if message_code not in TIPO_MENSAJE:
                logger.error(f"Invalid message code: {message_code}")
                self._protocol_gateway._stop_client()
                return False
            
            if message_code == "DISCONNECT":
                logger.info("Client requested disconnection.")
                self._protocol_gateway._stop_client()
                return True

            client_id = encoded_id.decode("utf-8")
            logger.debug(f"client {client_id} - {message_code} - Receiving batch {current_batch}")
            
            payload = self._protocol_gateway.receive_payload(payload_len)
            if not payload or len(payload) != payload_len:
                logger.error("Failed to receive full payload")
                self._protocol_gateway._stop_client()
                return False
            
            logger.debug(f"Received payload of length {len(payload)}")
            
            processed_data = self._protocol_gateway.process_payload(message_code, payload)
            if processed_data is None:
                if message_code == "BATCH_CREDITS":
                    # May be a partial batch
                    return True
                else:
                    logger.error("Failed to process payload")
                    return False

            self._publish_message(message_code, client_id, request_number, current_batch, is_last_batch, processed_data)
            return True
        
        except (socket.error, socket.timeout) as e:
            logger.error(f"Socket error raised: {e}, from client {self._client_id}")
            self._protocol_gateway._stop_client()
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error in _processs_request: {e}, from client {self._client_id}")
            self._protocol_gateway._stop_client()
            return False


    def _publish_message(self, message_code, client_id, request_number, current_batch, is_last_batch, processed_data):
        try:
            queue_key = self._get_queue_key(message_code)
            
            if queue_key:
                headers = {
                    "client_id": client_id,
                    "request_number": request_number,
                }
                batch_payload = [asdict(item) for item in processed_data]
                self.broker.publish(
                    target=queue_key,
                    message=batch_payload,
                    msg_type=message_code, 
                    headers=headers
                )

                if is_last_batch == IS_LAST_BATCH_FLAG:
                    self.broker.publish(
                        target=queue_key,
                        message={},  # Empty message to indicate end of batch
                        msg_type="EOS", 
                        headers=headers
                    )
                    self._processed_datasets_from_request += 1

        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing data to JSON: {e}")
            logger.error(processed_data)

        except Exception as e:
            logger.error(f"Unexpected error in _publish_message: {e}, from client {self._client_id}")
            self._protocol_gateway._stop_client()
            return

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
            logger.debug(f"Sending result to client {self._client_id}: {result_data}")
            self._protocol_gateway.send_result(result_data)
            self.add_sent_answer()
        
        except Exception as e:
            logger.error(f"Error sending result to client {self._client_id}: {e}")
            self._protocol_gateway._stop_client()