import socket
import signal
import os
import uuid
from dataclasses import asdict

from common.logger import get_logger
logger = get_logger("Gateway")

from protocol_gateway_client import ProtocolGateway
from client_registry import ClientRegistry
from common.protocol import TIPO_MENSAJE, IS_LAST_BATCH_FLAG
from common.mom import RabbitMQProcessor

from result_dispatcher import ResultDispatcher
from connected_client import ConnectedClient

class Gateway():
    def __init__(self, config):
        self.config = config

        # Initialize gateway socket
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', int(config["DEFAULT"]["GATEWAY_PORT"])))
        self._gateway_socket.listen(int(config["DEFAULT"]["LISTEN_BACKLOG"]))
        logger.info(f"Gateway listening on port {config['DEFAULT']['GATEWAY_PORT']}")
        self._was_closed = False

        # Initialize connected clients registry that monitors the connected clients
        self._clients_connected = ClientRegistry()
        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])

        # Connect to RabbitMQ with retry
        self.rabbitmq = RabbitMQProcessor(
            config=config,
            source_queues=[],  # Gateway does not consume messages, so empty list
            target_queues=[
                config["DEFAULT"]["movies_raw_queue"],
                config["DEFAULT"]["credits_raw_queue"],
                config["DEFAULT"]["ratings_raw_queue"],
                config["DEFAULT"]["results_queue"],
            ],
            rabbitmq_host=config["DEFAULT"]["rabbitmq_host"]
        )
        connected = self.rabbitmq.connect()
        if not connected:
            raise RuntimeError("Failed to connect to RabbitMQ.")

        try:
            with open("/tmp/gateway_ready", "w") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error(f"Failed to create healthcheck file: {e}")

        # Initialize ResultDispatcher
        self.result_dispatcher = ResultDispatcher(
            config["DEFAULT"]["rabbitmq_host"],
            config["DEFAULT"]["results_queue"],
            self._clients_connected
        )
        self.result_dispatcher.start()

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def run(self):
        while not self._was_closed:
            try:
                new_connected_client = self.__accept_new_connection()
                self.__handle_client_connection(new_connected_client)
            except OSError as e:
                if self._was_closed:
                    break
                logger.error(f"Error accepting new connection: {e}")

    def __accept_new_connection(self):
        logger.info("Waiting for new connections...")
        accepted_socket, accepted_address = self._gateway_socket.accept()
        logger.info(f"New connection from {accepted_address}")

        new_connected_client = ConnectedClient(
            client_id = str(uuid.uuid4()),
            client_socket = accepted_socket,
            client_addr = accepted_address
        )
        self._clients_connected.add(new_connected_client)
        new_connected_client.send_client_id()
        return new_connected_client

    def __handle_client_connection(self, new_connected_client: ConnectedClient):
        try:
            while new_connected_client._client_is_connected():
                logger.debug("Waiting for message...")
                protocol_gateway = new_connected_client.get_protocol_gateway()

                header = protocol_gateway.receive_header()
                if header is None:
                    logger.error("Header is None")
                    protocol_gateway._stop_client()
                    break

                message_code, encoded_id, request_number, current_batch, is_last_batch, payload_len = header
                logger.debug(f"Message code: {message_code}")
                if message_code not in TIPO_MENSAJE:
                    logger.error(f"Invalid message code: {message_code}")
                    protocol_gateway._stop_client()
                    break

                if message_code == "DISCONNECT":
                    logger.info("Client requested disconnection.")
                    protocol_gateway._stop_client()
                    break

                client_id = encoded_id.decode("utf-8")

                logger.info(f"client {client_id} - {message_code} - Receiving batch {current_batch}")
                payload = protocol_gateway.receive_payload(payload_len)
                if not payload or len(payload) != payload_len:
                    logger.error("Failed to receive full payload")
                    protocol_gateway._stop_client()
                    break
                logger.debug(f"Received payload of length {len(payload)}")

                processed_data = protocol_gateway.process_payload(message_code, payload)
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
                        self.rabbitmq.publish(
                            target=queue_key,
                            message=batch_payload,
                            msg_type=message_code
                        )

                        if is_last_batch == IS_LAST_BATCH_FLAG:
                            self.rabbitmq.publish(
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
                        total_lines = protocol_gateway._decoder.get_decoded_movies()
                        dataset_name = "movies"
                    elif message_code == "BATCH_CREDITS":
                        total_lines = protocol_gateway._decoder.get_decoded_credits()
                        dataset_name = "credits"
                    elif message_code == "BATCH_RATINGS":
                        total_lines = protocol_gateway._decoder.get_decoded_ratings()
                        dataset_name = "ratings"

                    logger.info(f"Received {total_lines} lines from {dataset_name}")
                    new_connected_client._received_datasets += 1

                if new_connected_client._received_datasets == new_connected_client._expected_datasets_to_receive_per_request:
                    logger.info("All datasets received, processing queries.")
                    self.result_dispatcher.join()
                    break

        except OSError as e:
            if not protocol_gateway._client_is_connected():
                logger.error(f"Client disconnected: {e}")
                return

        except Exception as e:
            logger.error(f"Error handling client connection: {e}")
            return

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping server...")
        self._stop_server()

    def _stop_server(self):
        try:
            if self.result_dispatcher:
                self.result_dispatcher.stop()
                self.result_dispatcher.join()
                logger.info("Result dispatcher thread stopped.")
            
            if self.rabbitmq:
                self.rabbitmq.close()
                logger.info("RabbitMQ connection closed.")

            if self._gateway_socket:
                self._was_closed = True
                self._close_connected_clients()
                try:
                    self._gateway_socket.shutdown(socket.SHUT_RDWR)
                except OSError as e:
                    logger.error(f"Socket already shut down")
                finally:
                    if self._gateway_socket:
                        self._gateway_socket.close()
                        logger.info("Gateway socket closed.")
                    try:
                        os.remove("/tmp/gateway_ready")
                    except FileNotFoundError:
                        pass
                    logger.info("Server stopped.")

        except Exception as e:
            logger.error(f"Failed to close server properly: {e}")

    def _close_connected_clients(self):
        logger.info("Closing connected clients...")
        try:
            self._clients_connected.clear()
        except Exception as e:
            logger.error(f"Error closing client socket: {e}")