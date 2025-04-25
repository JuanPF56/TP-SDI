import socket
import signal
import pika
import json
import os
import time
from dataclasses import asdict

from common.logger import get_logger
logger = get_logger("Gateway")

from protocol_gateway_client import ProtocolGateway
from common.protocol import TIPO_MENSAJE, SUCCESS, ERROR, IS_LAST_BATCH_FLAG

from result_dispatcher import ResultDispatcher

def connect_to_rabbitmq(host, retries=5, delay=3):
    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            logger.info("Successfully connected to RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
    raise RuntimeError("Failed to connect to RabbitMQ after multiple attempts.")

class Gateway():
    def __init__(self, config):
        self.config = config
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', int(config["DEFAULT"]["GATEWAY_PORT"])))
        self._gateway_socket.listen(int(config["DEFAULT"]["LISTEN_BACKLOG"]))
        self._was_closed = False
        self._clients_conected = []
        self._datasets_expected = int(config["DEFAULT"]["DATASETS_EXPECTED"])
        self._datasets_received = 0

        # Connect to RabbitMQ with retry
        self.rabbitmq_connection = connect_to_rabbitmq(config["DEFAULT"]["rabbitmq_host"])
        self.rabbitmq_channel = self.rabbitmq_connection.channel()

        # Declare queues
        self.rabbitmq_channel.queue_declare(queue=config["DEFAULT"]["movies_raw_queue"])
        self.rabbitmq_channel.queue_declare(queue=config["DEFAULT"]["credits_raw_queue"])
        self.rabbitmq_channel.queue_declare(queue=config["DEFAULT"]["ratings_raw_queue"])
        self.rabbitmq_channel.queue_declare(queue=config["DEFAULT"]["results_queue"])

        logger.info(f"Gateway listening on port {config['DEFAULT']['GATEWAY_PORT']}")

        # Initialize ResultDispatcher
        self.result_dispatcher = ResultDispatcher(
            config["DEFAULT"]["rabbitmq_host"],
            config["DEFAULT"]["results_queue"],
            self._clients_conected
        )
        self.result_dispatcher.start()

        try:
            with open("/tmp/gateway_ready", "w") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error(f"Failed to create healthcheck file: {e}")

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def run(self):
        while not self._was_closed:
            try:
                protocol_gateway = self.__accept_new_connection()
                self.__handle_client_connection(protocol_gateway)
            except OSError as e:
                if self._was_closed:
                    break
                logger.error(f"Error accepting new connection: {e}")

    def __accept_new_connection(self):
        logger.info("Waiting for new connections...")
        c, addr = self._gateway_socket.accept()
        protocol_gateway = ProtocolGateway(c)
        self._clients_conected.append(protocol_gateway)
        logger.info(f"New connection from {addr}")
        return protocol_gateway

    def __handle_client_connection(self, protocol_gateway: ProtocolGateway):
        try:
            while protocol_gateway._client_is_connected():
                logger.debug("Waiting for message...")

                header = protocol_gateway.receive_header()
                if header is None:
                    logger.error("Header is None")
                    break

                message_code, current_batch, is_last_batch, payload_len = header
                logger.debug(f"Message code: {message_code}")
                if message_code not in TIPO_MENSAJE:
                    logger.error(f"Invalid message code: {message_code}")
                    protocol_gateway.send_confirmation(ERROR)
                    break

                logger.debug(f"{message_code} - Receiving batch {current_batch}")
                payload = protocol_gateway.receive_payload(payload_len)
                if not payload or len(payload) != payload_len:
                    logger.error("Failed to receive full payload")
                    break

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
                        # 🆕 Send the entire batch as a single message (JSON array)
                        batch_payload = json.dumps([asdict(item) for item in processed_data])
                        self.rabbitmq_channel.basic_publish(
                            exchange='',
                            routing_key=queue_key,
                            body=batch_payload,
                            properties=pika.BasicProperties(type=message_code)
                        )

                        # 🧊 End-of-stream marker only if it's the last batch
                        if is_last_batch == IS_LAST_BATCH_FLAG:
                            self.rabbitmq_channel.basic_publish(
                                exchange='',
                                routing_key=queue_key,
                                body=b'',
                                properties=pika.BasicProperties(type="EOS")
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
                    self._datasets_received += 1

                if self._datasets_received == self._datasets_expected:
                    logger.info("All datasets received, processing queries.")
                    self.result_dispatcher.join()
                    break

        except OSError as e:
            if not protocol_gateway._client_is_connected():
                logger.error(f"Client disconnected: {e}")
                return

        except Exception as e:
            logger.error(f"Error handling client connection: {e}")
            logger.error("Client socket is not connected")
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
            
            if self.rabbitmq_channel:
                self.rabbitmq_channel.close()
                logger.info("RabbitMQ channel closed.")

            if self.rabbitmq_connection:
                self.rabbitmq_connection.close()
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
        for client in self._clients_conected:
            try:
                client._stop_client()
            except Exception as e:
                logger.error(f"Error closing client socket: {e}")
        self._clients_conected.clear()