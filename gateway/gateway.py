import socket
import signal
import json
import uuid
import os
import threading
import time

from common.logger import get_logger
logger = get_logger("Gateway")

from client_registry import ClientRegistry
from connected_client import ConnectedClient
from result_dispatcher import ResultDispatcher
from common.mom import RabbitMQProcessor

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

        # Initialize and start the ResultDispatcher
        self._result_dispatcher = None
        self._setup_result_dispatcher()

        # Nodes ready queue
        system_nodes_str = os.getenv("SYSTEM_NODES", "")
        logger.info(f"System nodes: {system_nodes_str}")
        self._system_nodes = len(system_nodes_str.split(",")) if system_nodes_str else 0
        self.source_queues = [self.config["DEFAULT"].get("nodes_ready_queue", "nodes_ready")]
        self.rabbitmq_processor = None
        self._ready_nodes = set()
        self._ready_nodes_lock = threading.Lock()
        self.rabbitmq_processor = None
        self._initialize_rabbitmq_processor()
        try:
            with open("/tmp/gateway_ready", "w") as f:
                f.write("ready")
            logger.info("Gateway is ready. Healthcheck file created.")
        except Exception as e:
            logger.error(f"Failed to create healthcheck file: {e}")

        # Signal handling
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _setup_result_dispatcher(self):
        """
        Set up the ResultDispatcher.
        """
        self._result_dispatcher = ResultDispatcher(
            self.config,
            clients_connected=self._clients_connected
        )
        self._result_dispatcher.start()

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues={} # Not publishing in this component
        )

        if not self.rabbitmq_processor.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            raise Exception("Error connecting to RabbitMQ.")
        
        logger.info("Connected to RabbitMQ.")

    def _wait_for_nodes_to_connect_to_rabbit(self, timeout=3600) -> bool:
        logger.info(f"Waiting for {self._system_nodes} nodes to connect...")
        
        # Iniciar el consumidor en otro hilo
        consumer_thread = threading.Thread(
            target=lambda: self.rabbitmq_processor.consume(self.callback),
            daemon=True
        )
        consumer_thread.start()

        start_time = time.time()
        wait_time = 1  # Tiempo inicial de espera (1 segundo)
        max_wait_time = 60  # Tiempo máximo de espera (en segundos)

        while len(self._ready_nodes) < self._system_nodes:
            logger.info(f"Connected nodes: {len(self._ready_nodes)} / {self._system_nodes}")
            logger.info(f"Connected nodes: {self._ready_nodes}")

            # Verifica si hemos excedido el tiempo máximo de espera
            if time.time() - start_time > timeout:
                logger.error("Timeout waiting for nodes to connect.")
                return False
            
            # Espera exponencial: duplicar el tiempo de espera en cada iteración
            time.sleep(wait_time)
            wait_time = min(wait_time * 2, max_wait_time)  # Asegura que el tiempo de espera no supere el límite

        logger.info("All nodes are connected. Ready to accept connections.")
        return True

    def callback(self, channel, method_frame, header_frame, body, queue_name):
        """
        Callback function to handle messages from the nodes_ready queue.
        """
        logger.info(f"Received message from nodes_ready queue: {body}")
        if body:
            try:
                node_name = json.loads(body)
                with self._ready_nodes_lock:
                    self._ready_nodes.add(node_name)
                logger.info(f"Node {node_name} is ready.")

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    def run(self):
        if self._wait_for_nodes_to_connect_to_rabbit() is False:
            logger.error("Error waiting for nodes to connect to RabbitMQ. Exiting...")
            return
        while not self._was_closed:
            try:
                self._reap_disconnected_clients()

                new_connected_client = self.__accept_new_connection()
                if new_connected_client is None:
                    logger.error("Failed to accept new connection")
                    continue

                logger.info(f"New client connected: {new_connected_client.get_client_id()}")
                new_connected_client.start()

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
            client_addr = accepted_address,
            config=self.config
        )
        self._clients_connected.add(new_connected_client)
        return new_connected_client

    def _reap_disconnected_clients(self):
        try:
            logger.info("Checking for any disconnected clients...")
            all_clients = self._clients_connected.get_all()
            for client in all_clients.values():
                if not client._client_is_connected():
                    logger.info(f"Removing disconnected client {client.get_client_id()}")
                    self._clients_connected.remove(client)
            logger.info("Done checking.")
        except Exception as e:
            logger.error(f"Error during client cleanup: {e}")

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping server...")
        self._stop_server()

    def _stop_server(self):
        logger.info("Stopping server...")

        if self._result_dispatcher:
            try:
                self._result_dispatcher.stop()
                self._result_dispatcher.join()
                logger.info("ResultDispatcher stopped.")
            except Exception as e:
                logger.warning(f"Error stopping ResultDispatcher: {e}")

        if self.rabbitmq_processor:
            try:
                self.rabbitmq_processor.close()
                logger.info("RabbitMQ connection closed.")
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ connection: {e}")

        self._was_closed = True

        try:
            self._close_connected_clients()
        except Exception as e:
            logger.warning(f"Error closing clients: {e}")

        if self._gateway_socket:
            try:
                self._gateway_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                logger.warning("Gateway socket already shut down or not connected.")
            except Exception as e:
                logger.error(f"Unexpected error during socket shutdown: {e}")
            finally:
                try:
                    self._gateway_socket.close()
                    logger.info("Gateway socket closed.")
                except Exception as e:
                    logger.warning(f"Error closing gateway socket: {e}")
                self._gateway_socket = None

        logger.info("Server stopped.")

    def _close_connected_clients(self):
        logger.info("Closing connected clients...")
        try:
            self._clients_connected.clear()
        except Exception as e:
            logger.error(f"Error closing client socket: {e}")