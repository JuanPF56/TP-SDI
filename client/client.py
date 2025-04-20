import socket
import signal
import time

from common.logger import get_logger
logger = get_logger("Client")

from protocol_client_gateway import ProtocolClient
from utils import download_dataset, send_datasets_to_server

MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 10

class Client:
    def __init__(self, host, port, max_batch_size):
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._protocol = None
        self._max_batch_size = max_batch_size
        self._was_closed = False

        signal.signal(signal.SIGTERM, self._stop_client)
        signal.signal(signal.SIGINT, self._stop_client)

    def _connect(self, retries=MAX_RETRIES, delay=DELAY_BETWEEN_RETRIES):
        attempt = 0
        while attempt < retries:
            try:
                self._socket.connect((self._host, self._port))
                logger.info(f"Connected to server at {self._host}:{self._port}")
                self._protocol = ProtocolClient(self._socket, self._max_batch_size)
                return
            except Exception as e:
                attempt += 1
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Max connection attempts reached. Check if server is up.")

    def _stop_client(self):
        try:
            if self._socket:
                self._was_closed = True
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
                logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Failed to close connection properly: {e}")

    def run(self, use_test_dataset):
        if use_test_dataset:
            datasets_path = "/datasets"
            logger.info("Using test dataset.")
        else:
            datasets_path = download_dataset()
            if not datasets_path:
                logger.error("Dataset download failed.")
                return
            logger.info("Using full dataset.")

        self._connect()
        if not self._protocol:
            logger.error("Protocol initialization failed. Stopping client.")
            return
        if not self._socket:
            logger.error("Socket connection failed.")
            return
        
        while self._protocol._is_connected():
            try:
                logger.info("Sending datasets to server...")
                send_datasets_to_server(datasets_path, self._protocol)
                logger.info("Datasets sent successfully.")
                break
            
            except OSError as e:
                if self._was_closed:
                    logger.info("Socket client was closed.")
                    break

            except Exception as e:
                logger.error(f"An error occurred: {e}")
                break

        self._stop_client()