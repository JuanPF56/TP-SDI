import socket
import signal
import time

from common.logger import get_logger
logger = get_logger("Client")

from protocol_client_gateway import ProtocolClient
from utils import download_dataset, send_datasets_to_server

from result_receiver import ResultReceiver

import common.exceptions as exceptions

MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 10

QUERYS_EXPECTED = 5

DEFAULT_CLIENT_ID = 0

REQUESTS_TO_SERVER = 1 # Number of requests to server (1 request = 3 datasets = 5 queries)

class Client:
    def __init__(self, host, port, max_batch_size, requests_to_server=REQUESTS_TO_SERVER):
        self._client_id = DEFAULT_CLIENT_ID
        self._requests_to_server = requests_to_server
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._protocol = None
        self._max_batch_size = max_batch_size
        self._was_closed = False

        self._query_responses_expected = QUERYS_EXPECTED
        self._results_thread = None

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

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

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping client...")
        self._stop_client()

    def _stop_client(self):
        try:
            if self._socket:
                self._was_closed = True
                try:
                    self._socket.shutdown(socket.SHUT_RDWR)
                except OSError as e:
                    logger.error(f"Socket already shutted")
                finally:
                    if self._socket:
                        self._socket.close()
                        logger.info("Socket closed.")
            if self._results_thread:
                self._results_thread.stop()
                self._results_thread.join()
                logger.info("Result receiver thread stopped.")
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
        
        try:
            self._client_id = self._protocol.get_client_id()
            logger.info(f"Client ID: {self._client_id}")
        except exceptions.ProtocolError as e:
            logger.error(f"Protocol error: {e}")
            self._stop_client()
            return

        self._results_thread = ResultReceiver(self._protocol, self._query_responses_expected, self._requests_to_server)
        self._results_thread.start()

        while self._protocol._is_connected():
            try:
                for i in range(self._requests_to_server):
                    logger.info("REQUEST N° " + str(i + 1) + " / " + str(self._requests_to_server))
                    send_datasets_to_server(datasets_path, self._protocol, i+1)
                    
                logger.info("Waiting for pending results...")    
                self._results_thread.join()
                break

            except exceptions.ServerNotConnectedError:
                logger.error("Connection closed by server")
                self._results_thread.stop()
                self._results_thread.join()
                break

            except OSError as e:
                if self._was_closed:
                    logger.info("Socket client was closed.")
                    self._results_thread.stop()
                    self._results_thread.join()
                    break

            except Exception as e:
                logger.error(f"An error occurred: {e}")
                break

        self._stop_client()