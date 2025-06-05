"""
Client module for connecting to a server, sending datasets, and receiving results.
"""

import socket
import signal
import time

from utils import send_datasets_to_server
from result_receiver import ResultReceiver
from protocol_client_gateway import ProtocolClient, ServerNotConnectedError

from common.protocol import ProtocolError
from common.logger import get_logger

logger = get_logger("Client")


MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 50

QUERYS_EXPECTED = 5

DEFAULT_CLIENT_ID = 0


class Client:
    def __init__(self, host, port, max_batch_size):
        self._client_id = DEFAULT_CLIENT_ID
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
                logger.info("Connected to server at %s:%s", self._host, self._port)
                self._protocol = ProtocolClient(self._socket, self._max_batch_size)
                return
            except Exception as e:
                attempt += 1
                logger.warning("Connection attempt %d failed: %s", attempt, e)
                if attempt < retries:
                    logger.info("Retrying in %d seconds...", delay)
                    time.sleep(delay)
                else:
                    logger.error(
                        "Max connection attempts reached. Check if server is up."
                    )

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping client...")
        self._stop_client()

    def _stop_client(self):
        try:
            if self._socket:
                self._was_closed = True
                try:
                    self._socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    logger.error("Socket already shutted")
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
        """
        Main method to run the client.
        Args:
            use_test_dataset (bool): If True, uses a smaller test dataset.
        """
        datasets_path = "/data"
        if use_test_dataset:
            logger.info("Using test dataset.")
        else:
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
            logger.info("Client ID: %s", self._client_id)
        except ProtocolError as e:
            logger.error("Protocol error: %s", e)
            self._stop_client()
            return

        self._results_thread = ResultReceiver(
            self._protocol,
            self._query_responses_expected,
            self._client_id,
        )
        self._results_thread.start()

        while self._protocol.is_connected():
            try:

                send_datasets_to_server(datasets_path, self._protocol)

                logger.info("Waiting for pending results...")
                self._results_thread.join()
                break

            except ServerNotConnectedError:
                logger.error("Connection closed by server")
                self._results_thread.stop()
                self._results_thread.join()
                break

            except OSError:
                if self._was_closed:
                    logger.info("Socket client was closed.")
                    self._results_thread.stop()
                    self._results_thread.join()
                    break

            except Exception as e:
                logger.error("An error occurred: %s", e)
                break

        self._stop_client()
