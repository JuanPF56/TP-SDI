import ast
import kagglehub
import os
import csv
import socket
import signal

from common.logger import get_logger
logger = get_logger("Client")

from protocol_client_gateway import ProtocolClient
from utils import read_first_3_movies, log_movies, send_movies

class Client:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._protocol = None
        self._was_closed = False

        signal.signal(signal.SIGTERM, self._stop_client)
        signal.signal(signal.SIGINT, self._stop_client)

    def _connect(self):
        try:
            self._socket.connect((self._host, self._port))
            logger.info(f"Connected to server at {self._host}:{self._port}")
            self._protocol = ProtocolClient(self._socket)
        except Exception as e:
            logger.error(f"Failed to connect to server: {e}")

    def _download_dataset(self):
        try:
            logger.info("Downloading dataset with kagglehub...")
            path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
            logger.info(f"Dataset downloaded at: {path}")
            return path
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            return None

    def _stop_client(self):
        try:
            self._was_closed = True
            self._socket.close()
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")

    def run(self):
        dataset_path = self._download_dataset()
        if not dataset_path:
            logger.error("Dataset download failed.")
            return

        # TODO: Es solo para probar el cliente, borrar cuando se haga el cliente real
        movies = read_first_3_movies(dataset_path)
        log_movies(movies)

        self._connect()
        if not self._socket:
            logger.error("Socket connection failed.")
            return
        
        while not self._was_closed:
            try:
                # Send movies to server
                send_movies(dataset_path, self._protocol)
                logger.info("Sent movies to server.")
                
                # Receive response from server
                response = self._protocol.receive()
                logger.info(f"Received response from server: {response}")
                self._was_closed = True  # For testing purposes, close after one message
            
            except OSError as e:
                if self._was_closed:
                    logger.info("Socket was closed.")
                    break

            except Exception as e:
                logger.error(f"An error occurred: {e}")
                break

        self._stop_client()