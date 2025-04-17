import kagglehub
import os
import csv
import socket
from common.logger import get_logger

logger = get_logger("Client")

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        try:
            self.socket.connect((self.host, self.port))
            logger.info(f"Connected to server at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to server: {e}")

    def send(self, message):
        try:
            self.socket.sendall(message.encode())
            logger.info(f"Sent message: {message}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def receive(self):
        try:
            data = self.socket.recv(1024)
            if not data:
                logger.info("Server closed the connection")
                return None
            logger.info(f"Received message: {data.decode()}")
            return data.decode()
        except Exception as e:
            logger.error(f"Failed to receive message: {e}")
            return None

    def download_dataset(self):
        try:
            logger.info("Downloading dataset with kagglehub...")
            path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
            logger.info(f"Dataset downloaded at: {path}")
            return path
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            return None

    # TODO: Es solo para probar el cliente, borrar cuando se haga el cliente real
    def read_first_3_movies(self, path):
        try:
            csv_path = os.path.join(path, "movies_metadata.csv")
            with open(csv_path, mode="r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                movies = []
                for i, row in enumerate(reader):
                    if i >= 3:
                        break
                    movies.append(f"Movie {i+1}: {row.get('title')} ({row.get('release_date')})")
                return movies
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            return []
        
    def log_movies(self, movies):
        for movie in movies:
            logger.info(movie)

    def close(self):
        try:
            self.socket.close()
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")

    def run(self):
        dataset_path = self.download_dataset()
        if not dataset_path:
            return

        movies = self.read_first_3_movies(dataset_path)
        if not movies:
            return

        self.log_movies(movies)

        self.connect()
        while True:
            message = "Hello, Gateway!"
            self.send(message)
            response = self.receive()
            if response is None:
                break
        self.close()