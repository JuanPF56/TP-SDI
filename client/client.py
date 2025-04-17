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
        
    def close(self):
        try:
            self.socket.close()
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")

    def run(self):
        self.connect()
        while True:
            message = "Hello, Gateway!"
            self.send(message)
            response = self.receive()
            if response is None:
                break
        self.close()
