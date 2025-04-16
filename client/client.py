import socket
import configparser
from common.logger import get_logger

logger = get_logger("Client")

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

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

def main():
    config = load_config()
    logger.info("Client node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    client = Client(config["DEFAULT"]["GATEWAY_HOST"], int(config["DEFAULT"]["GATEWAY_PORT"]))
    client.connect()
    while True:
        message = "Hello, Gateway!"
        client.send(message)
        response = client.receive()
        if response is None:
            break
        print(f"Server response: {response}")
    client.close()

if __name__ == "__main__":
    main()

