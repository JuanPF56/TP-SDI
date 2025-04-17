import socket
import signal

from common.logger import get_logger

logger = get_logger("Gateway")

class Gateway():
    def __init__(self, port, listen_backlog):
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))
        self._gateway_socket.listen(listen_backlog)
        self._was_closed = False
        self._clients_conected = []
        logger.info(f"Gateway listening on port {port}")

        signal.signal(signal.SIGTERM, self._stop_server)
        signal.signal(signal.SIGINT, self._stop_server)

    def run(self):
        while not self._was_closed:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._was_closed:
                    break
                logger.error(f"Error accepting new connection: {e}")

    def __accept_new_connection(self):
        logger.info("Waiting for new connections...")
        c, addr = self._gateway_socket.accept()
        self._clients_conected.append(c)
        logger.info(f"New connection from {addr}")
        return c

    def __handle_client_connection(self, client_sock: socket.socket):
        try:
            data = client_sock.recv(1024)
            if not data:
                logger.info("Client disconnected")
                self._clients_conected.remove(client_sock)
                client_sock.close()
                return
            logger.info(f"Received data: {data.decode()}")

            message = "Hello, Client!"
            logger.info(f"Sending message: {message}")
            client_sock.sendall(message.encode())
        except Exception as e:
            logger.error(f"Error handling client connection: {e}")
            client_sock.close()
            self._clients_conected.remove(client_sock)

    def _stop_server(self, signum, frame):
        logger.info("Stopping server...")
        self._was_closed = True
        for client in self._clients_conected:
            try:
                client.close()
            except Exception as e:
                logger.error(f"Error closing client socket: {e}")
        self._clients_conected.clear()
        self._gateway_socket.shutdown(socket.SHUT_RDWR)
        self._gateway_socket.close()
        logger.info("Server stopped.")