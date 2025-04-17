import socket
import signal

from common.logger import get_logger
logger = get_logger("Gateway")

from protocol_gateway_client import ProtocolGateway

SUCCESS = 0
ERROR = 1

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
            protocol_gateway = ProtocolGateway(client_sock)

            while protocol_gateway._client_is_connected():
                # message_code = protocol_gateway.receive_message_code()

                amount_of_lines = protocol_gateway.receive_amount_of_lines()
                if amount_of_lines is None:
                    logger.error("Amount of lines is None")
                    break

                logger.info(f"Amount of lines to receive: {amount_of_lines}")
                for _ in range(amount_of_lines):
                    movie_data_size = protocol_gateway.receive_movie_data_len()
                    if movie_data_size is None:
                        logger.error("Data size is None")
                        break

                    movie_data = protocol_gateway.receive_movie_data(movie_data_size)
                    if movie_data_size is None:
                        logger.error("Movie data is None")
                        break
                    else:
                        logger.debug(str(movie_data))
                logger.info("All movies received")

                protocol_gateway.send_confirmation(SUCCESS)
                break

        except OSError as e:
            if protocol_gateway._client_is_connected() is False:
                logger.error(f"Client disconnected: {e}")
                return

        except Exception as e:
            logger.error(f"Error handling client connection: {e}")
            logger.error("Client socket is not connected")
            return

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