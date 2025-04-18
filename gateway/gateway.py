import socket
import signal

from common.logger import get_logger
logger = get_logger("Gateway")

from protocol_gateway_client import ProtocolGateway
from common.protocol import TIPO_MENSAJE, SUCCESS, ERROR, IS_LAST_BATCH_FLAG

class Gateway():
    def __init__(self, port, listen_backlog, datasets_expected):
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))
        self._gateway_socket.listen(listen_backlog)
        self._was_closed = False
        self._clients_conected = []
        self._datasets_expected = datasets_expected
        self._datasets_received = 0
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
                logger.debug("Waiting for message...")

                header = protocol_gateway.receive_header()
                if header is None:
                    logger.error("Header is None")
                    break
                message_code, current_batch, is_last_batch, payload_len = header

                logger.debug(f"Message code: {message_code}")
                if message_code not in TIPO_MENSAJE:
                    logger.error(f"Invalid message code: {message_code}")
                    protocol_gateway.send_confirmation(ERROR)
                    break

                else:
                    logger.info(f"{message_code} - Receiving batch {current_batch}")
                    payload = protocol_gateway.receive_payload(payload_len)
                    if not payload or len(payload) != payload_len:
                        logger.error("Failed to receive full payload")
                        break

                    protocol_gateway.process_payload(payload)
                    if is_last_batch == IS_LAST_BATCH_FLAG:
                        protocol_gateway.send_confirmation(SUCCESS)
                        logger.info(f"Received all batches for {message_code}")
                        self._datasets_received += 1

                    if self._datasets_received == self._datasets_expected:
                        logger.info("All datasets received, processing queries.")
                        # TODO: PROCESAR LAS QUERIES Y MANDAR RESPUESTAS
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