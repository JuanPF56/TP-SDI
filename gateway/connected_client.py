import socket

from common.logger import get_logger
logger = get_logger("ConnectedClient")

from protocol_gateway_client import ProtocolGateway

DATASETS_PER_REQUEST = 3
QUERYS_PER_REQUEST = 5

class ConnectedClient():
    """
    Class representing a connected client.
    """

    def __init__(self, client_id: str, client_socket: socket.socket, client_addr):
        """
        Initialize the ConnectedClient instance.

        :param protocol_gateway: The ProtocolGateway instance associated with this client.
        :param client_id: The unique identifier for this client.
        """
        self._client_id = client_id
        self._client_socket = client_socket
        self._client_addr = client_addr
        self._protocol_gateway = ProtocolGateway(client_socket)
        self.was_closed = False

        self.results_sender = None

        self._expected_datasets_to_receive_per_request = DATASETS_PER_REQUEST
        self._received_datasets = 0

        self._expected_answers_to_send_per_request = QUERYS_PER_REQUEST
        self._answeres_sent = 0

    def get_client_id(self):
        """
        Get the unique identifier for this client.

        :return: The unique identifier for this client.
        """
        return self._client_id
    
    def send_client_id(self):
        """
        Send the client ID to the connected client.
        """
        logger.info(f"Sending client ID {self._client_id} to client {self._client_addr}")
        self._protocol_gateway.send_client_id(self._client_id)

    def _client_is_connected(self) -> bool:
        """
        Check if the client is connected
        """
        return self._protocol_gateway._client_is_connected()
    
    def _stop_client(self) -> None:
        """
        Close the client socket
        """
        logger.info(f"Stopping client {self._client_id}")
        self._protocol_gateway._stop_client()
        self.was_closed = True

    def get_protocol_gateway(self):
        """
        Get the ProtocolGateway instance associated with this client.

        :return: The ProtocolGateway instance.
        """
        return self._protocol_gateway