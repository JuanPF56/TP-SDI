"""Listener for gateways to check their availability and reconnect if needed."""

import socket
import threading

from client_handler import ClientHandler

from common.protocol import SIZE_OF_UINT8
import common.receiver as receiver
from common.logger import get_logger

logger = get_logger("gateways_listener")

RETRY_INTERVAL = 5  # segundos


class GatewaysListener(threading.Thread):
    def __init__(self, proxy):
        super().__init__(daemon=True)
        self.proxy = proxy
        self._stop_flag = threading.Event()

        self.gateways_port = int(
            proxy.config["DEFAULT"].get("GATEWAY_ACCEPT_PORT", 9000)
        )
        self.gateways_listener_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )
        self.gateways_listener_socket.bind(("", self.gateways_port))
        self.gateways_listener_socket.listen(
            int(self.proxy.config["DEFAULT"].get("LISTEN_BACKLOG", 5))
        )
        self.gateways_listener_socket.settimeout(
            1
        )  # Para chequear la bandera de stop periódicamente

    def run(self):
        logger.info(
            "GatewaysListener listening for gateway connections on port %d",
            self.gateways_port,
        )
        while not self._stop_flag.is_set():
            try:
                gateway_socket, addr = self.gateways_listener_socket.accept()
                logger.info("Received connection from gateway at %s", addr)

                # Leer identificador del gateway
                gateway_id = self._receive_identifier(gateway_socket)
                if not gateway_id:
                    logger.warning("Failed to receive gateway ID, closing socket.")
                    gateway_socket.close()
                    continue

                logger.info("Identified gateway as '%s'", gateway_id)

                # Si ya había un socket previo para este gateway, cerrarlo
                previous_socket = self.proxy._gateways_connected.get(gateway_id)
                if previous_socket:
                    logger.info(
                        "Replacing existing connection for gateway %s", gateway_id
                    )
                    try:
                        previous_socket.shutdown(socket.SHUT_RDWR)
                    except Exception:
                        pass
                    previous_socket.close()

                self.proxy._gateways_connected[gateway_id] = gateway_socket
                self.proxy._gateway_locks[gateway_id] = threading.Lock()
                logger.info("Gateway '%s' connected and registered.", gateway_id)

                self.proxy.start_gateway_response_handler(gateway_id, gateway_socket)

                # Si hay clientes esperando por este gateway, reconectarlos
                clients = self.proxy._clients_per_gateway.get(gateway_id, [])
                for client_id in clients:
                    client_info = self.proxy._connected_clients.get(client_id)
                    if client_info:
                        client_socket = client_info["client_socket"]
                        addr = client_info["addr"]

                        logger.info(
                            "Reconnecting client %s to gateway %s",
                            client_id,
                            gateway_id,
                        )

                        ClientHandler(
                            proxy=self.proxy,
                            client_socket=client_socket,
                            addr=addr,
                            gateway_id=gateway_id,
                            client_id=client_id,
                        ).start()

            except socket.timeout:
                continue
            except Exception as e:
                logger.error("Error accepting gateway connection: %s", e)

    def _receive_identifier(self, sock):
        try:
            gateway_number = receiver.receive_data(
                sock, SIZE_OF_UINT8, timeout=RETRY_INTERVAL
            )
            if not gateway_number:
                return None
            return int.from_bytes(gateway_number, byteorder="big")
        except Exception as e:
            logger.error("Failed to read gateway number: %s", e)
            return None

    def stop(self):
        logger.info("Stopping gateways listener...")
        self._stop_flag.set()
        try:
            self.gateways_listener_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        self.gateways_listener_socket.close()
        self.join()
        logger.info("Gateways listener stopped.")
