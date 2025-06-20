# proxy/threads/clients_listener.py

import threading
import socket

from client_handler import ClientHandler
from common.logger import get_logger

logger = get_logger("clients_listener")


class ClientsListener(threading.Thread):
    def __init__(self, proxy):
        super().__init__(daemon=True)
        self.proxy = proxy
        self._stop_flag = threading.Event()

        self.clients_port = int(
            proxy.config["DEFAULT"].get("CLIENTS_ACCEPT_PORT", 8000)
        )
        self.clients_listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients_listener_socket.bind(("", self.clients_port))
        self.clients_listener_socket.listen(
            int(self.proxy.config["DEFAULT"].get("LISTEN_BACKLOG", 5))
        )
        self.clients_listener_socket.settimeout(
            1
        )  # Para chequear la bandera de stop periódicamente

        self.current_index = 0

    def run(self):
        logger.info(
            "ClientsListener listening for client connections on port %d",
            self.clients_port,
        )
        while not self._stop_flag.is_set():
            try:
                client_socket, addr = self.clients_listener_socket.accept()
                logger.info("Accepted client %s", addr)

                # Round-robin gateway selection
                if not self.proxy._gateways_connected:
                    logger.warning("No gateways connected, rejecting client.")
                    client_socket.close()
                    continue

                gateways_list = list(self.proxy._gateways_connected.items())
                available_gateways = [
                    gw for gw, connected in gateways_list if connected
                ]
                if not available_gateways:
                    logger.warning("No available gateways for client %s", addr)
                    client_socket.close()
                    continue

                gateway = available_gateways[
                    self.current_index % len(available_gateways)
                ]
                logger.info(
                    "Selected gateway %s for client %s",
                    gateway,
                    addr,
                )
                self.current_index += 1

                ClientHandler(self.proxy, client_socket, addr, gateway).start()

            except socket.timeout:
                continue  # volver al loop para chequear el _stop_flag

            except Exception as e:
                if self.proxy._was_closed:
                    logger.info("Proxy closing, stopping client listener.")
                    break
                logger.error("Error accepting client: %s", e)

    def stop(self):
        logger.info("Stopping clients listener...")
        self._stop_flag.set()
        self.join()
        logger.info("Clients listener stopped.")
