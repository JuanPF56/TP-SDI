# proxy/threads/clients_listener.py

import threading

from client_handler import client_handler
from common.logger import get_logger

logger = get_logger("clients_listener")


class ClientsListener(threading.Thread):
    def __init__(self, proxy):
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        self.proxy = proxy
        self._was_closed = False
        self.current_index = 0

    def run(self):
        while not self._stop_flag.is_set():
            try:
                client_socket, addr = self.proxy.proxy_socket.accept()
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
                self.current_index += 1

                threading.Thread(
                    target=client_handler,
                    args=(self.proxy, client_socket, addr, gateway),
                    daemon=True,
                ).start()

            except Exception as e:
                if self.proxy._was_closed:
                    logger.info("Proxy closing, stopping client listener.")
                    break
                logger.error("Error accepting client: %s", e)
