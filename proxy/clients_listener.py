# proxy/threads/clients_listener.py

import threading

from client_handler import client_handler
from common.logger import get_logger

logger = get_logger("clients_listener")


def clients_listener(proxy):
    current_index = 0
    while not proxy._was_closed:
        try:
            client_socket, addr = proxy.proxy_socket.accept()
            logger.info("Accepted client %s", addr)

            # Round-robin gateway selection
            if not proxy._gateways_connected:
                logger.warning("No gateways connected, rejecting client.")
                client_socket.close()
                continue

            gateways_list = list(proxy._gateways_connected.items())
            available_gateways = [gw for gw, connected in gateways_list if connected]
            if not available_gateways:
                logger.warning("No available gateways for client %s", addr)
                client_socket.close()
                continue

            gateway = available_gateways[current_index % len(available_gateways)]
            current_index += 1

            threading.Thread(
                target=client_handler,
                args=(proxy, client_socket, addr, gateway),
                daemon=True,
            ).start()

        except Exception as e:
            if proxy._was_closed:
                logger.info("Proxy closing, stopping client listener.")
                break
            logger.error("Error accepting client: %s", e)
