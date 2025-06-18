"""Listener for gateways to check their availability and reconnect if needed."""

import socket
import time

from common.logger import get_logger

logger = get_logger("gateways_listener")

RETRY_INTERVAL = 5  # segundos


def gateways_listener(proxy):
    while not proxy._was_closed:
        for host, port in proxy.gateways:
            key = (host, port)
            if key in proxy._gateways_connected and proxy._gateways_connected[key]:
                continue  # ya está conectado

            try:
                sock = socket.create_connection((host, port), timeout=2)
                sock.close()
                proxy._gateways_connected[key] = True
                logger.info("Reconnected to gateway %s:%s", host, port)
            except Exception as e:
                proxy._gateways_connected[key] = False
                logger.debug("Gateway %s:%s still down: %s", host, port, e)

        time.sleep(RETRY_INTERVAL)
