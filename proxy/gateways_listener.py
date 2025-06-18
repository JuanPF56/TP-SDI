"""Listener for gateways to check their availability and reconnect if needed."""

import socket
import time
import threading

from common.logger import get_logger

logger = get_logger("gateways_listener")

RETRY_INTERVAL = 5  # segundos


class GatewaysListener(threading.Thread):
    def __init__(self, proxy):
        super().__init__(daemon=True)
        self._stop_flag = threading.Event()

        self.proxy = proxy

    def run(self):
        while not self.proxy._was_closed:
            for host, port in self.proxy.gateways:
                key = (host, port)
                if (
                    key in self.proxy._gateways_connected
                    and self.proxy._gateways_connected[key]
                ):
                    continue  # ya está conectado

                try:
                    sock = socket.create_connection((host, port), timeout=2)
                    sock.close()
                    self.proxy._gateways_connected[key] = True
                    logger.info("Reconnected to gateway %s:%s", host, port)
                except Exception as e:
                    self.proxy._gateways_connected[key] = False
                    logger.debug("Gateway %s:%s still down: %s", host, port, e)

            time.sleep(RETRY_INTERVAL)

    def stop(self):
        logger.info("Stopping gateways listener...")
        self._stop_flag.set()
        self.join()
        logger.info("Gateways listener stopped.")
