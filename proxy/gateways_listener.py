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

                # Check if already marked as alive
                if (
                    key in self.proxy._gateways_connected
                    and self.proxy._gateways_connected[key]
                ):
                    continue  # ya está conectado

                try:
                    # Test if the gateway is back up
                    sock = socket.create_connection((host, port), timeout=2)
                    sock.close()

                    was_down = not self.proxy._gateways_connected.get(key, False)
                    self.proxy._gateways_connected[key] = True

                    logger.info("Reconnected to gateway %s:%s", host, port)

                    # If we had a GatewayConnection for this gateway, trigger reconnection
                    gateway_conn = self.proxy.gateway_connections.get(key)
                    if was_down and gateway_conn:
                        logger.info(
                            "Triggering reconnection on GatewayConnection %s:%s",
                            host,
                            port,
                        )
                        gateway_conn.reconnect()

                except Exception as e:
                    self.proxy._gateways_connected[key] = False
                    logger.debug("Gateway %s:%s still down: %s", host, port, e)

            time.sleep(RETRY_INTERVAL)

    def stop(self):
        logger.info("Stopping gateways listener...")
        self._stop_flag.set()
        self.join()
        logger.info("Gateways listener stopped.")
