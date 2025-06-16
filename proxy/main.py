"""
Proxy server that forwards requests to multiple gateways.
This script listens for incoming connections on port 9000 and forwards
requests to a list of defined gateways. If a gateway is unavailable,
it tries the next one in the list. The script uses threading to handle
multiple clients concurrently.
"""

import os
import socket
import threading

from common.logger import get_logger

logger = get_logger("proxy")

GATEWAYS = os.getenv("GATEWAYS", "localhost:8000").split(",")
GATEWAYS = [
    (gateway.split(":")[0], int(gateway.split(":")[1]))
    for gateway in GATEWAYS
    if ":" in gateway
]


def _forward(source, destination):
    while True:
        try:
            data = source.recv(4096)
            if not data:
                break
            destination.sendall(data)
        except:
            break


def _handle_client(client_socket):
    for host, port in GATEWAYS:
        try:
            gateway_socket = socket.create_connection((host, port), timeout=2)
            print(f"Connected to {host}")
            threading.Thread(
                target=_forward, args=(client_socket, gateway_socket)
            ).start()
            threading.Thread(
                target=_forward, args=(gateway_socket, client_socket)
            ).start()
            return
        except Exception as e:
            print(f"Could not connect to {host}: {e}")
    client_socket.close()


def main():
    """
    Main function to start the proxy server.
    It reads the GATEWAYS environment variable to determine which gateways
    to forward requests to. If no gateways are configured, it logs an error
    and exits. The server listens on the port specified by the PROXY_PORT
    environment variable, defaulting to 9000 if not set.
    """
    logger.info("Starting proxy server...")
    if not GATEWAYS:
        logger.error("No gateways configured. Set the GATEWAYS environment variable.")
        return
    logger.info("Configured gateways: %s", GATEWAYS)
    port = int(os.getenv("PROXY_PORT", "9000"))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("", port))
    server.listen()
    logger.info("Proxy listening on port %s", port)
    while True:
        client_sock, _ = server.accept()
        threading.Thread(target=_handle_client, args=(client_sock,)).start()


if __name__ == "__main__":
    main()
