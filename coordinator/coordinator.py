"""This script monitors Docker containers and restarts them if they are not running."""

import os
import time
import docker
from common.logger import get_logger

logger = get_logger("Coordinator")

SLEEP = 10

MONITORED_NODES = os.getenv("MONITORED_NODES", "")
nodos = MONITORED_NODES.split(",") if MONITORED_NODES else []

client = docker.from_env()


def reiniciar_nodo(nombre):
    """Reinicia un contenedor Docker dado su nombre."""
    try:
        container = client.containers.get(nombre)
        container.stop()
        time.sleep(SLEEP)
        container.start()
        logger.info("%s reiniciado", nombre)
    except Exception as e:
        logger.error("Error al reiniciar %s: %s", nombre, e)


def monitorear():
    """Monitorea los contenedores y reinicia los que no están corriendo."""
    if not nodos:
        logger.warning("No hay nodos configurados en MONITORED_NODES.")
        return

    logger.info("Coordinador iniciado. Monitoreando los siguientes nodos: %s", nodos)
    while True:
        for nodo in nodos:
            try:
                estado = client.containers.get(nodo).status
                if estado != "running":
                    logger.info(
                        "[!] %s no está corriendo, intentando reiniciar...", nodo
                    )
                    reiniciar_nodo(nodo)
            except Exception as e:
                logger.error("Error revisando %s: %s", nodo, e)
        time.sleep(SLEEP)


if __name__ == "__main__":
    monitorear()
