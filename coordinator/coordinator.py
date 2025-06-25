"""This script monitors Docker containers and restarts them if they are not running."""

import os
import time
import signal
import sys
import docker

from common.logger import get_logger

logger = get_logger("Coordinator")

DEAD_TIME = 10
MONITOR_SLEEP = 1

MONITORED_NODES = os.getenv("MONITORED_NODES", "")
nodos = MONITORED_NODES.split(",") if MONITORED_NODES else []

client = docker.from_env()


def crear_flag_recovery(nombre):
    """Crea un archivo de flag de recuperación para el nodo."""
    try:
        if "gateway" in nombre.lower():
            return  # No crear flag de recuperación para gateways
        node_type = nombre.split("_")[0]
        node_suffix = nombre.split("_")[1] if len(nombre.split("_")) > 1 else ""
        node_number = nombre.split("_")[-1] if len(nombre.split("_")) > 2 else ""
        dir = f"./storage/{node_type}_{node_suffix}/"
        file = f"recovery_mode_{node_number}.flag"
        with open(os.path.join(dir, file), "w") as f:
            # Write a simple message to indicate recovery mode
            f.write("Recovery mode activated for node: " + nombre)
        logger.info("Flag de recuperación creado para %s", nombre)
    except Exception as e:
        logger.error("Error al crear flag de recuperación para %s: %s", nombre, e)


def reiniciar_nodo(nombre):
    """Reinicia un contenedor Docker dado su nombre."""
    try:
        container = client.containers.get(nombre)
        container.stop()
        crear_flag_recovery(nombre)
        wait_time = 1 if "gateway" in nombre.lower() else DEAD_TIME
        logger.info("Deteniendo %s (caida de %s segundos)", nombre, wait_time)
        time.sleep(wait_time)
        container.start()
        logger.info("%s reiniciado (tras caida de %s segundos)", nombre, wait_time)
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
        time.sleep(MONITOR_SLEEP)


def shutdown(signum, frame):
    """Apaga todo el sistema: detiene todos los contenedores y termina el proceso."""
    logger.warning(
        "Señal de apagado recibida (%s). Deteniendo TODOS los contenedores...", signum
    )
    try:
        all_containers = client.containers.list(all=True)
        for container in all_containers:
            try:
                if container.status == "running":
                    logger.info("Deteniendo contenedor: %s", container.name)
                    container.stop(timeout=5)
            except Exception as e:
                logger.error("Error deteniendo contenedor %s: %s", container.name, e)
    except Exception as e:
        logger.error("Error obteniendo lista de contenedores: %s", e)

    logger.warning("Todos los contenedores fueron detenidos. Saliendo...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    monitorear()
