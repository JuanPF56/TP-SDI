"""This script monitors Docker containers and restarts them if they are not running."""

import os
import time
import signal
import sys
import docker
import threading
from common.leader_election import LeaderElector

from common.logger import get_logger

logger = get_logger("Coordinator")

DEAD_TIME = 20
MONITOR_SLEEP = 1


class Coordinator:
    def __init__(self):
        """Initialize the Coordinator with leader election capabilities."""
        self.client = docker.from_env()
        self.running = True
        self.is_leader = False
        self.monitor_thread = None
        
        self.monitored_nodes = self._parse_monitored_nodes()
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.peers = os.getenv("PEERS", "")
        self.election_port = int(os.getenv("ELECTION_PORT", "9000"))
        
        self.leader_elector = LeaderElector(
            node_id=self.node_id,
            peers=self.peers,
            election_port=self.election_port,
            election_logic=self.on_leader_change
        )
        
        logger.info(
            f"Coordinator initialized - Node ID: {self.node_id}, "
            f"Monitoring: {self.monitored_nodes}, "
            f"Peers: {self.peers}"
        )
    def _parse_peers(self, peers_str):
        """
        Convert peers string like "filter_cleanup_1:9001,filter_cleanup_2:9002"
        into dict {1: ("filter_cleanup_1", 9001), 2: ("filter_cleanup_2", 9002)}
        Use container names instead of 127.0.0.1 for Docker networking
        """
        peers = {}
        for item in peers_str.split(","):
            name, port = item.split(":")
            node_id = int(name.split("_")[-1])
            peers[node_id] = (name, int(port))
        return peers
    
    def _parse_monitored_nodes(self):
        """Parse monitored nodes from environment variable."""
        monitored_nodes_str = os.getenv("MONITORED_NODES", "")
        return monitored_nodes_str.split(",") if monitored_nodes_str else []

    def on_leader_change(self, new_leader_id):
        """Called when leader changes."""
        old_leader_status = self.is_leader
        self.is_leader = (new_leader_id == self.node_id)
        
        if self.is_leader and not old_leader_status:
            logger.info(f"[Node {self.node_id}] ✅ I am now the LEADER! Starting coordinator duties...")
            self.start_monitoring()
        elif not self.is_leader and old_leader_status:
            logger.info(f"[Node {self.node_id}] ❌ I am no longer the leader. Stopping all coordinator activities...")
            self.stop_monitoring()
        elif self.is_leader:
            logger.info(f"[Node {self.node_id}] ✅ Continuing as leader and coordinator...")
        else:
            logger.info(f"[Node {self.node_id}] 💤 Node {new_leader_id} is the leader. I'm in STANDBY mode (doing nothing)...")

    def start_monitoring(self):
        """Start the monitoring thread ONLY if this node is the leader."""
        if not self.is_leader:
            logger.warning(f"[Node {self.node_id}] ❌ Cannot start coordinator duties - I'm NOT the leader")
            return
            
        if self.monitor_thread and self.monitor_thread.is_alive():
            logger.info(f"[Node {self.node_id}] ✅ Coordinator duties already running")
            return
            
        if not self.monitored_nodes:
            logger.warning(f"[Node {self.node_id}] ⚠️ No containers configured in MONITORED_NODES")
            return
            
        logger.info(f"[Node {self.node_id}] 🚀 Starting COORDINATOR duties - monitoring containers: {self.monitored_nodes}")
        self.monitor_thread = threading.Thread(target=self._monitor_containers, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop the monitoring (monitoring thread will check is_leader flag)."""
        logger.info(f"[Node {self.node_id}] 🛑 Stopping COORDINATOR duties - entering STANDBY mode")

    def _monitor_containers(self):
        """Monitor containers and restart them if needed (ONLY when I'm the leader)."""
        logger.info(f"[Node {self.node_id}] 👁️ COORDINATOR monitoring started")
        
        while self.running:
            if not self.is_leader:
                logger.info(f"[Node {self.node_id}] 💤 No longer leader, stopping coordinator duties and entering STANDBY")
                break
                
            for nodo in self.monitored_nodes:
                if not self.running or not self.is_leader:
                    break
                    
                try:
                    container = self.client.containers.get(nodo)
                    estado = container.status
                    if estado != "running":
                        logger.warning(
                            f"[Node {self.node_id}] 🔧 COORDINATOR ACTION: {nodo} is DOWN, restarting..."
                        )
                        self._restart_container(nodo)
                    else:
                        logger.debug(f"[Node {self.node_id}] ✅ {nodo} is running")
                except Exception as e:
                    logger.error(f"[Node {self.node_id}] ❌ Error checking {nodo}: {e}")
                    
            time.sleep(MONITOR_SLEEP)
            
        logger.info(f"[Node {self.node_id}] 🛑 COORDINATOR monitoring stopped - now in STANDBY mode")

    def _create_recovery_flag(self, nombre):
        """Crea un archivo de flag de recuperación para el nodo."""
        try:
            if "gateway" in nombre.lower():
                return  
                
            node_type = nombre.split("_")[0]
            node_suffix = nombre.split("_")[1] if len(nombre.split("_")) > 1 else ""
            node_number = nombre.split("_")[-1] if len(nombre.split("_")) > 2 else ""
            dir_path = f"./storage/{node_type}_{node_suffix}/"
            file_path = f"recovery_mode_{node_number}.flag"
            
            os.makedirs(dir_path, exist_ok=True)
            
            with open(os.path.join(dir_path, file_path), "w") as f:
                f.write("Recovery mode activated for node: " + nombre)
                
            logger.info(f"[Node {self.node_id}] Flag de recuperación creado para {nombre}")
        except Exception as e:
            logger.error(f"[Node {self.node_id}] Error al crear flag de recuperación para {nombre}: {e}")

    def _restart_container(self, nombre):
        """Reinicia un contenedor Docker (SOLO si soy el líder)."""
        if not self.is_leader:
            logger.warning(f"[Node {self.node_id}] ❌ Cannot restart {nombre} - I'm NOT the leader!")
            return
            
        try:
            container = self.client.containers.get(nombre)
            container.stop()
            self._create_recovery_flag(nombre)
            
            wait_time = 1 if "gateway" in nombre.lower() else DEAD_TIME
            logger.info(f"[Node {self.node_id}] 🔧 COORDINATOR: Stopping {nombre} (downtime: {wait_time}s)")
            time.sleep(wait_time)
            
            container.start()
            logger.info(f"[Node {self.node_id}] ✅ COORDINATOR: {nombre} restarted successfully!")
        except Exception as e:
            logger.error(f"[Node {self.node_id}] ❌ COORDINATOR ERROR: Failed to restart {nombre}: {e}")

    def shutdown_all_containers(self):
        """Apaga todos los contenedores del sistema."""
        logger.warning(f"[Node {self.node_id}] Deteniendo TODOS los contenedores...")
        try:
            all_containers = self.client.containers.list(all=True)
            for container in all_containers:
                try:
                    if container.status == "running":
                        logger.info(f"[Node {self.node_id}] Deteniendo contenedor: {container.name}")
                        container.stop(timeout=5)
                except Exception as e:
                    logger.error(f"[Node {self.node_id}] Error deteniendo contenedor {container.name}: {e}")
        except Exception as e:
            logger.error(f"[Node {self.node_id}] Error obteniendo lista de contenedores: {e}")

    def run(self):
        """Run the coordinator."""
        logger.info(f"[Node {self.node_id}] 🚀 Coordinator started - participating in leader election...")
        logger.info(f"[Node {self.node_id}] 💤 Currently in STANDBY mode (waiting for election results)")
        
        try:
            while self.running:
                # Just stay alive and let leader election handle everything
                # If we become leader, on_leader_change() will start coordinator duties
                # If we're not leader, we just stay in this loop doing nothing
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"[Node {self.node_id}] KeyboardInterrupt received")
        finally:
            self.shutdown()

    def shutdown(self):
        """Shutdown the coordinator gracefully."""
        logger.info(f"[Node {self.node_id}] Shutting down coordinator...")
        self.running = False
        
        self.stop_monitoring()
        
        if self.is_leader:
            self.shutdown_all_containers()
        
        if hasattr(self, 'leader_elector'):
            self.leader_elector.cleanup()
        
        logger.info(f"[Node {self.node_id}] Coordinator shutdown complete")


if __name__ == "__main__":
    coordinator = None
    
    def signal_handler(signum, frame):
        """Handle shutdown signals."""
        logger.warning(f"Señal de apagado recibida ({signum})")
        
        if coordinator:
            coordinator.shutdown()
        
        logger.warning("Saliendo...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        coordinator = Coordinator()
        coordinator.run()
    except Exception as e:
        logger.error(f"Error running coordinator: {e}")
        sys.exit(1)